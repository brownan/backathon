import io
import os
import os.path
import stat
import logging
import math
import random

import umsgpack

from django.db import models, IntegrityError
from django.db.transaction import atomic
from django.db import connection

from gbackup.fields import PathField
from gbackup import chunker
from gbackup import util
from gbackup import signals

scanlogger = logging.getLogger("gbackup.scan")

class DependencyError(Exception):
    pass


class Object(models.Model):
    """This table keeps track of what objects exist in the remote data store

    The existence of an object in this table implies that an object has been
    committed to the remote data store.

    The payload field is only filled in for tree and inode object types. Blob
    types are not stored locally. In other words, we only cache metadata type
    objects locally.

    The children relation is used in the calculation of garbage objects. If
    an object depends on another in any way, it is added as a "child". Then,
    when a root object is deleted, a set of unreachable garbage objects can
    be calculated.
    """
    # This is the binary representation of the hash of the payload.
    # To get the int representation, you can use int.from_bytes(objid, 'little')
    # To get the hex representation, use objid.hex()
    # To create a bytes representation from a hex representation,
    # use bytes.fromhex(hex_representation)
    objid = models.BinaryField(primary_key=True)
    payload = models.BinaryField(blank=True, null=True)

    children = models.ManyToManyField(
        "self",
        symmetrical=False,
        related_name="parents",
    )

    def __repr__(self):
        return "<Object {}>".format(self.objid.hex())

    def __str__(self):
        return self.objid.hex()

    def load_payload(self, payload):
        """Loads a payload of data into this Object entry

        When an object is created or an object is being downloaded from a
        remote data store, the blob contents get stored locally. This method
        takes the blob and stores it, but also does any deeper processing
        needed to build indices to make searching the backups easier and faster.

        :type payload: bytes

        Which indices are populated is configurable, and not all indices may
        be used depending on the settings. For example, caching filenames of
        tree and inode objects enables file searching, but it takes up quite
        a bit of disk space for those fields and indices on those fields.

        This method is idempotent.

        If settings related to the indices change, then you'll need to
        iterate through all Objects and call obj.load_payload(obj.payload) to
        recompute fields that may not have been populated.
        """
        payload_items = Object.unpack_payload(payload)
        objtype = next(payload_items)
        payload_items.close()

        if objtype != "blob":
            self.payload = payload

        # TODO: further processing and indexing of the contents of the payload.
        # (no other indices are currently implemented)

    def calculate_children(self):
        """Gets the set of children from the cached payload

        Callers can use this to rebuild the children relation table. It's
        mostly useful when re-building the cache from remote repository data.

        Callers should take care to not just pass the result to
        obj.children.set(), because the objects may not exist in the
        database, either because it's missing from the repository, or it just
        hasn't been inserted yet.

        Further, due to SQLite's deferrable foreign keys, an integrity error
        won't be raised until the end of the transaction.

        A caller will probably want to check each child object for existence
        manually before inserting it as a child reference. Alternately,
        it can use SQLite's foreign_key_check pragma to insert children
        references in bulk, and then clean them up before committing the
        transaction.

        """
        if not self.payload:
            return []

        payload_items = Object.unpack_payload(self.payload)
        objtype = next(payload_items)
        try:
            if objtype == "tree":
                next(payload_items)
                children = [c[1] for c in next(payload_items)]

            elif objtype == "inode":
                # By coincidence, tree and inode objects have identical
                # routines for extracting the child objects
                next(payload_items)
                children = [c[1] for c in next(payload_items)]

            else:
                children = []

        finally:
            payload_items.close()

        return children

    @staticmethod
    def unpack_payload(payload):
        """Returns an iterator over a payload, iterating over the msgpacked
        objects within

        This exists as a static method since callers may need to call it
        without wanting to load in into an Object instance
        """
        buf = util.BytesReader(payload)
        try:
            while True:
                try:
                    yield umsgpack.unpack(buf)
                except umsgpack.InsufficientDataException:
                    return
        finally:
            buf.close()

    @classmethod
    def collect_garbage(cls):
        """Yields garbage objects from the Object table.

        Callers should take care to atomically delete objects in the remote
        storage backend along with rows in the Object table. It's more
        important to delete the rows, however, because if a row exists
        without a backing object, that can corrupt future backups that may
        try to reference that object. Leaving an un-referenced object on the
        backing store doesn't hurt anything except by taking up space.

        """
        # The approach implemented below is to construct a simple bloom filter
        # such that we collect about 95% of all garbage objects.

        # This approach was chosen because it should be quick (2 passes over
        # the database, where the first pass is read-only) and memory
        # efficient (uses about 760k for a million objects in the table)

        # One alternative is to perform a query for objects with no
        # references, which is quick due to indices on the
        # gbackup_object_children table, but requires many queries in a loop
        # to collect all garbage. It's theoretically possible to do this with
        # a single recursive query, but that requires holding the entire
        # garbage set in memory, which could get big.

        # Another approach is a traditional garbage collection strategy such as
        # mark-and-sweep. Problem with that is it would involve writing each
        # row on the first pass, which is a lot more IO and would probably be
        # slower.

        num_objects = cls.objects.all().count()

        # m - number of bits in the filter. Depends on num_objects
        # k - number of hash functions needed. Should be 4 for p=0.05
        p = 0.05
        m = int(math.ceil((num_objects * math.log(p)) / math.log(1 / math.pow(
            2, math.log(2)))))
        k = int(round(math.log(2) * m / num_objects))

        arr_size = int(math.ceil(m/8))
        bloom = bytearray(arr_size)

        # The "hash" functions will just be a random number that will be
        # xor'd with the object IDs. Using a different random int each time
        # also guards against false positives from collisions happening from
        # the same two objects each run.
        r = random.SystemRandom()
        hashes = [r.getrandbits(256) for _ in range(k)]

        # This query iterates over all the reachable objects by walking the
        # hierarchy formed using the Snapshot table as the roots and
        # traversing the links in the ManyToMany relation.
        query = """
        WITH RECURSIVE reachable(id) AS (
            SELECT root_id FROM gbackup_snapshot
            UNION ALL
            SELECT to_object_id FROM gbackup_object_children
            INNER JOIN reachable ON reachable.id=from_object_id
        ) SELECT id FROM reachable
        """
        with connection.cursor() as c:
            c.execute(query)
            for row in c:
                objid_int = int.from_bytes(row[0], 'little')

                for h in hashes:
                    h ^= objid_int
                    h %= m
                    bytepos, bitpos = divmod(h, 8)
                    bloom[bytepos] |= 1 << bitpos

        def hash_match(h, objid):
            h ^= objid
            h %= m
            bytepos, bitpos = divmod(h, 8)
            return bloom[bytepos] & (1 << bitpos)

        # Now we can iterate over all objects. If an object does not appear
        # in the bloom filter, we can guarantee it's not reachable.
        for obj in cls.objects.all().iterator():
            objid = int.from_bytes(obj.objid, 'little')

            if not all(hash_match(h, objid) for h in hashes):
                yield obj

    def get_child_by_name(self, name):
        """For tree objects, this gets the child object of the given name

        Raises ValueError if the current object is not a tree.
        Raises Object.DoesNotExist if a child with the given name is not found

        :param name: The name to search for. If a string is given,
        it's encoded with the default filesystem encoding.
        :type name: str|bytes
        """
        # Currently implemented by parsing the metadata. When a directory
        # index is implemented, this method should be rewritten to check that
        if isinstance(name, str):
            name = os.fsencode(name)

        if not self.payload:
            raise self.DoesNotExist("No payload")

        payload_items = Object.unpack_payload(self.payload)
        if next(payload_items) != "tree":
            raise ValueError("Object is not a tree")

        next(payload_items)
        for n, objid in next(payload_items):
            if n == name:
                return self.children.get(id=objid)
        raise Object.DoesNotExist("Object has no child named {!r}".format(name))


class FSEntry(models.Model):
    """Keeps track of an entry in the local filesystem, either a directory,
    or a file.

    This tracks the last known state of each filesystem entry, so that it can
    be compared to the actual state of the filesystem to see if it has changed.

    It also keeps track of the last known object ID that was uploaded for
    this object. If obj is null, then this entry is considered "dirty"
    and needs to be uploaded.
    """
    obj = models.ForeignKey(
        "Object",
        null=True, blank=True,
        on_delete=models.SET_NULL,
    )

    # Note: be careful about using self.path and self.name in anything but
    # calls to os functions, since they may contain non-decodable bytes
    # embedded as unicode surrogates as specified in PEP 383, which will
    # crash most other attempts to encode or print them. Use the
    # printablepath property instead, or explicitly encode with os.fsencode().
    path = PathField(
        help_text="Absolute path on the local filesystem",
        unique=True,
    )

    @property
    def name(self):
        return os.path.basename(self.path)

    @property
    def printablepath(self):
        """Used in printable representations"""
        # Use the replacement error handler to turn any surrogate codepoints
        # into something that won't crash attempts to encode them
        bytepath = os.fsencode(self.path)
        return bytepath.decode("utf-8", errors="replace")

    # Note about the DO_NOTHING delete action: we create the SQLite tables
    # with ON DELETE CASCADE, so the database will perform cascading
    # deletes instead of Django. For memory efficiency, we tell Django to do
    # nothing and let SQLite take care of it.
    parent = models.ForeignKey(
        'self',
        related_name="children",
        on_delete=models.DO_NOTHING,
        null=True, blank=True,
        help_text="The parent FSEntry. This relation defines the hierarchy of "
                  "the filesystem. It is null for the root entry of the "
                  "backup set."
    )

    new = models.BooleanField(
        default=True,
        db_index=True,
        help_text="Indicates this is a new entry and needs scanning. "
                  "It forces an update to the metadata next scan.",
    )

    # These fields are used to determine if an entry has changed
    st_mode = models.IntegerField(null=True)
    st_mtime_ns = models.IntegerField(null=True)
    st_size = models.IntegerField(null=True)

    def update_stat_info(self, stat_result: os.stat_result):
        self.st_mode = stat_result.st_mode
        self.st_mtime_ns = stat_result.st_mtime_ns
        self.st_size = stat_result.st_size

    def compare_stat_info(self, stat_result: os.stat_result):
        return (
            self.st_mode == stat_result.st_mode and
            self.st_mtime_ns == stat_result.st_mtime_ns and
            self.st_size == stat_result.st_size
        )

    def __repr__(self):
        return "<FSEntry {}>".format(self.printablepath)

    def __str__(self):
        return self.printablepath

    def invalidate(self):
        """Runs a query to invalidate this node and all parents up to the root

        """
        with connection.cursor() as cursor:
            cursor.execute("""
            WITH RECURSIVE ancestors(id) AS (
              SELECT id FROM gbackup_fsentry WHERE id=%s
              UNION ALL
              SELECT gbackup_fsentry.parent_id FROM gbackup_fsentry
              INNER JOIN ancestors ON (gbackup_fsentry.id=ancestors.id)
              WHERE gbackup_fsentry.parent_id IS NOT NULL
            ) UPDATE gbackup_fsentry SET obj_id=NULL
              WHERE gbackup_fsentry.id IN ancestors
            """, (self.id,))

    @atomic()
    def scan(self):
        """Scans this entry for changes

        Performs an os.lstat() on this entry. If its metadata differs from
        the database, it is invalidated: its obj is set to NULL and its
        metadata is updated. The new flag is cleared if it was set.

        If the entry is a directory entry and the metadata indicates it's
        changed, listdir() is called and a database query for this entry's
        children is made. Any old entries are deleted and any new entries are
        created (with the "new" flag set)

        If this entry used to be a directory but has changed file types,
        all children are deleted.

        """
        scanlogger.debug("Entering scan for {}".format(self))
        try:
            stat_result = os.lstat(self.path)
        except (FileNotFoundError, NotADirectoryError):
            # NotADirectoryError can happen if we're trying to scan a file,
            # but one of its parent directories is no longer a directory.
            scanlogger.info("Not found, deleting: {}".format(self))
            self.delete()
            return

        if (
                self.st_mode is not None and
                stat.S_ISDIR(self.st_mode) and
                not stat.S_ISDIR(stat_result.st_mode)
        ):
            # The type of entry has changed from directory to something else.
            # Normally, directories when they are deleted will hit the
            # FileNotFound exception above, which will recursively cascade to
            # delete their children. But if a file is recreated with the same
            # name before a scan runs, it could leave orphaned children in
            # the database.
            scanlogger.info("No longer a directory: {}".format(self))
            self.children.all().delete()

        if not self.new and self.compare_stat_info(stat_result):
            scanlogger.debug("No change to {}".format(self))
            return

        self.obj = None
        self.new = False

        self.update_stat_info(stat_result)

        if stat.S_ISDIR(self.st_mode):

            children = list(self.children.all())

            # Check the directory entries against the database.
            # We need to do a listdir to compare the entries in the database
            # against the actual entries in the directory
            try:
                entries = set(os.listdir(self.path))
            except PermissionError:
                scanlogger.warning("Permission denied: {}".format(
                    self))
                entries = set()

            # Create new entries
            for newname in entries.difference(c.name for c in children):
                newpath = os.path.join(self.path, newname)
                try:
                    with atomic():
                        newentry = FSEntry.objects.create(
                            path=newpath,
                            parent=self,
                            new=True,
                        )
                except IntegrityError:
                    # This can happen if a new root is added to the database
                    # that is an ancestor of an existing root. Scanning from
                    # the new root will re-discover the existing root. In
                    # this case, just re-parent the old root, merging the two
                    # trees.
                    newentry = FSEntry.objects.get(path=newpath)
                    scanlogger.warning("Trying to create path but already "
                                       "exists. Reparenting: {}".format(
                        newentry))
                    # If this isn't a root, something is really wrong with
                    # our tree!
                    assert newentry.parent_id is None
                    newentry.parent = self
                    newentry.save(update_fields=['parent'])
                else:
                    scanlogger.info("New path: {}".format(newentry))

            # Delete old entries
            for child in children:
                if child.name not in entries:
                    scanlogger.info("deleting from dir: {}".format(
                        child))
                    child.delete()

        scanlogger.info("Entry updated: {}".format(self))
        self.save()
        self.invalidate()
        return

    def _open_file(self):
        """Opens this file for reading"""
        flags = os.O_RDONLY

        # Add O_BINARY on windows
        flags |= getattr(os, "O_BINARY", 0)

        try:
            flags_noatime = flags | os.O_NOATIME
        except AttributeError:
            return os.fdopen(os.open(self.path, flags), "rb")

        # Add O_NOATIME if available. This may fail with permission denied,
        # so try again without it if failed
        try:
            return os.fdopen(os.open(self.path, flags_noatime), "rb")
        except PermissionError:
            pass
        return os.fdopen(os.open(self.path, flags), "rb")

    def backup(self):
        """Back up this entry

        Reads this entry in from the file system, creates one or more object
        payloads, and yields them to the caller for uploading to the backing
        store. The caller is expected to send the Object instance
        back into this iterator function.

        Yields: (payload_buffer, list_of_child_Object_instances)
        Caller sends: models.Object instance of the last yielded payload

        The payload_buffer is a file-like object ready for reading.
        Usually a BytesIO instance.

        Note: this sequence of operations was chosen over having this
        method upload the objects itself so that the caller may choose to
        buffer and upload objects in batch. It's also more flexible in
        several ways. E.g. while a recursive algorithm would
        have to upload items in a post-order traversal of the tree, here
        the caller is free to do a SQL query to get items ordered by any
        criteria. Like, say, all small files first and pack them together into
        a single upload.

        For directories: yields a single payload for the directory entry.
        Raises a DependencyError if one or more children do not have an
        obj already. It's the caller's responsibility to call backup() on
        entries in an order to avoid dependency issues.

        For files: yields one or more payloads for the file's contents,
        then finally a payload for the inode entry.

        IMPORTANT: every exit point from this function must either update
        this entry's obj to a non-null value, OR delete the entry before
        returning.
        """
        try:
            stat_result = os.lstat(self.path)
        except (FileNotFoundError, NotADirectoryError):
            scanlogger.info("File disappeared: {}".format(self))
            self.delete()
            return

        # If this entry is significantly different from what it looked like
        # when it was scanned, then we shouldn't try to back it up. The logic
        # for managing child references and such lives in the scan() method,
        # so delete this entry and let it get re-created next scan.
        if stat.S_IFMT(self.st_mode) != stat.S_IFMT(stat_result.st_mode):
            scanlogger.warning("File changed type since scan, deleting: "
                               "{}".format(self))
            self.delete()
            return

        self.update_stat_info(stat_result)

        if stat.S_ISREG(self.st_mode):
            # File
            chunks = []
            childobjs = []

            try:
                with self._open_file() as fobj:
                    for pos, chunk in chunker.DefaultChunker(fobj):
                        buf = io.BytesIO()
                        umsgpack.pack("blob", buf)
                        umsgpack.pack(chunk, buf)
                        buf.seek(0)
                        chunk_obj = yield (buf, [])
                        childobjs.append(chunk_obj)
                        chunks.append((pos, chunk_obj.objid))
            except FileNotFoundError:
                scanlogger.info("File disappeared: {}".format(self))
                self.delete()
                return
            except OSError as e:
                # This happens with permission denied errors
                scanlogger.exception("Error in system call when reading file "
                                     "{}".format(self))
                # In order to not crash the entire backup, we must delete
                # this entry so that the parent directory can still be backed
                # up. This code path may leave one or more objects saved to
                # the remote storage, but there's not much we can do about
                # that here. (Basically, since every exit from this method
                # must either acquire and save an obj or delete itself,
                # we have no choice)
                self.delete()
                return

            # Now construct the payload for the inode
            buf = io.BytesIO()
            umsgpack.pack("inode", buf)
            info = dict(
                size=stat_result.st_size,
                inode=stat_result.st_ino,
                uid=stat_result.st_uid,
                gid=stat_result.st_gid,
                mode=stat_result.st_mode,
                mtime=stat_result.st_mtime_ns,
                atime=stat_result.st_atime_ns,
            )
            umsgpack.pack(info, buf)
            umsgpack.pack(chunks, buf)
            buf.seek(0)

            self.obj = yield (buf, childobjs)
            scanlogger.info("Backed up file into {} objects: {}".format(
                len(chunks)+1,
                self
            ))

        elif stat.S_ISDIR(self.st_mode):
            # Directory
            # Note: backing up a directory doesn't involve reading
            # from the filesystem aside from the lstat() call from above. All
            # the information we need is already in the database.
            children = list(self.children.all().select_related("obj"))

            # This block asserts all children have been backed up before
            # entering this method. If they haven't, then the caller is in
            # error. The current backup strategy involves the caller
            # traversing nodes to back them up in an order that avoids
            # dependency issues.
            # A simplified backup strategy would be to make this method
            # recursive (using `yield from`) and then just call backup on the
            # root nodes. There's no reason I can think of that that wouldn't
            # work. Enforcing this here is just a sanity check for the current
            # backup strategy.
            if any(c.obj is None for c in children):
                raise DependencyError(
                    "{} depends on these paths, but they haven't been "
                    "backed up yet. This is a bug. {}"
                    "".format(
                        self.printablepath,
                        ", ".join(c.printablepath
                                  for c in children if c.obj is None),
                    )
                )

            buf = io.BytesIO()
            umsgpack.pack("tree", buf)
            info = dict(
                uid=stat_result.st_uid,
                gid=stat_result.st_gid,
                mode=stat_result.st_mode,
                mtime=stat_result.st_mtime_ns,
                atime=stat_result.st_atime_ns,
            )
            umsgpack.pack(info, buf)
            umsgpack.pack(
                # We have to store the original binary representation of
                # the filename or msgpack will error at filenames with
                # bad encodings
                [(os.fsencode(c.name), c.obj.objid) for c in children],
                buf,
            )
            buf.seek(0)
            
            self.obj = yield (buf, (c.obj for c in children))

            scanlogger.info("Backed up dir: {}".format(
                self
            ))

        else:
            scanlogger.warning("Unknown file type, not backing up {}".format(
                self))
            self.delete()
            return

        self.save()
        return

class Snapshot(models.Model):
    """A snapshot of a filesystem at a particular time"""
    path = PathField(
        help_text="Root directory of this snapshot on the original filesystem"
    )
    root = models.ForeignKey(
        Object,
        on_delete=models.PROTECT,
    )
    date = models.DateTimeField(db_index=True)

    @property
    def printablepath(self):
        """Used in printable representations"""
        # Use the replacement error handler to turn any surrogate codepoints
        # into something that won't crash attempts to encode them
        bytepath = os.fsencode(self.path)
        return bytepath.decode("utf-8", errors="replace")

class Setting(models.Model):
    """Configuration table for settings set at runtime"""
    key = models.TextField(primary_key=True)
    value = models.TextField()

    _empty = object()
    @classmethod
    def get(cls, key, default=_empty):
        try:
            return cls.objects.get(key=key).value
        except cls.DoesNotExist:
            if default is cls._empty:
                raise KeyError("No such setting: {}".format(key))
            return default

    @classmethod
    def set(cls, key, value):
        s = cls(key=key, value=value)
        s.save()
        signals.db_setting_changed.send(sender=Setting, setting=key,
                                        value=value)
