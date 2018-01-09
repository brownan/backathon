import io
import os
import os.path
import stat
import logging

import umsgpack

from django.db import models, IntegrityError
from django.db.transaction import atomic
from django.db import connection

from . import chunker

scanlogger = logging.getLogger("gbackup.scan")

class DependencyError(Exception):
    pass

class PathField(models.CharField):
    """Stores path strings as their binary version

    On Linux, filenames are binary strings, but are typically displayed using a
    system encoding. Some filenames may contain un-decodable byte sequences,
    however, and Python will automatically embed un-decodable bytes as
    unicode surrogates, as specified in PEP 383.

    This field stores file paths as binary sequences, and uses the
    os.fsencode() and os.fsdecode() functions to translate to and from
    strings when loading/saving from the database.

    This avoids encoding problems, as passing a string with surrogates to
    SQLite will raise an exception when trying to encode.

    Note that many of the common query lookups don't work on BLOB fields the
    same as TEXT fields. For example, using the __startswith lookup will
    never match because SQLite doesn't implement the LIKE operator for BLOB
    types.
    """
    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 4096)
        super().__init__(**kwargs)

    def get_internal_type(self):
        return "BinaryField"

    def get_prep_value(self, value):
        if isinstance(value, str):
            return os.fsencode(value)
        return value

    def from_db_value(self, value, expression, connection):
        if isinstance(value, bytes):
            return os.fsdecode(value)
        return value

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
    objid = models.CharField(max_length=64, primary_key=True)
    payload = models.BinaryField(blank=True, null=True)

    children = models.ManyToManyField(
        "self",
        symmetrical=False,
        related_name="parents",
    )

    def __repr__(self):
        return "<Object {}>".format(self.objid)

    @classmethod
    def empty_garbage(cls):
        # untested: TODO test this
        query = """
        WITH RECURSIVE reachable(id) AS (
            SELECT root_id FROM gbackup_snapshot
            UNION ALL
            SELECT to_object_id FROM gbackup_object_children
            INNER JOIN reachable ON reachable.id=from_object_id
        ) SELECT gbackup_object.* FROM gbackup_object
          WHERE gbackup_object.objid NOT IN reachable
        """
        pass # TODO: execute

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
        cursor = connection.cursor()
        cursor.execute("""
        WITH RECURSIVE ancestors(id) AS (
          SELECT id FROM gbackup_fsentry WHERE id=?
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
        except FileNotFoundError:
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
                    # that is an ancestor of an existing root. Scaning from
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

    def backup(self):
        """Back up this entry

        Reads this entry in from the file system, creates one or more object
        payloads, and yields them to the caller for uploading to the backing
        store. The caller is expected to send the Object instance
        back into this iterator function.

        Yields: (payload_buffer, list_of_child_FSEntry_objects)
        Caller sends: models.Object instance of the last yielded payload

        The payload_buffer will be a file-like object ready for reading.
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
        except FileNotFoundError:
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

            # TODO: handle permission denied errors
            try:
                with open(self.path, "rb") as fobj:
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
                scanlogger.exception("Error in system call when reading file "
                                     "{}".format(self))
                # In order to not crash the entire backup, we must delete
                # this entry so that the parent directory can still be backed
                # up. This code path may leave one or more objects saved to
                # the remote storage, but there's not much we can do about
                # that here. (Basically, since every exit from this method
                # must either acquire and save an objid or delete itself,
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
                ctime=stat_result.st_ctime_ns,
                mtime=stat_result.st_mtime_ns,
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
            if any(c.obj is None for c in children):
                raise DependencyError(self.printablepath)

            buf = io.BytesIO()
            umsgpack.pack("tree", buf)
            info = dict(
                uid=stat_result.st_uid,
                gid=stat_result.st_gid,
                mode=stat_result.st_mode,
                ctime=stat_result.st_ctime_ns,
                mtime=stat_result.st_mtime_ns,
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
        help_text="Root directory of this snapshot"
    )
    root = models.ForeignKey(
        Object,
        on_delete=models.PROTECT,
    )
    date = models.DateTimeField(db_index=True)