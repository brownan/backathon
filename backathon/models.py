import logging
import os
import os.path
import stat

from django.db import IntegrityError, connections, models
from django.db.transaction import atomic

from backathon.fields import PathField
from backathon.util import atomic_immediate

scanlogger = logging.getLogger("backathon.scan")


class Object(models.Model):
    """This table keeps track of what objects exist in the remote repository

    The existence of an object in this table implies that an object has been
    committed to the repository.

    The children relation is used in the calculation of garbage objects. If
    an object depends on another in any way, it is added as a "child". Then,
    when a root object is deleted, a set of unreachable garbage objects can
    be calculated.
    """

    class Meta:
        db_table = "objects"

    # This is the binary representation of the hash of the payload.
    # To get the int representation, you can use int.from_bytes(objid, 'little')
    # To get the hex representation, use objid.hex()
    # To create a bytes representation from a hex representation,
    # use bytes.fromhex(hex_representation)
    objid = models.BinaryField(primary_key=True)

    children = models.ManyToManyField(
        "self",
        symmetrical=False,
        related_name="parents",
        through="ObjectRelation",
        through_fields=("parent", "child"),
    )

    # These fields are cached about the object. They may or may not have
    # values depending on the object type. Additionally, they may not be
    # filled in after a restore, as the objects have not yet been downloaded
    # and decoded.
    type = models.CharField(
        max_length=16,
        blank=True,
        null=True,
        default=None,
    )
    uploaded_size = models.PositiveIntegerField(
        blank=True,
        null=True,
        help_text="Size of the uploaded payload, after compression and " "encryption",
    )
    file_size = models.PositiveIntegerField(
        blank=True,
        null=True,
        help_text="For inode objects, this is the file's size",
    )
    last_modified_time = models.DateTimeField(
        blank=True,
        null=True,
        help_text="For inode and tree objects, this is the last modified time",
    )

    def __repr__(self):
        return "<Object {}>".format(self.objid.hex()[:7])

    def __str__(self):
        return self.objid.hex()[:7]


class ObjectRelation(models.Model):
    """Keeps track of the dependency graph between objects

    This model's primary purpose is to track object relations (when one object
    references another object) so that we can do garbage collection
    calculations purely on the client. This model is not used during a
    restore. During a restore, the object payloads are decoded and the object
    tree traversed from the validated contents of each object.

    This model is also used to support browsing file manifests in the UI.

    The relations table may not be filled in at all after a recovery,
    in which case UI browsing will be impossible or will have to fetch the
    objects on demand. But full restores of an entire snapshot are still
    possible.
    """

    class Meta:
        db_table = "object_relations"

    parent = models.ForeignKey(
        "Object",
        on_delete=models.CASCADE,
        related_name="+",
    )
    child = models.ForeignKey(
        "Object",
        on_delete=models.CASCADE,
        related_name="+",
    )

    name = models.CharField(
        max_length=4096,
        help_text="The decoded name of this directory entry if parent is a "
        "tree object. Names are decoded using the 'ignore' error "
        "handler",
        blank=True,
        null=True,
        default=None,
    )

    def __repr__(self):
        return "<ObjectRelation {}→{}>".format(
            self.parent_id.hex()[:7],
            self.child_id.hex()[:7],
        )


class FSEntry(models.Model):
    """Keeps track of an entry in the local filesystem, either a directory,
    or a file.

    This tracks the last known state of each filesystem entry, so that it can
    be compared to the actual state of the filesystem to see if it has changed.

    It also keeps track of the last known object ID that was uploaded for
    this object. If obj is null, then this entry is considered "dirty"
    and needs to be uploaded.
    """

    class Meta:
        db_table = "fsentry"

    obj = models.ForeignKey(
        "Object",
        null=True,
        blank=True,
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
    # deletes instead of Django. Django tries to pull the entire deletion
    # set into memory. For memory efficiency, we tell Django to do nothing
    # and let SQLite take care of it.
    parent = models.ForeignKey(
        "self",
        related_name="children",
        on_delete=models.DO_NOTHING,
        null=True,
        blank=True,
        help_text="The parent FSEntry. This relation defines the hierarchy of "
        "the filesystem. It is null for the root entry of the "
        "backup set.",
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
            self.st_mode == stat_result.st_mode
            and self.st_mtime_ns == stat_result.st_mtime_ns
            and self.st_size == stat_result.st_size
        )

    def __repr__(self):
        return "<FSEntry {}>".format(self.printablepath)

    def __str__(self):
        return self.printablepath

    def invalidate(self):
        """Runs a query to invalidate this node and all parents up to the root"""
        with connections[self._state.db].cursor() as cursor:
            cursor.execute(
                """
            WITH RECURSIVE ancestors(id) AS (
              SELECT id FROM fsentry WHERE id=%s
              UNION ALL
              SELECT fsentry.parent_id FROM fsentry
              INNER JOIN ancestors ON (fsentry.id=ancestors.id)
              WHERE fsentry.parent_id IS NOT NULL
            ) UPDATE fsentry SET obj_id=NULL
              WHERE fsentry.id IN ancestors
            """,
                (self.id,),
            )

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
        with atomic_immediate(using=self._state.db):
            try:
                stat_result = os.lstat(self.path)
            except (FileNotFoundError, NotADirectoryError):
                # NotADirectoryError can happen if we're trying to scan a file,
                # but one of its parent directories is no longer a directory.
                scanlogger.info("Not found, deleting: {}".format(self))
                self.delete()
                return

            if (
                self.st_mode is not None
                and stat.S_ISDIR(self.st_mode)
                and not stat.S_ISDIR(stat_result.st_mode)
            ):
                # The type of entry has changed from directory to something else.
                # Normally, directories when they are deleted will hit the
                # FileNotFound exception above, which will recursively cascade to
                # delete their children. But if a file is recreated with the same
                # name before a scan runs, it could leave orphaned children in
                # the database. (They would be cleaned up when those child entries
                # are scanned, though, so this is probably unnecessary)
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
                    scanlogger.warning("Permission denied: {}".format(self))
                    entries = set()

                # Create new entries
                for newname in entries.difference(c.name for c in children):
                    newpath = os.path.join(self.path, newname)
                    try:
                        with atomic(using=self._state.db):
                            newentry = FSEntry.objects.using(self._state.db).create(
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
                        newentry = FSEntry.objects.using(self._state.db).get(path=newpath)
                        scanlogger.warning(
                            "Trying to create path but already exists. "
                            "Reparenting: {}".format(newentry)
                        )
                        # If this isn't a root, something is really wrong with
                        # our tree!
                        assert newentry.parent_id is None
                        newentry.parent = self
                        newentry.save(update_fields=["parent"])
                    else:
                        scanlogger.info("New path     : {}".format(newentry))

                # Delete old entries
                for child in children:
                    if child.name not in entries:
                        scanlogger.info("deleting from dir: {}".format(child))
                        child.delete()

            scanlogger.info("Entry updated: {}".format(self))
            self.save()
            self.invalidate()
            return


class Snapshot(models.Model):
    """A snapshot of a filesystem at a particular time"""

    class Meta:
        db_table = "snapshots"

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

    class Meta:
        db_table = "settings"

    key = models.TextField(primary_key=True)
    value = models.TextField()

    _empty = object()

    @classmethod
    def get(cls, key, default=_empty, using=None):
        try:
            return cls.objects.using(using).get(key=key).value
        except cls.DoesNotExist:
            if default is cls._empty:
                raise KeyError("No such setting: {}".format(key))
            return default

    @classmethod
    def set(cls, key, value, using=None):
        s = cls(key=key, value=value)
        s.save(using=using)
