import sys
import os
import os.path
import stat
import logging

from django.db import models
from django.db.transaction import atomic

scanlogger = logging.getLogger("gbackup.scan")

class Object(models.Model):
    """This table keeps track of what objects exist in the remote data store

    The existence of an object in this table implies that an object has been
    committed to the remote data store.

    The payload field is only filled in for tree and inode object types. Blob
    types are not stored locally. In other words, we only cache metadata type
    objects locally.
    """
    objid = models.CharField(max_length=64, primary_key=True)
    payload = models.TextField(blank=True, null=True)

    children = models.ManyToManyField("Object")

    def __repr__(self):
        return "<Object {}>".format(self.objid)

class PathField(models.CharField):
    """Stores path strings as their binary version

    On Linux, filenames are binary strings, but are typically displayed using a
    system encoding. Some filenames may not be valid encodings though, so this
    field makes sure we store the binary form in the database, and does the
    conversion to and from the string representation for use in the python code.

    This is necessary because trying to store an invalid unicode string in
    SQLite will raise an error, as Python's SQLite driver will be unable to
    encode it to UTF-8. With this field, the data type is actually a binary
    BLOB type.
    """
    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 4096)
        super().__init__(**kwargs)

    def get_internal_type(self):
        return "BinaryField"

    def get_prep_value(self, value):
        if isinstance(value, str):
            return value.encode(sys.getfilesystemencoding(),
                                errors=sys.getfilesystemencodeerrors())
        return value

    def from_db_value(self, value, expression, connection):
        if isinstance(value, bytes):
            return value.decode(sys.getfilesystemencoding(),
                                errors=sys.getfilesystemencodeerrors())
        return value

class FSEntry(models.Model):
    """Keeps track of an entry in the local filesystem, either a directory,
    or a file.

    This tracks the last known state of each filesystem entry, so that it can
    be compared to the actual state of the filesystem to see if it needs
    updating.

    It also keeps track of the last known object ID that was uploaded for
    this object. If the objid is null, then this entry is considered "dirty"
    and needs to be uploaded.
    """
    objid = models.ForeignKey(
        "Object",
        null=True, blank=True,
        on_delete=models.SET_NULL,
    )

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
        # Turn back to bytes, and re-encode as UTF-8
        bytepath = self.path.encode(sys.getfilesystemencoding(),
                                    errors=sys.getfilesystemencodeerrors())
        return bytepath.decode("utf-8", errors="replace")

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

    def __repr__(self):
        return "<FSEntry {}>".format(self.printablepath)

    def __str__(self):
        return self.printablepath

    @atomic()
    def scan(self):
        """Scans this entry for changes

        Performs an os.lstat() on this entry. If its metadata differs from
        the database, it is invalidated: its objid is set to NULL and its
        metadata is updated. The new flag is cleared if it was set.

        If the entry is a directory entry and the metadata indicates it's
        changed, listdir() is called and a database query for this entry's
        children is made. Any old entries are deleted and any new entries are
        created (with the "new" flag set)

        If this entry used to be a directory but has changed file types,
        all children are deleted.

        :returns: the size of this entry if the entry has changed, so the
            caller can collect stats

        """
        scanlogger.debug("Entering scan for {}".format(self))
        try:
            stat_result = os.lstat(self.path)
        except FileNotFoundError:
            # This path no longer exists, so neither should this database entry
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
            # name before a scan runs, then there's no other mechanism to
            # delete the children.
            scanlogger.info("No longer a directory: {}".format(self))
            self.children.all().delete()

        if not self.new and (
            self.st_mode == stat_result.st_mode and
            self.st_mtime_ns == stat_result.st_mtime_ns and
            self.st_size == stat_result.st_size
        ):
            # This entry has not changed
            scanlogger.debug("No change to {}".format(self))
            return

        self.objid = None
        self.new = False

        self.st_mode = stat_result.st_mode
        self.st_mtime_ns = stat_result.st_mtime_ns
        self.st_size = stat_result.st_size


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
                newentry = FSEntry.objects.create(
                    path=newpath,
                    parent=self,
                    new=True,
                )
                scanlogger.info("New path: {}".format(newentry))

            # Delete old entries
            for child in children:
                if child.name not in entries:
                    scanlogger.info("deleting from dir: {}".format(
                        child))
                    child.delete()

        scanlogger.info("Entry updated: {}".format(self))
        self.save()
        return self.st_size
