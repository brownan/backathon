import collections
import os
import os.path
import stat
import logging

from django.db import models

scanlogger = logging.getLogger("gbackup.scan")

class Object(models.Model):
    """This table keeps track of what objects exist in the remote data store

    The existence of an object in this table implies that an object has been
    committed to the remote data store.

    """
    objid = models.CharField(max_length=64, primary_key=True)
    payload = models.TextField(blank=True, null=True)

    children = models.ManyToManyField("Object")

    def __repr__(self):
        return "<Object {}>".format(self.objid)

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

    path = models.CharField(
        max_length=4096,
        help_text="Absolute path on the local filesystem",
        unique=True,
    )
    @property
    def name(self):
        return os.path.basename(self.path)

    parent = models.ForeignKey(
        'self',
        related_name="children",
        on_delete=models.CASCADE,
        null=True,
        help_text="The parent FSEntry. This relation defines the hierarchy of "
                  "the filesystem. It is null for the root entry of the "
                  "backup set."
    )

    # These fields are used to determine if an entry has changed
    st_mode = models.IntegerField(null=True)
    st_mtime_ns = models.IntegerField(null=True)
    st_size = models.IntegerField(null=True)

    def __repr__(self):
        return "<FSEntry {}>".format(self.path)

    def update(self):
        """Checks whether this filesystem entry needs updating by performing
        an os.stat() call. If this is a directory, also adds or deletes
        children by performing an os.listdir() call.

        If this entry has changed, sets its objid field to NULL

        :returns: a list of children objects. The returned list will be empty if
            the entry type is not a directory.
        """
        try:
            stat_result = os.lstat(self.path)
        except FileNotFoundError:
            # This path no longer exists, so neither should this database entry
            scanlogger.info("Not found: {}".format(self.path))
            self.delete()
            return []

        if self.st_mode is not None and \
                stat.S_IFMT(stat_result.st_mode) != stat.S_IFMT(self.st_mode):
            # The type of entry has changed. Consider it deleted, and let the
            # parent recreate it next time the parent updates
            scanlogger.info("Changed type: {}".format(self.path))
            self.delete()
            return []

        if not (
            self.st_mode == stat_result.st_mode and
            self.st_mtime_ns == stat_result.st_mtime_ns and
            self.st_size == stat_result.st_size
        ):
            # Something changed. Invalidate this entry
            changed = True
            self.objid = None
            scanlogger.info("Marking as changed: {}".format(self.path))
        else:
            changed = False

        self.st_mode = stat_result.st_mode
        self.st_mtime_ns = stat_result.st_mtime_ns
        self.st_size = stat_result.st_size

        children = list(self.children.all())

        if changed and stat.S_ISDIR(self.st_mode):
            # Check the directory entries against the database.
            # This entry has been invalidated, so we need to do a listdir
            # to compare the entries in the database against the actual
            # entries in the directory
            try:
                entries = set(os.listdir(self.path))
            except PermissionError:
                scanlogger.warning("Permission denied: {}".format(self.path))
                entries = set()

            # Create new entries
            for newname in entries.difference(c.name for c in children):
                newpath = os.path.join(self.path, newname)
                scanlogger.info("New path: {}".format(newpath))
                newentry = FSEntry.objects.create(
                    path=newpath,
                    parent=self,
                )
                children.append(newentry)
                # It will be updated in the next block when we update each child

            # Delete old entries
            for child in children:
                if child.name not in entries:
                    scanlogger.info("Child not in dir: {}".format(child.path))
                    child.delete()

            # Return children that still exist
            children = [c for c in children if c.id is not None]

        self.save()
        return children

    def update_all(self, update_func=None):
       """Initiates a depth-first tree traversal starting at this node

       Calls update() on each node in a pre-order traversal
       """
       queue = collections.deque([self])

       numfiles = 0
       size = 0

       while queue:
           entry = queue.popleft()

           children = entry.update()

           if entry.id is None:
               # Node itself was deleted, ignore it.
               continue

           numfiles += 1
           size += entry.st_size

           if update_func is not None and numfiles%100==0:
               update_func(numfiles, size)

           children.reverse()
           queue.extendleft(children)
