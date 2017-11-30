"""Classes in this module represent objects in the object store

THIS FILE IS OBSOLETE. I'm leaving it in the repo for now since it has some
code related to serializing objects that I want to keep as reference

There are different types of objects. Some terminology

* An object's "contents" is the serialized representation of the data it
  represents. The contents is what's stored in the local object cache.

* An object's "payload" is the contents after it's been compressed and
  encrypted. If compression and encryption are disabled, the payload is the
  same as the contents. The payload is what's uploaded to the remote object
  store.

* An object ID is a hash or HMAC digest of the contents, and becomes the key
  for the object in the object store

"""

import io
import os
from collections import OrderedDict
from stat import S_ISDIR, S_ISREG
import logging

import msgpack

from .chunker import FixedChunker

logger = logging.getLogger("gbackup.objects")

class Tree:
    """A tree object represents a directory and its entries

    Currently supports entries that are:
    * Other directories
    * Regular files

    Not currently supported:
    * Character devices
    * Block devices
    * FIFOs
    * Symbolic Links (TODO)
    * Sockets

    """
    def __init__(self, path, filecache, objid=None):
        """Initialize a Tree object

        :param path: The absolute path to the directory on the local filesystem.
        :type filecache: gbackup.cache.FileCache:
        :param objid: If given, this object is initialized with an object id.

        """
        self._path = path
        self._cache = filecache
        self._objid = objid

        # This is a mapping of filename to (hash, object) where object is
        # either another Tree object or an Inode object. Hash may be None in
        # this tuple if the value is not yet known.
        self._children = OrderedDict()

        # TODO: initialize the files and children if objid was given
        if objid: raise NotImplementedError()

    def __repr__(self):
        return "<Tree {!r}>".format(self._path)

    def update_all(self, update_status=None):
        """Recursively scan the entire filesystem tree starting at this point

        Calls update() on this object, and calls update_all() on all child Tree
        objects and update() on all other child objects.

        :param update_status: If given, this is a callable that will be called
            each iteration with the current (numfiles, size) as parameters,
            for the purposes of status updates.

        :returns: the number of files, and the total size of the backup set
        :rtype: tuple[int, int]
        """
        # Start the numfiles at 1 for this directory
        numfiles = 1
        size = 0

        # Update this directory's child list and create any new child objects
        # as needed
        self.update()

        # Now recurse into each child and update them
        for hash, child in self._children.values():
            if isinstance(child, Tree):
                res = child.update_all(
                    (lambda x,y: update_status(x+numfiles, y+size))
                    if update_status is not None else None
                )
                numfiles += res[0]
                size += res[1]
            elif isinstance(child, Inode):
                res = child.update()
                if res is not None:
                    numfiles += 1
                    size += res
            else:
                raise NotImplementedError(str(type(child)))

        if update_status is not None:
            update_status(numfiles, size)

        return numfiles, size

    def update(self):
        """Scan this directory tree for changes.

        Performs a single stat and a single listdir on the directory to
        attempt to see if the cached info differs from the filesystem.

        Creates and deletes child objects as needed. Does not recurse into
        child objects

        :returns: None

        """
        # Mostly the same logic here as for Inode.update()
        try:
            stat = os.lstat(self._path)
        except FileNotFoundError:
            self._objid = None
            return None
        if not S_ISDIR(stat.st_mode):
            self._objid = None
            return None

        # Has the directory changed?
        old_entries = set(self._children)
        try:
            current_entries = os.listdir(self._path)
        except IOError:
            # Race condition: directory was deleted after the stat
            self._objid = None
            return None

        # Deleted entries
        for entry in old_entries.difference(current_entries):
            self._objid = None
            del self._children[entry]

        # New entries
        for entry in current_entries:
            if entry in old_entries:
                # Nothing to create
                continue

            self._objid = None
            assert entry not in self._children
            newpath = os.path.join(self._path, entry)
            try:
                stat = os.lstat(newpath)
            except IOError:
                # Race condition, item was deleted after doing the listdir
                continue
            if S_ISDIR(stat.st_mode):
                newobj = Tree(newpath, self._cache, None)
                self._children[entry] = (None, newobj)
            elif S_ISREG(stat.st_mode):
                newobj = Inode(newpath, self._cache, None)
                self._children[entry] = (None, newobj)
            else:
                logger.info("Not backing up {}, unknown file type {}".format(
                    newpath, stat.st_mode
                ))

    def backup(self):
        """Backs up the given directory tree if it's been updated since the
        last backup.

        This is a generator function.

        Yields binary strings that represent objects that need to be
        saved to the data store. Expects the resulting object ID to be sent
        back into the iterator.

        Returns the object ID of this object.

        This method does not scan the local filesystem for changes to this
        directory, but it does recurse into child tree and inode objects.
        This means that if a child changes, its entry in this tree is
        updated, but new files are not detected until update() is called.

        Once the tree has been backed up once, calling this function again
        becomes cheap, as the filesystem is not scanned and no system calls
        are made. Call update() to scan this tree for changes.

        """
        # For the present model, backing up requires traversing into all
        # children. In theory, this should be fairly quick if no children
        # have to be backed up, as the Inode objects will just return their
        # cached object id.
        # If this assumption turns out to be wrong and this process takes
        # considerable time for large filesystems, then consider adding back
        # links from nodes to their parent so they can notify their parents
        # of changes, then changing this to only traverse to children that
        # need updating.
        for entry in list(self._children):
            entryid, obj = self._children[entry]

            newentryid = yield from obj.backup()

            if newentryid is None:
                # This entry was deleted
                self._objid = None
                del self._children[entry]

            elif entryid != newentryid:
                # This entry has changed contents.
                self._objid = None
                self._children[entry] = (newentryid, obj)

        if self._objid is not None:
            return self._objid

        # From here on, we have to compute a new object ID by serializing this
        # tree
        try:
            stat = os.stat(self._path)
        except FileNotFoundError:
            # This directory was deleted
            return None
        if not S_ISDIR(stat.st_mode):
            # This path is not a directory. It must have been replaced with a
            # file by the same name. Assume the directory was deleted. The
            # parent Tree object should fix this on its next update()
            return None

        buf = io.BytesIO()
        msgpack.pack(b't', buf)
        msgpack.pack((b'u', stat.st_uid), buf)
        msgpack.pack((b'g', stat.st_gid), buf)
        msgpack.pack((b'm', stat.st_mode), buf)
        msgpack.pack((b'ct', stat.st_ctime_ns), buf)
        msgpack.pack((b'mt', stat.st_mtime_ns), buf)

        for entry in sorted(self._children):
            entryid = self._children[entry][0]
            msgpack.pack((b'e', entry, entryid), buf)

        buf.seek(0)
        self._objid = yield buf
        return self._objid

    def restore(self):
        pass # TODO

    def verify(self):
        pass # TODO


class Inode:
    """An inode object represents a file on the filesystem

    It holds metadata about the file, and links to one or more blobs
    containing the contents for the file.
    """
    def __init__(self, path, filecache, objid=None):
        """

        :param path: is the absolute path to the file on the local
        filesystem.
        :type filecache: gbackup.cache.FileCache
        :param objid: If given, this object is initialized with an object id.
        """
        self._path = path
        self._cache = filecache

        # This object's identifier. It is cached in this instance variable so
        # that subsequent backups are quick if nothing has changed. If we are
        # updated, this is invalidated.
        self._objid = objid

    def __repr__(self):
        return "<Inode {!r}>".format(self._path)

    def update(self):
        """Scans the local filesystem to see if the file has changed. If so,
        this file will be backed up on the next call to backup().

        This method always performs exactly one os.stat() call.

        This method is typically called recursively on every object when a
        full scan for changed files is requested. This should be done
        periodically, and when first initialized.

        This method is also designed to be called from some asynchronous
        notification service such as Linux's inotify subsystem to inform this
        object that it needs updating.

        :returns: Returns the size that needs to be updated (or at least
            scanned for updates), else returns None. This lets recursive callers
            get a feel for the size of the dataset.

        """
        # If the file's current stats match an entry in the file cache,
        # then we assume the object representing that file is still fully
        # uploaded
        try:
            stat = os.lstat(self._path)
        except FileNotFoundError:
            # If the file doesn't exist, return None. During the next backup,
            # we will check again and return None there, signaling to the
            # parent object that this object should be deleted along with the
            #  entry in the Tree object.
            self._objid = None
            return None
        if not S_ISREG(stat.st_mode):
            # File is no longer a file. Consider it deleted.
            self._objid = None
            return None

        objid = self._cache.get_file_cache(
            self._path,
            stat.st_ino,
            stat.st_mtime_ns,
            stat.st_size,
        )
        self._objid = objid

        if objid is None:
            # Needs updating, since this file wasn't found in the cache
            return stat.st_size

    def backup(self):
        """Backs up the given file, if it's been updated since the last backup

        This is a generator function.

        Yields binary strings that represent objects that need to be
        saved to the data store. Expects the resulting object ID to be sent
        back into the iterator.

        Returns the object ID of this object.

        This method does not scan the local filesystem for changes. Once the
        file has been backed up once, calling this function again becomes
        very cheap, as it just returns the cached object identifier. Call
        update() to scan the local filesystem for changes.

        Returns None if the file doesn't exist on the filesystem.
        """

        # Short circuit the entire method for quick incremental backups. If
        # objid is already set, then we've already been backed up once.
        # Assume the file hasn't changed. (Call update() to scan the local
        # filesystem for changes to this file)
        if self._objid is not None:
            return self._objid

        # Don't have an object ID cached? We need to compute this file's
        # object ID by reading in the contents.

        try:
            stat = os.lstat(self._path)
        except FileNotFoundError:
            return None
        if not S_ISREG(stat.st_mode):
            # This path exists but isn't a file. The file may have been
            # replaced since the last backup with some non-file. The parent
            # Tree object should fix this on its next update()
            return None

        buf = io.BytesIO()
        msgpack.pack(b'i', buf)
        msgpack.pack((b's', stat.st_size), buf)
        msgpack.pack((b'i', stat.st_ino), buf)
        msgpack.pack((b'u', stat.st_uid), buf)
        msgpack.pack((b'g', stat.st_gid), buf)
        msgpack.pack((b'm', stat.st_mode), buf)
        msgpack.pack((b'ct', stat.st_ctime_ns), buf)
        msgpack.pack((b'mt', stat.st_mtime_ns), buf)
        # Note that the file name or path is not part of this metadata. This
        # metadata mirrors the filesystem inode, and the name of the file is
        # part of the directory listing, not part of the inode itself.

        with open(self._path, "rb") as f:
            for offset, chunk in FixedChunker(f):
                blob = Blob(chunk)
                blobid = yield blob.backup()
                msgpack.pack((b'd', offset, blobid), buf)

        buf.seek(0)
        self._objid = yield buf

        # This object has been committed to the data store. Now add it to the
        # file cache
        self._cache.set_file_cache(
            self._path,
            stat.st_ino,
            stat.st_mtime_ns,
            stat.st_size,
            self._objid,
        )

        return self._objid

    def restore(self):
        pass # TODO

    def verify(self):
        pass # TODO

class Blob:
    """A blob object is just a blob of data, representing a portion of a file.

    """
    def __init__(self, data):
        """Initialize a blob object from a chunk of data

        data is any bytes-like object representing the raw data
        """
        self.data = data

    def backup(self):
        """Returns the msgpacked representation of this blob, as an in-memory
        bytes buffer

        """
        buf = io.BytesIO()
        msgpack.pack(b'b', buf)
        msgpack.pack((b'd', self.data), buf)
        buf.seek(0)
        return buf

    def restore(self):
        pass # TODO

    def verify(self):
        pass # TODO

