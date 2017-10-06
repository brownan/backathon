"""Classes in this module represent objects in the object store

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

import msgpack

from .chunker import FixedChunker

class Tree:
    """A tree object represents a directory

    """
    def __init__(self, directory):
        self.directory = directory

    def update(self):
        pass # TODO

    def scan(self):
        pass # TODO

    def backup(self):
        pass # TODO

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

    def update(self):
        """Scans the local filesystem to see if the file has changed. If so,
        this file will be backed up on the next call to backup().

        This method always performs exactly one os.stat() call.

        This method is designed to be called from some asynchronous
        notification service such as Linux's inotify subsystem to inform this
        object that it needs updating. If this strategy is taken, this method
        should still be recursively called on every object once in a while in
        case some changes were missed. In particular, it should be called on
        every object when first instantiated.

        :returns: Returns the size that needs to be updated (or at least
            scanned for updates), else returns None. This lets callers
            recursively get a feel for the size of the dataset.

        """
        # This turns out to be pretty simple. If the file's current stats
        # match an entry in the file cache, then we assume the object
        # representing that file is still fully uploaded
        stat = os.stat(self._path)
        objid = self._cache.get_file_cache(
            self._path,
            stat.st_ino,
            stat.st_mtime_ns,
            stat.st_size,
        )
        self._objid = objid

        if objid is None:
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

        :returns: the object id for this inode object
        """

        # Short circuit the entire method for quick incremental backups. If
        # objid is already set, then we've already been backed up once.
        # Assume the file hasn't changed. (Call update() to scan the local
        # filesystem for changes to this file)
        if self._objid is not None:
            return self._objid

        # Don't have an object ID cached? We need to compute this file's
        # object ID by reading in the contents.

        stat = os.stat(self._path)

        buf = io.BytesIO()
        msgpack.pack(b'i', buf)
        msgpack.pack((b'p', self._path), buf)
        msgpack.pack((b's', stat.st_size), buf)
        msgpack.pack((b'i', stat.st_ino), buf)
        msgpack.pack((b'u', stat.st_uid), buf)
        msgpack.pack((b'g', stat.st_gid), buf)
        msgpack.pack((b'm', stat.st_mode), buf)
        msgpack.pack((b'ct', stat.st_ctime_ns), buf)
        msgpack.pack((b'mt', stat.st_mtime_ns), buf)

        chunks = []
        with open(self._path, "rb") as f:
            for offset, chunk in FixedChunker(f):
                blob = Blob(chunk)
                blobid = yield blob.backup()
                chunks.append((offset, blobid))
        msgpack.pack((b'd', chunks), buf)

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
        pass

    def verify(self):
        pass

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
        pass

    def verify(self):
        pass

