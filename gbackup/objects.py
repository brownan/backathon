"""Classes in this module represent objects in the object store

There are different types of objects. Some terminology

* An object's "contents" is the serialized representation of the data it
  represents. The contents is what's stored in the local object cache.

* An object's "payload" is the contents after it's been compressed and
  encrypted. If compression and encryption are disabled, the payload is the
  same as the contents. The payload is what's uploaded to the remote object
  store.

* An object ID is a hash or HMAC digest of the contents, and becomes the name
  of the object in the object store

Note that each object has a similar interface but they are not compatible.
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

    def scan(self):
        pass

    def backup(self):
        pass

    def restore(self):
        pass

    def verify(self):
        pass


class Inode:
    """An inode object represents a file on the filesystem

    It holds metadata about the file, and links to one or more blobs
    containing the contents for the file.
    """
    def __init__(self, path=None, objid=None):
        """

        :param path: is the absolute path to the file on the local filesystem
        """
        self._path = path
        self._objid = objid

        self._stat = None

    def scan(self):
        stat = self._stat = os.stat(self._path)
        return 1, stat.st_size

    def backup(self, cache):
        """Backs up the given file

        This is a generator function. Yields object contents that need to be
        hashed, and then if they don't exist in the remote object store,
        they also need to be compressed, encrypted, and uploaded.

        Expects the object id to be sent back to the iterator.

        Returns the object ID of this object.

        :param cache: The ObjCache used to determine if a file on the local
            filesystem has changed.
        :type cache: gbackup.objcache.ObjCache
        """
        assert self._path is not None, "backup called, but was not " \
                                       "initialized with a path"

        if self._stat is not None:
            stat = self._stat
        else:
            stat = self._stat = os.stat(self._path)

        objid = cache.get_file_cache(
            self._path,
            stat.st_ino,
            stat.st_mtime_ns,
            stat.st_size,
        )
        if objid is not None:
            return objid

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
        myid = yield buf
        return myid

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
        buf = io.BytesIO()
        msgpack.pack(b'b', buf)
        msgpack.pack((b'd', self.data), buf)
        buf.seek(0)
        return buf

    def restore(self):
        pass

    def verify(self):
        pass

