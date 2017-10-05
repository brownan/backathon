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
    def __init__(self, path):
        """

        :param path: is the absolute path to the file on the local
        filesystem.
        """
        self._path = path

        # This object's objid. It is cached in this instance variable so that
        # subsequent backups are quick if nothing has changed. If we are
        # updated, this is invalidated.
        self._objid = None

    def update(self):
        """Called when an external event modifies this file.

        This is used as a callback to let this object know the file has been
        modified, and next backup it should be checked for changes.

        """
        self._objid = None

    def scan(self, update=False):
        """Return the size of this file.

        Used to gather information on the size of a backup set, for progress
        reporting.

        If this file does not need updating, returns None.

        :param update: if true, assumes this file needs updating. Otherwise,
            only return the file size if this is the first call or if update()
            was called since the last backup.
        """
        if update or self._objid is None:
            stat = os.stat(self._path)
            return stat.st_size
        return None

    def backup(self, cache, update=False):
        """Backs up the given file

        This is a generator function.

        Yields binary strings that represent objects that need to be
        saved to the data store. Expects the resulting object ID to be sent
        back into the iterator.

        Returns the object ID of this object.

        If update is True, does an os.stat() on the file and compares it
        against the local file cache to determine whether the file needs
        backing up.

        If update is False, assumes the file hasn't changed unless this is
        the first call or if update() has been called since the last backup.

        If this object determines a backup is not necessary, it returns the
        cached object id that was determined from the first backup.

        :param cache: The ObjCache used to determine if a file on the local
            filesystem has changed.
        :type cache: gbackup.objcache.ObjCache
        :param update: Check whether the file has changed
        :type update: bool

        :returns: the object id for this inode object
        """
        if update:
            self._objid = None

        # Short circuit the entire method for quick incremental backups. If
        # objid is already set, then we've already been backed up once.
        # Assume the file hasn't changed.
        if self._objid:
            return self._objid

        stat = os.stat(self._path)

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
        self._objid = yield buf
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

