"""
The object store abstraction sits on top of the storage layer and handles
encryption, compression, and hashing of objects in the object store.
"""
import os.path

from .objcache import ObjCache

class ObjStore:
    """The object store is an interface to read and write objects in the
    underlying storage backend.

    All objects reside in a subdirectory called /objects under the storage
    backend's root.

    """
    def __init__(self, metadatadir):
        self.metadatadir = metadatadir

        self._storage = ...
        self._encryption = ...
        self._compression = ...

        self.cache = ObjCache(
            os.path.join(self.metadatadir, "cache.sqlite3")
        )

    def get_object(self, objname):
        """Gets an object.

        :rtype: gbackup.objects.Object
        """
        raise NotImplementedError()

    def put_object(self, f):
        """Writes an object. Returns the hash it was stored with

        :param f: A file-like object open for reading
        :type f: io.IOBase
        """
        raise NotImplementedError()

