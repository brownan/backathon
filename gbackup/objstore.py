"""
The object store abstraction sits on top of the storage layer and handles
encryption, compression, and hashing of objects in the object store.
"""

class ObjStore:
    """The object store is an interface to read and write objects in the
    underlying storage backend.

    All objects reside in a subdirectory called /objects under the storage
    backend's root.

    """
    def __init__(self, storage, encryption, compression):
        self._storage = storage
        self._encryption = encryption
        self._compression = compression

    def get_object(self, objname, f):
        """Gets an object. Streams the object contents into the file-like
        object `f`

        :param f: A file-like object open for writing
        :type f: io.IOBase
        """
        raise NotImplementedError()

    def put_object(self, f):
        """Writes an object. Returns the hash it was stored with

        :param f: A file-like object open for reading
        :type f: io.IOBase
        """
        raise NotImplementedError()

