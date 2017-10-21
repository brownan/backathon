"""
The object store abstraction sits on top of the storage layer and handles
encryption, compression, and hashing of objects in the object store.
"""
import os.path
import hmac
import hashlib

class ObjStore:
    """The object store is an interface to read and write objects in the
    underlying storage backend.

    All objects reside in a subdirectory called /objects under the storage
    backend's root.

    """
    def __init__(self, storage, encryption=None, compression=None):
        """

        :param storage: The storage backend to use
        :type storage: gbackup.storage.Storage
        :param encryption: The encryption class to use
        :param compression: The compression class to use
        """

        self._storage = storage
        self._encryption = encryption
        self._compression = compression

    def get_object(self, objname):
        """Gets an object.

        :rtype: gbackup.objects.Object
        """
        raise NotImplementedError()

    def put_object(self, f):
        """Writes an object. Returns the hash it was stored with. Encrypts
        and compresses the contents before storing, if enabled.

        :param f: An in-memory BytesIO object seeked to 0
        :type f: io.BytesIO
        """
        if self._encryption is None:
            m = hashlib.sha256()
        else:
            raise NotImplementedError("hmac")


        m.update(f.getbuffer())
        hexdigest = m.hexdigest()

        self._storage.put("objects/{}/{}".format(
            hexdigest[:2], hexdigest
        ), f.getbuffer())
        return m.digest()


