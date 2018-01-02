import django.core.files.storage

class Backend:
    """This class acts as an interface to the storage backend

    It has logic to keep the local cache in sync with the objects stored on
    the backend.
    """
    def __init__(self, storage):
        assert isinstance(storage, django.core.files.storage.Storage)
        self.storage = storage


    def push_object(self, name, payload):
        pass # TODO

    def get_object(self, name):
        """Retrieves the object. Checks the cache first."""

    def exists(self, objname):
        pass # TODO

    def rebuild_obj_cache(self):
        """Rebuilds the entire local object cache from the remote data store

        This is what you use if your local cache is missing or corrupt
        """
        pass # TODO

    def check_cache(self):
        """Walks the local tree of objects and makes sure we have everything
        we should. This performs a sanity check on the local database
        consistency and integrity. If anything comes up wrong here, it could
        indicate a bigger problem somewhere.

        * Checks that all tree objects have objects for each child
        * Checks that all tree and inode objects have a readable payload in
          the cache
        * Checks that all blob objects referenced by inodes exist in the cache
        """
        pass # TODO

    def put_snapshot(self, snapshot):
        pass # TODO

    def get_snapshot_list(self):
        pass