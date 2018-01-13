import hashlib

import django.core.files.storage
from django.db.transaction import atomic

import umsgpack

from gbackup import models

class DataStore:
    """This class acts as an interface to the storage backend

    It has logic to keep the local cache in sync with the objects stored on
    the backend.
    """
    def __init__(self):
        # TODO: pull settings for these from the config or database
        self.storage = django.core.files.storage.default_storage
        self.hasher = hashlib.sha256

    @staticmethod
    def _get_obj_type(buf):
        pos = buf.tell()
        try:
            return umsgpack.unpack(buf)
        finally:
            buf.seek(pos)

    def push_object(self, payload, children):
        """Pushes the given payload as a new object into the object store

        :param payload: The file-like object to push to the remote data store
        :type payload: io.BytesIO

        :param children: A list of Objects that should be added as children
        of this object if we have to create the object.

        :returns: The new or existing Object
        :rtype: models.Object
        """
        objtype = self._get_obj_type(payload)
        view = payload.getbuffer()
        objid = self.hasher(view).hexdigest()

        with atomic():
            obj_instance, isnew = models.Object.objects.get_or_create(
                objid=objid,
                defaults={
                    'payload':view if objtype != "blob" else None,
                },
            )

            if isnew:
                obj_instance.children.set(children)
                name = "objects/{}/{}".format(
                    objid[:2],
                    objid,
                )
                self.storage.save(name, payload)
            else:
                # At the cost of another database query, we do a sanity check
                # here
                assert set(children) == set(obj_instance.children.all())

        return obj_instance

    def get_object(self, name):
        """Retrieves the object. Checks the cache first."""
        pass # TODO

    def exists(self, objname):
        pass # TODO

    def delete_object(self, objname):
        pass # TODO

    def rebuild_obj_cache(self):
        """Rebuilds the entire local object cache from the remote data store

        This is what you use if your local cache is missing or corrupt

        Performs these steps:
        1) Downloads all snapshot index files from the remote storage
        2) Downloads the referenced object files from the remote store
        3) Parses the object payloads, and recurses to each referenced object

        """
        pass # TODO

    def verify_cache(self):
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
        """Adds a new snapshot index file to the storage backend"""
        pass # TODO

    def get_snapshot_list(self):
        """Gets a list of snapshots"""
