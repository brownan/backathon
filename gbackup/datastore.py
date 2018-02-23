import hashlib

import django.core.files.storage
from django.core.exceptions import ImproperlyConfigured
from django.db.transaction import atomic

from gbackup import models

class CorruptedRepositoryException(Exception):
    pass

class DataStore:
    """This class acts as an interface to the storage backend

    It has logic to keep the local cache in sync with the objects stored on
    the backend.
    """
    def __init__(self):
        backend = models.Setting.get("REPO_BACKEND")
        if backend == "local":
            self.storage = django.core.files.storage.FileSystemStorage(
                location=models.Setting.get("REPO_PATH")
            )
        else:
            raise ImproperlyConfigured("Invalid repository backend defined in "
                                       "settings: {}".format(backend))

        self.hasher = hashlib.sha256

    def push_object(self, payload, children):
        """Pushes the given payload as a new object into the object store.

        Returns the newly created models.Object instance.

        If the payload already exists in the remote data store, then it is
        not uploaded, and the existing object is returned instead.

        :param payload: The file-like object to push to the remote data store
        :type payload: io.BytesIO

        :param children: A list of Objects that should be added as children
        of this object if we have to create the object.

        :returns: The new or existing Object
        :rtype: models.Object

        Note that since this routine is called during backup, we expect all
        dependent objects to be in the database already.
        """
        view = payload.getbuffer()
        objid = self.hasher(view).digest()

        with atomic():
            try:
                obj_instance = models.Object.objects.get(
                    objid=objid
                )
            except models.Object.DoesNotExist:
                # Object wasn't in the database. Create it.
                obj_instance = models.Object(objid=objid)
                obj_instance.load_payload(view)
                obj_instance.save()

                obj_instance.children.set(children)
                name = self._get_path(objid)
                self.storage.save(name, payload)
            else:
                # It was already in the database
                # Do a sanity check to make sure the object's payload is the
                # same as the one we found in the database. They should be
                # since the objects are addressed by the hash of their
                # payload, so this would only happen if there's a bug or
                # someone mucked with the database manually (by changing the
                # payload).
                assert view == obj_instance.payload \
                       or obj_instance.payload is None
                # At the cost of another database query, also check the
                # children match. The set of children passed in comes from
                # FSEntry.backup(), as the FSEntry's children's objects. The
                # the Object keeps its own list of children which should be the
                # same.
                assert set(children) == set(obj_instance.children.all())

        return obj_instance

    def _get_path(self, objid):
        """Returns the path for the given objid"""
        return "objects/{}/{}".format(
            objid[:2],
            objid,
        )

    def get_object(self, objid):
        """Retrieves the object from the remote datastore.

        Returns an open file-like object with the Object's payload, decrypted
        and verified if applicable.
        """
        file = self.storage.open(self._get_path(objid))
        hasher = self.hasher()
        for chunk in file.chunks():
            hasher.update(chunk)
        digest = hasher.digest()
        if digest != objid:
            raise CorruptedRepositoryException("Object payload does not "
                                               "match its hash for objid "
                                               "{}".format(objid))
        file.seek(0)
        return file

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

def get_datastore():
    return DataStore()
