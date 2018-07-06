import io
import uuid
import hmac
import json
import os.path
import zlib

import django.core.files.storage
import django.db
from django.db.transaction import atomic
from django.utils.functional import cached_property
from django.utils.text import slugify

import umsgpack

from . import models
from . import util
from .exceptions import CorruptedRepository
from . import encryption
from . import storage

class KeyRequired(Exception):
    pass

class Settings:
    """A loose proxy for the Settings database model that does json
    encoding/decoding

    """
    def __init__(self, alias):
        self.alias = alias

    def __getitem__(self, item):
        value = models.Setting.get(item, using=self.alias)
        return json.loads(value)

    def __setitem__(self, key, value):
        models.Setting.set(key, value, using=self.alias)


###########################
###########################

class Repository:
    """This class acts as an interface to the storage backend as well as all
    operations that are performed on the repository. It also manages the
    local cache database.

    Note: creating a new instance of this class registers a new database with
    Django. There's not really a clean way to un-register databases and close
    old connections, so avoid creating short lived Repository objects,
    or database connections are likely to be left open.

    """

    def __init__(self, dbfile):
        # Create the database connection and register it with Django
        dbfile = os.path.abspath(dbfile)
        self.alias = slugify(dbfile) # Something unique for this file
        config = {
            'ENGINE': 'backathon.sqlite3_backend',
            'NAME': dbfile,
        }
        if self.alias not in django.db.connections.databases:
            django.db.connections.databases[self.alias] = config

        # Initialize our settings object
        self.settings = Settings(self.alias)

        # Make sure the database has all the migrations applied
        self._migrate()

    ##########################
    # The next set of properties and methods manipulate the utility classes
    # that are used by this class
    ##########################

    @cached_property
    def encrypter(self):
        data = self.settings['ENCRYPTION_SETTINGS']
        cls_name = data['class']
        settings = data['settings']

        cls = {
            "none": encryption.NullEncryption,
            "nacl": encryption.NaclSealedBox,
        }[cls_name]

        return cls.init_from_private(settings)

    def set_encrypter(self, cls_name, settings):
        # Save new settings
        self.settings['ENCRYPTION_SETTINGS'] = {
            'class': cls_name,
            'settings': settings
        }

        # Re-initialize the encrypter object
        self.__dict__.pop("encrypter", None)
        return self.encrypter

    @cached_property
    def compression(self):
        return self.settings['COMPRESSION_ENABLED']

    def set_compression(self, enabled):
        enabled = bool(enabled)
        self.settings['COMPRESSION_ENABLED'] = enabled
        self.__dict__['compression'] = enabled
        return enabled

    @cached_property
    def storage(self):
        data = self.settings['STORAGE_SETTINGS']

        cls_name = data['class']
        settings = data['settings']

        if cls_name == "local":
            cls = storage.FilesystemStorage
        elif cls_name == "b2":
            from .b2 import B2Bucket
            cls = B2Bucket
        else:
            raise KeyError("Unknown storage class {}".format(cls_name))

        return cls(**settings)

    def set_storage(self, cls_name, settings):
        self.settings['STORAGE_SETTINGS'] = {
            'class': cls_name,
            'settings': settings,
        }

        self.__dict__.pop("storage", None)
        return self.storage

    ################
    # Some private utility methods
    ################

    def _migrate(self):
        """Runs migrate on the given database

        If this is a new database, it's created and the tables are populated

        If this is an existing database, makes sure all migrations are applied

        """
        # This workflow is simplified down to just what we need from the
        # "migrate" management command
        from django.db.migrations.executor import MigrationExecutor
        conn = django.db.connections[self.alias]
        executor = MigrationExecutor(conn)
        executor.loader.check_consistent_history(conn)
        if executor.loader.detect_conflicts():
            raise RuntimeError("Migration conflict")
        targets = executor.loader.graph.leaf_nodes("backathon")
        executor.migrate(targets)

    def compress_bytes(self, b):
        if self.compression:
            return zlib.compress(b)
        else:
            return b

    def decompress_bytes(self, b):
        # Detect the compression used.
        # Zlib compression always starts with byte 0x78
        # Since our messages always start with a msgpack'd string specifying
        # the object type, messages always start with one of 0xd9, 0xda, 0xdb,
        # or bytes 0xa0 through 0xbf.
        # Therefore, we can unambiguously detect whether compression is used
        if b[0] == 0x78:
            return zlib.decompress(b)
        return b

    def _get_path(self, objid):
        """Returns the path for the given objid"""
        objid_hex = objid.hex()
        return "objects/{}/{}".format(
            objid_hex[:3],
            objid_hex,
        )

    ################################
    # The next methods define the public interface to this class
    ################################

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
        objid = self.encrypter.get_object_id(view)

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

                # Note, there could be duplicate children so we have to
                # deduplicate to avoid a unique constraint violation
                models.ObjectRelation.objects.bulk_create([
                    models.ObjectRelation(
                        parent=obj_instance,
                        child_id=c,
                    ) for c in set(child.objid for child in children)
                ])

                name = self._get_path(objid)

                to_upload = self.encrypter.encrypt_bytes(
                    self.compress_bytes(view)
                )
                self.storage.upload_file(name, util.BytesReader(to_upload))
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

    def get_object(self, objid, key=None):
        """Retrieves the object from the remote datastore.

        :param objid: The object ID to retrieve
        :type objid: bytes

        :param key: The key to decrypt files if decryption was enabled
        :type key: None | nacl.public.PrivateKey

        Returns a bytes-like object containing the Object's payload. This
        method takes care of decrypting and decompressing, if applicable. It
        also verifies the payload's hash matches the object id.

        A CorruptedRepository exception is raised if there is a problem
        retrieving this object's payload, such as the checksum not matching
        or a problem decrypting the payload.

        """
        try:
            _, file = self.storage.download_file(self._get_path(objid))
            contents = self.decompress_bytes(
                self.encrypter.decrypt_bytes(
                    file.read(),
                    key
                )
            )
        except Exception as e:
            raise CorruptedRepository("Failed to read object {}: {}".format(
                objid.hex(), e
            )) from e

        digest = self.encrypter.get_object_id(contents)
        if not hmac.compare_digest(digest, objid):
            raise CorruptedRepository("Object payload does not "
                                      "match its hash for objid "
                                      "{}".format(objid))
        return contents

    def put_snapshot(self, snapshot):
        """Adds a new snapshot index file to the storage backend

        :type snapshot: models.Snapshot
        """
        path = "snapshots/" + str(uuid.uuid4())
        contents = io.BytesIO()
        umsgpack.pack("snapshot", contents)
        umsgpack.pack({
            "date": snapshot.date.timestamp(),
            "root": snapshot.root_id,
            "path": snapshot.path,
        }, contents)
        contents.seek(0)
        to_upload = self.encrypter.encrypt_bytes(
            self.compress_bytes(
                contents.getbuffer()
            )
        )
        self.storage.upload_file(path, util.BytesReader(to_upload))

