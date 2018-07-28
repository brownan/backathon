import io
import uuid
import hmac
import json
import os.path
import zlib

import django.core.files.storage
import django.db
from django.core.exceptions import ImproperlyConfigured
from django.utils.functional import cached_property
from django.utils.text import slugify

import umsgpack

from .util import atomic_immediate
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

    def get(self, item, default=None):
        value = models.Setting.get(item, using=self.alias, default=default)
        return json.loads(value)

    def __setitem__(self, key, value):
        value = json.dumps(value)
        models.Setting.set(key, value, using=self.alias)

    def __contains__(self, item):
        return models.Setting.objects.using(self.alias).filter(key=item).exists()


class SimpleSetting:
    """A descriptor class that is used to define a getter+setter on the
    Repository class that reads/writes a simple (immutable) value from the
    database

    """

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def __get__(self, instance, owner):
        if instance is None:
            return self

        try:
            return instance.__dict__[self.name]
        except KeyError:
            try:
                value = instance.settings[self.name]
            except KeyError:
                value = self.default

            instance.__dict__[self.name] = value
            return value

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value
        instance.settings[self.name] = value


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
        # Create the database connection and register it with Django.
        # In-memory databases are not recommended because separate threads
        # will not share the same in-memory database.
        if dbfile != ":memory:":
            dbfile = os.path.abspath(dbfile)
        self.db = slugify(dbfile)  # Something unique for this file
        config = {'ENGINE': 'backathon.sqlite3_backend', 'NAME': dbfile, }
        if self.db not in django.db.connections.databases:
            django.db.connections.databases[self.db] = config

        # Initialize our settings object
        self.settings = Settings(self.db)

        # Make sure the database has all the migrations applied
        self._migrate()

    @property
    def conn(self):
        # Shortcut for this database connection
        return django.db.connections[self.db]

    ##########################
    # The next set of properties and methods manipulate the utility classes
    # that are used by this class
    ##########################

    # This sets the size threshold for inline file contents. Below this
    # size, files and their metadata are backed up into a single object in
    # the repository. Larger than this and the contents and metadata are
    # separated.
    #
    # Currently set at 2 megabytes by default, this was chosen as a value
    # that will catch a majority of small files. Small files probably won't
    # have much deduplication potential and thus aren't worth the additional
    # overhead of creating two objects in the repository for the one backed
    # up file.
    #
    # Good values for this probably range from between 1 and 10 megabytes.
    backup_inline_threshold = SimpleSetting("BACKUP_INLINE_THRESHOLD", 2 ** 21)

    @cached_property
    def encrypter(self):
        data = self.settings['ENCRYPTION_SETTINGS']
        cls_name = data['class']
        settings = data['settings']

        cls = {"none": encryption.NullEncryption,
               "nacl": encryption.NaclSealedBox, }[cls_name]

        return cls.init_from_private(settings)

    def set_encrypter(self, encrypter):
        """Sets this repo's encrypter instance

        Saves the encrypter settings in the repo metadata

        :type encrypter: encryption.BaseEncryption
        """
        cls_name = {encryption.NullEncryption: "none",
                    encryption.NaclSealedBox: "nacl", }[type(encrypter)]
        settings = encrypter.get_private_params()

        self.settings['ENCRYPTION_SETTINGS'] = {'class': cls_name,
                                                'settings': settings, }
        self.__dict__["encrypter"] = encrypter

    @cached_property
    def compression(self):
        try:
            return self.settings['COMPRESSION_ENABLED']
        except KeyError:
            return False

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
        self.settings['STORAGE_SETTINGS'] = {'class': cls_name,
                                             'settings': settings, }

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
        conn = django.db.connections[self.db]
        executor = MigrationExecutor(conn)
        executor.loader.check_consistent_history(conn)
        if executor.loader.detect_conflicts():
            raise RuntimeError("Migration conflict")
        targets = executor.loader.graph.leaf_nodes("backathon")
        executor.migrate(targets)

    def compress_bytes(self, b):
        """Compress a byte-like object

        Returns the bytes unchanged if compression isn't enabled.

        All outgoing data to be written to the repository is passed through
        this method before being encrypted then uploaded.
        """
        if self.compression:
            return zlib.compress(b)
        else:
            return b

    def decompress_bytes(self, b):
        """Decompress a byte-like object

        Detects whether compression was used by whether it starts with the
        zlib magic byte 0x78.
        """
        # Detect the compression used.
        # Zlib compression always starts with byte 0x78
        # Since our messages always start with a msgpack'd string specifying
        # the object type, messages always start with one of 0xd9, 0xda, 0xdb,
        # or bytes 0xa0 through 0xbf (see the msgpack specification).
        # Therefore, we can unambiguously detect whether compression is used
        if b[0] == 0x78:
            return zlib.decompress(b)
        return b

    def _get_path(self, objid):
        """Returns the path to use in the remote repository for the given
        objid

        """
        # A prefix of the first 3 hex digits will give 16^3=4096 possible
        # object directories. This was chosen such that for a repository of
        # ten million objects, each directory has on the order of 10^3
        # objects per directory. Should be manageable for most filesystems.
        objid_hex = objid.hex()
        return "objects/{}/{}".format(objid_hex[:3], objid_hex, )

    ################################
    # These next methods are the low level interface used by the scanning,
    # backup, and restore routines.
    ################################

    def push_object(self, payload, obj, relations):
        """Pushes the given payload to the remote repository.

        Atomically commits the given models.Object and related ObjectRelation
        instances to the database if the upload was successful.

        The payload is hashed to determine the object ID. If an ID
        already exists in the Object table, implying it already exists in the
        remote repository, then it is not uploaded, and the existing object
        is returned instead.

        Otherwise, the payload is uploaded, the obj and relation
        instances are saved, and the newly saved models.Object
        instance is returned.

        :param payload: The file-like object to push to the remote data store
        :type payload: io.BytesIO

        :param obj: An unsaved Object that corresponds to the given payload
        :type obj: models.Object

        :param relations: A list of obj relations that should be saved along
        with the given Object instance
        :type relations: list[models.ObjectRelations]

        :returns: The new or existing Object
        :rtype: models.Object

        """
        view = payload.getbuffer()
        objid = self.encrypter.calculate_objid(view)

        with atomic_immediate():
            try:
                obj = models.Object.objects.using(self.db).get(objid=objid)
            except models.Object.DoesNotExist:
                # Object wasn't in the database.
                obj.objid = objid
                obj.uploaded_size = len(view)
                obj.save(using=self.db, force_insert=True)
                for r in relations:
                    r.parent = obj
                models.ObjectRelation.objects.using(self.db).bulk_create(
                    relations
                )

                to_upload = self.encrypter.encrypt_bytes(
                    self.compress_bytes(
                        view
                    )
                )

                self.storage.upload_file(
                    self._get_path(objid),
                    util.BytesReader(to_upload),
                )

        return obj

    def get_object(self, objid, key=None):
        """Retrieves the object from the remote datastore.

        :param objid: The object ID to retrieve
        :type objid: bytes

        :param key: The key to decrypt files if decryption was enabled
        :type key: None | nacl.public.PrivateKey

        :returns: The decrypted, decompressed payload
        :rtype: bytes

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
                self.encrypter.decrypt_bytes(file.read(), key))
        except Exception as e:
            raise CorruptedRepository(
                "Failed to read object {}: {}".format(objid.hex(), e)) from e

        digest = self.encrypter.calculate_objid(contents)
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
        umsgpack.pack(
            {"date": snapshot.date.timestamp(), "root": snapshot.root_id,
             "path": snapshot.path, }, contents)
        contents.seek(0)
        to_upload = self.encrypter.encrypt_bytes(
            self.compress_bytes(contents.getbuffer()))
        self.storage.upload_file(path, util.BytesReader(to_upload))

    ############################
    # These next methods define the high level interface to this repository.
    # These methods are meant to be called from the UI code.
    ############################
    def scan(self, skip_existing=False, progress=None):
        """Scans the backup set

        The backup set is the set of files and directories starting at the
        root paths.

        See more info in the backathon.scan module
        """
        from . import scan
        scan.scan(alias=self.db, progress=progress, skip_existing=skip_existing)

    def add_root(self, root_path):
        """Adds a new root path to the backup set

        This just adds the root. The caller may want to call
        scan(skip_existing=True) afterwards to update the local filesystem
        cache.

        If this entry is already a root or is a descendant of an existing
        root, this call raises an IntegrityError
        """
        root_path = os.path.abspath(root_path)
        models.FSEntry.objects.using(self.db).create(path=root_path)

    def del_root(self, root_path):
        root_path = os.path.abspath(root_path)
        entry = models.FSEntry.objects.using(self.db)\
            .filter(parent__isnull=True)\
            .get(path=root_path)
        entry.delete()

    def get_roots(self):
        return models.FSEntry.objects.using(self.db).filter(parent__isnull=True)

    def backup(self, progress=None):
        """Perform a backup

        See documentation in the backathon.backup module

        """
        try:
            _ = self.encrypter
        except KeyError:
            raise ImproperlyConfigured("You must configure the encryption "
                                       "first")

        try:
            _ = self.storage
        except KeyError:
            raise ImproperlyConfigured("You must configure the storage "
                                       "backend first")

        from . import backup
        backup.backup(self, progress)

    def save_metadata(self):
        """Updates the metadata file in the remote repository

        Callers should call this after changing any parameters such as the
        encryption settings. Having up to date metadata in the remote
        repository is essential for recovering from a complete loss of the
        local cache
        """
        data = {"encryption": self.encrypter.get_public_params(),
                "compression": self.compression, }
        buf = io.BytesIO(json.dumps(data).encode("utf-8"))
        self.storage.upload_file("backathon.json", buf)

    def restore(self, obj, path, password):
        """Restores the given object to the given path

        See docstring on the restore.restore_item() function for more details.
        """
        key = self.encrypter.get_decryption_key(password)

        from . import restore
        restore.restore_item(self, obj, path, key)
