import io
import uuid
import hashlib
import hmac
import json

import django.core.files.storage
from django.core.exceptions import ImproperlyConfigured
from django.db.transaction import atomic
from django.utils.functional import SimpleLazyObject, cached_property

import nacl.public
import nacl.pwhash.argon2id
import nacl.secret
import nacl.utils

import umsgpack

from gbackup import models
from gbackup import util
from gbackup.exceptions import CorruptedRepository
from gbackup.signals import db_setting_changed

class KeyRequired(Exception):
    pass

class DataStore:
    """This class acts as an interface to the storage backend

    It has logic to keep the local cache in sync with the objects stored on
    the backend.

    """
    def __init__(self):

        db_setting_changed.connect(self._clear_cached_properties)

    def initialize(self, encryption, compression, repo_backend,
                   repo_path, password=None):
        """Initializes a new repository.

        This sets local settings, and also uploads necessary metadata files
        to the remote repository

        :param encryption: A string indicating what kind of encryption to use
        :param compression: A string indicating what kind of compression to use
        :param repo_backend: A string indicating what storage backend to use
        :param repo_path: A string specifying parameters to the storage backend
        :param password: A string indicating the password securing the secret
        key, if encryption is used

        """
        with atomic():
            models.Setting.set("REPO_BACKEND", repo_backend)
            models.Setting.set("REPO_PATH", repo_path)
            models.Setting.set("COMPRESSION", compression)
            models.Setting.set("ENCRYPTION", encryption)

            if encryption == "nacl":
                # Derive a secret key from the password
                salt = nacl.utils.random(nacl.pwhash.argon2id.SALTBYTES)
                ops = nacl.pwhash.argon2id.OPSLIMIT_SENSITIVE
                mem = nacl.pwhash.argon2id.MEMLIMIT_SENSITIVE

                symmetrickey = nacl.pwhash.argon2id.kdf(
                    nacl.secret.SecretBox.KEY_SIZE,
                    password.encode("UTF-8"),
                    salt=salt,
                    opslimit=ops,
                    memlimit=mem,
                )

                # Generate a new key pair
                key = nacl.public.PrivateKey.generate()

                # Encrypt the private key's bytes using the symmetric key
                encrypted_private_key = nacl.secret.SecretBox(
                    symmetrickey
                ).encrypt(bytes(key))

                # This info dict will be stored in plain text locally and
                # remotely. It should not contain any secret parameters
                info = {
                    'salt': salt.hex(),
                    'ops': ops,
                    'mem': mem,
                    'key': encrypted_private_key.hex()
                }

                models.Setting.set("KEY", json.dumps(info))
                models.Setting.set("PUBKEY", bytes(key.public_key).hex())

                metadata = io.BytesIO(
                    json.dumps(info).encode("UTF-8")
                )

                self.storage.save("gbackup.config", metadata)

    def get_remote_privatekey(self, password):
        """Retrieves the private key from the remote store, decrypts it,
        and returns the PrivateKey object for passing in to the decrypt_bytes()
        routine

        :rtype: nacl.public.PrivateKey

        """
        info = json.load(self.storage.open("gbackup.config"))
        return self._decrypt_privkey(info, password)

    def get_local_privatekey(self, password):
        """Retrieves the private key from the local cache, decrypting it with
        the given password

        :rtype: nacl.public.PrivateKey
        """
        info = json.loads(models.Setting.get("KEY"))
        return self._decrypt_privkey(info, password)

    def _decrypt_privkey(self, info, password):
        salt = bytes.fromhex(info['salt'])
        ops = info['ops']
        mem = info['mem']
        encrypted_private_key = bytes.fromhex(info['key'])

        # Re-derive the symmetric key from the password
        symmetrickey = nacl.pwhash.argon2id.kdf(
            nacl.secret.SecretBox.KEY_SIZE,
            password.encode("UTF-8"),
            salt=salt,
            opslimit=ops,
            memlimit=mem,
        )

        # Decrypt the key
        decrypted_key_bytes = nacl.secret.SecretBox(symmetrickey).decrypt(encrypted_private_key)

        return nacl.public.PrivateKey(decrypted_key_bytes)


    def _clear_cached_properties(self, setting, **kwargs):
        """Since there is one instance of this object per process, we have to
        reconfigure when a setting is changed. This happens mostly when
        running tests."""

        if setting == "REPO_BACKEND":
            self.__dict__.pop('storage', None)

        elif setting == "REPO_PATH":
            self.__dict__.pop('storage', None)

        elif setting == "ENCRYPTION":
            self.__dict__.pop('encrypt_bytes', None)
            self.__dict__.pop('decrypt_bytes', None)
            self.__dict__.pop('hasher', None)

        elif setting == "PUBKEY":
            self.__dict__.pop('pubkey', None)

        elif setting == "COMPRESSION":
            self.__dict__.pop('compress_bytes', None)
            self.__dict__.pop('decompress_bytes', None)

    @cached_property
    def pubkey(self):
        key_hex = models.Setting.get("PUBKEY")
        key = bytes.fromhex(key_hex)
        return nacl.public.PublicKey(key)

    @cached_property
    def storage(self):
        backend = models.Setting.get("REPO_BACKEND")
        if backend == "local":
            return django.core.files.storage.FileSystemStorage(
                location=models.Setting.get("REPO_PATH")
            )
        else:
            raise ImproperlyConfigured("Invalid repository backend defined in "
                                       "settings: {}".format(backend))

    @cached_property
    def hasher(self):
        """Returns a hasher used to hash bytes into a digest

        If encryption is enabled, this is an hmac object partially evaluated
        to include the key.
        """
        encryption = models.Setting.get("ENCRYPTION")
        if encryption == "nacl":
            return lambda b=None: hmac.new(bytes(self.pubkey), msg=b,
                                           digestmod=hashlib.sha256)
        elif encryption == "none":
            return hashlib.sha256

        else:
            raise ImproperlyConfigured("Bad encryption type '{}'".format(encryption))

    @cached_property
    def encrypt_bytes(self):
        """Returns a function that will encrypt the given bytes depending on
        the encryption configuration"""
        encryption = models.Setting.get("ENCRYPTION")
        if encryption == "nacl":
            # Call bytes() on the input. If it's a memoryview or other
            # bytes-like object, pynacl will reject it.
            return lambda b: nacl.public.SealedBox(self.pubkey).encrypt(bytes(b))

        elif encryption == "none":
            return lambda b: b

        else:
            raise ImproperlyConfigured("Bad encryption type '{}'".format(encryption))

    @property
    def key_required(self):
        """True if decryption routines will require a key"""
        return models.Setting.get("ENCRYPTION") == "nacl"

    @cached_property
    def decrypt_bytes(self):
        """Returns a function that takes a bytes object and an optional key,
        and returns the decrypted bytes

        Raises a KeyRequired error if the key was not provided but is needed to
        decrypt the contents
        """
        encryption = models.Setting.get("ENCRYPTION")
        if encryption == "nacl":
            def _decrypt(b, key=None):
                if key is None:
                    raise KeyRequired("An encryption key is required")
                return nacl.public.SealedBox(key).decrypt(b)
            return _decrypt

        elif encryption == "none":
            return lambda b, k=None: b

        else:
            raise ImproperlyConfigured("Bad encryption type '{}'".format(encryption))

    def _get_compression_functions(self):
        compression = models.Setting.get("COMPRESSION")
        if compression == "zlib":
            import zlib
            pair = (zlib.compress, zlib.decompress)
        elif compression == "none":
            pair = (lambda b: b), (lambda b: b)
        else:
            raise ImproperlyConfigured("Bad compression type '{}'".format(
                compression))

        return pair

    @cached_property
    def compress_bytes(self):
        return self._get_compression_functions()[0]

    @cached_property
    def decompress_bytes(self):
        return self._get_compression_functions()[1]

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

                # Note, there could be duplicate children so we have to
                # deduplicate to avoid a unique constraint violation
                models.ObjectRelation.objects.bulk_create([
                    models.ObjectRelation(
                        parent=obj_instance,
                        child_id=c,
                    ) for c in set(child.objid for child in children)
                ])

                name = self._get_path(objid)

                to_upload = self.encrypt_bytes(
                    self.compress_bytes(view)
                )
                self.storage.save(name, util.BytesReader(to_upload))
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
        objid_hex = objid.hex()
        return "objects/{}/{}".format(
            objid_hex[:2],
            objid_hex,
        )

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
        or a problem decrypting the payload..

        """
        try:
            file = self.storage.open(self._get_path(objid))
            contents = self.decompress_bytes(
                self.decrypt_bytes(
                    file.read(),
                    key
                )
            )
        except Exception as e:
            raise CorruptedRepository("Failed to read object {}: {}".format(
                objid.hex(), e
            )) from e

        digest = self.hasher(contents).digest()
        if not hmac.compare_digest(digest, objid):
            raise CorruptedRepository("Object payload does not "
                                      "match its hash for objid "
                                      "{}".format(objid))
        return contents

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
        to_upload = self.encrypt_bytes(
            self.compress_bytes(
                contents.getbuffer()
            )
        )
        self.storage.save(path, util.BytesReader(to_upload))

    def get_snapshot_list(self):
        """Gets a list of snapshots"""


default_datastore = SimpleLazyObject(lambda: DataStore()) # type: DataStore
