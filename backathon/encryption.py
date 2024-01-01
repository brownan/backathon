import hashlib
import hmac

import nacl.exceptions
import nacl.public
import nacl.pwhash
import nacl.secret
import nacl.utils


class DecryptionError(Exception):
    pass


class BaseEncryption:
    """Base class for encryption classes

    See method documentation for info on how this API works

    Basically, it's set up for a public/private scheme where some info is
    stored locally in what is presumed to be secure storage, and some info is
    stored remotely but encrypted and is used for recovery.
    """

    password_required = True

    @classmethod
    def init_new(cls, password):
        """Generate new encryption keys using the given password"""
        raise NotImplementedError()

    @classmethod
    def init_from_public(cls, params, password):
        """Initialize this object from the public parameters

        The public parameters are stored in the remote repository and are
        used in event a full recovery is needed. Implementations are expected
        to use the password to decrypt any encrypted parameters.
        """
        raise NotImplementedError()

    @classmethod
    def init_from_private(cls, params):
        """Initialize this object from the private parameters

        The private parameters are stored locally and may contain more
        parameters than the remote repository, as it's assumed the local
        filesystem is more secure than the remote repository.

        This is the usual way the class will be initialized in normal
        operation.
        """
        raise NotImplementedError()

    def get_public_params(self):
        """Return the parameters that should be stored in the remote repository

        These parameters are stored in the remote repository which is
        presumed to be less secure. Therefore, implementations are expected
        to encrypt any sensitive parameters.

        Since the parameters stored in the remote repository may be all
        that's left after a disaster, implementations should be able to
        recover all other parameters using nothing but what's returned from
        this function and the password (presumed to be remembered by the user)

        The parameters returned from this are passed to init_from_public()
        during a recovery workflow where the local data is lost.

        """
        raise NotImplementedError()

    def get_private_params(self):
        """Return the parameters to store locally

        These parameters are stored on the local filesystem, which is
        presumed to be more secure than the remote repository.
        Implementations must store enough information locally to start up
        operation without the user's password.

        The parameters returned from this are passed to init_from_private()
        during normal startup where the local data is intact.
        """
        raise NotImplementedError()

    def encrypt_bytes(self, plaintext):
        """Encrypt a byte-like object

        :returns: A byte-like object containing the cyphertext

        All data saved to the remote repository is passed through this
        function except for a small amount of metadata (including the
        parameters that come from get_public_params)
        """
        raise NotImplementedError()

    def get_decryption_key(self, password):
        """This returns the decryption key to use for decrypt_bytes

        The API is designed with this call because the key derivation may be
        very slow, so the caller only has to call this once over many calls
        to decrypt_bytes. The result of this function is passed into
        decrypt_bytes()
        """
        raise NotImplementedError()

    def decrypt_bytes(self, cyphertext, key):
        """Decrypt a byte-like object

        :returns: A byte-like object containing the plain text
        :param cyphertext: The bytes to decrypt
        :param key: The key as returned from get_decryption_key()
        """
        raise NotImplementedError()

    def calculate_objid(self, content):
        """Hash the given object contents into an object ID

        This must return a secure hash of the given byte-like object.

        Since hashes of objects are public, this hash must also be
        authenticated using HMAC if using encryption, to prevent hash
        reversal attacks.

        :return: The byte string hash of the contents. Do not return the hex
            representation.
        :rtype: bytes
        """
        raise NotImplementedError()


class NullEncryption(BaseEncryption):
    """Performs no encryption

    Use this for local repositories, trusted repositories, or where data is
    not sensitive
    """

    password_required = False

    @classmethod
    def init_new(cls, password=None):
        return cls()

    @classmethod
    def init_from_private(cls, params):
        return cls()

    @classmethod
    def init_from_public(cls, params, password):
        return cls()

    def get_public_params(self):
        return {}

    def get_private_params(self):
        return {}

    def encrypt_bytes(self, plaintext):
        return plaintext

    def get_decryption_key(self, password):
        return None

    def decrypt_bytes(self, cyphertext, key):
        return cyphertext

    def calculate_objid(self, content):
        return hashlib.sha256(content).digest()


class NaclSealedBox(BaseEncryption):
    password_required = True

    OPSLIMIT = nacl.pwhash.argon2id.OPSLIMIT_SENSITIVE
    MEMLIMIT = nacl.pwhash.argon2id.MEMLIMIT_SENSITIVE

    def __init__(self, salt, ops, mem, pubkey, enc_privkey):
        self.salt = salt  # type: bytes
        self.ops = ops  # type: int
        self.mem = mem  # type: int
        self.pubkey = pubkey  # type: nacl.public.PublicKey
        self.enc_privkey = enc_privkey  # type: bytes

    def _get_symmetric_key(self, password):
        # This key is derived from the password and is used to encrypt
        # the private part of the generated public/private key. The encrypted
        # private key is then stored in the remote repository for recovery
        # purposes. Using a separate generated key from the derived key
        # lets us change the password without having to change the generated
        # key, which would require re-encrypting the entire repository contents.
        return nacl.pwhash.argon2id.kdf(
            nacl.secret.SecretBox.KEY_SIZE,
            password.encode("utf-8"),
            salt=self.salt,
            opslimit=self.ops,
            memlimit=self.mem,
        )

    def _decrypt_privkey(self, password):
        symmetric_key = self._get_symmetric_key(password)

        try:
            return nacl.public.PrivateKey(
                nacl.secret.SecretBox(symmetric_key).decrypt(self.enc_privkey)
            )
        except nacl.exceptions.CryptoError as e:
            raise DecryptionError(str(e)) from e

    @classmethod
    def init_new(cls, password):
        self = cls(
            salt=nacl.utils.random(nacl.pwhash.argon2id.SALTBYTES),
            ops=cls.OPSLIMIT,
            mem=cls.MEMLIMIT,
            pubkey=None,
            enc_privkey=None,
        )

        symmetric_key = self._get_symmetric_key(password)

        # This is the master key that will be used to encrypt all the
        # repository contents
        key = nacl.public.PrivateKey.generate()

        self.pubkey = key.public_key

        self.enc_privkey = nacl.secret.SecretBox(symmetric_key).encrypt(bytes(key))

        return self

    @classmethod
    def init_from_private(cls, params):
        return cls(
            salt=bytes.fromhex(params["salt"]),
            ops=params["ops"],
            mem=params["mem"],
            pubkey=nacl.public.PublicKey(bytes.fromhex(params["pubkey"])),
            enc_privkey=bytes.fromhex(params["key"]),
        )

    @classmethod
    def init_from_public(cls, params, password):
        self = cls(
            salt=bytes.fromhex(params["salt"]),
            ops=params["ops"],
            mem=params["mem"],
            pubkey=None,
            enc_privkey=bytes.fromhex(params["key"]),
        )

        self.pubkey = self._decrypt_privkey(password).public_key

        return self

    def get_public_params(self):
        return {
            "salt": self.salt.hex(),
            "ops": self.ops,
            "mem": self.mem,
            "key": self.enc_privkey.hex(),
        }

    def get_private_params(self):
        return {
            "salt": self.salt.hex(),
            "ops": self.ops,
            "mem": self.mem,
            "pubkey": bytes(self.pubkey).hex(),
            "key": self.enc_privkey.hex(),
        }

    def encrypt_bytes(self, plaintext):
        # Note: pynacl currently cannot encrypt byte-like objects like
        # memoryviews, so we must read it into a proper bytes object. This is
        # not a technical restriction as far as I can tell, just a bug.
        if isinstance(plaintext, memoryview):
            plaintext = bytes(plaintext)
        return nacl.public.SealedBox(self.pubkey).encrypt(plaintext)

    def get_decryption_key(self, password):
        return self._decrypt_privkey(password)

    def decrypt_bytes(self, cyphertext, key: nacl.public.PrivateKey):
        try:
            return nacl.public.SealedBox(key).decrypt(cyphertext)
        except nacl.exceptions.CryptoError as e:
            raise DecryptionError(str(e)) from e

    def calculate_objid(self, content):
        # Since the public key is not actually public, this should serve as a
        # good hmac key. While not usually a good idea to use an encryption
        # key for a different purpose like this, I doubt there are any odd
        # interactions between the Nacl SealedBox routines and hmac-sha256.
        # If someone is really worried about this, we could generate some
        # additional bytes from the KDF for the HMAC key.
        h = hmac.new(bytes(self.pubkey), msg=content, digestmod=hashlib.sha256)
        return h.digest()
