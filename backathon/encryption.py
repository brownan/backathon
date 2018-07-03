import nacl.utils
import nacl.pwhash
import nacl.secret
import nacl.public

class PasswordRequired(Exception):
    """Raised when None is passed to an encryption routine that requires a
    password
    """

class DecryptionError(Exception):
    pass

class BaseEncryption:
    """Base class for encryption classes

    See method documentation for info on how this API works

    Basically, it's set up for a public/private scheme where some info is
    stored locally in what is presumed to be secure storage, and some info is
    stored remotely but encrypted and is used for recovery.
    """

    @classmethod
    def init_new(cls, password):
        """Generate new encryption keys using the given password"""
        raise NotImplementedError()

    @classmethod
    def init_from_public(cls, params, password):
        """Initialize this object from the public parameters

        The public parameters are stored in the remote repository and are
        used in event a full recovery is needed. The password is required in
        order to decrypt any keys that are stored in the public storage
        """
        return cls()

    @classmethod
    def init_from_private(cls, params):
        """Initiliaze this object from the private parameters

        This is the usual way the class will be initialized in normal
        operation. The parameters stored locally are passed in.
        """
        return cls()

    def get_public_params(self):
        """Return the parameters that should be stored in the remote repository

        These parameters are stored unencrypted and are used in event a full
        recovery is needed. It should contain everything necessary to
        reconfigure the encryption except for a password. The password and
        these params are provided to init_from_public() in event of a recovery.
        """
        raise NotImplementedError()

    def get_private_params(self):
        """Return the parameters to store locally

        These parameters are stored unencrypted locally and are used in
        normal operation to re-initialize this class by passing them in to
        init_from_private()
        """
        raise NotImplementedError()

    def encrypt_bytes(self, plaintext):
        raise NotImplementedError()

    def decrypt_bytes(self, cyphertext, password):
        raise NotImplementedError()

class NullEncryption(BaseEncryption):
    @classmethod
    def init_new(cls, password):
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
    def decrypt_bytes(self, cyphertext, password):
        return cyphertext


class NaclSealedBox(BaseEncryption):
    pass # TODO
