import hashlib
import logging
from typing import IO
from typing import Annotated
from typing import Any
from typing import cast

import nacl.exceptions
import nacl.public
import nacl.pwhash.argon2id
import nacl.secret
import nacl.utils
from pydantic import BaseModel
from pydantic import EncodedBytes
from typing_extensions import Buffer
from typing_extensions import Self

from backathon.encryption import KeyNotDecrypted
from backathon.encryption.base import EncrypterBase
from backathon.encryption.base import Payload
from backathon.exceptions import CorruptedRepository
from backathon.models import BytesHexEncoder
from backathon.models import ObjIDType
from backathon.proftools import perf_block

logger = logging.getLogger("backathon.nacl")


class NaclConfig(BaseModel):
    salt: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]
    ops: int
    mem: int

    # The actual bytes of the public key, to be passed right in to
    # nacl.public.PublicKey()
    pubkey: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]

    # The bytes of the private key are encrypted using nacl SecretBox
    # and a key derived from the password using argon2id and the
    # above kdf params
    privkey: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]


class NaclEncrypter(EncrypterBase[NaclConfig]):
    """An encrypter using the Nacl library

    This encrypter uses the NaCl SealedBox abstraction to encrypt files at rest.
    This construction uses:
    * X25519 for the key exchange
    * XSalsa20 for the encryption
    * Poly1305 for the authentication

    The keys used for the sealed box are a public-private keypair generated at the time
    of the repository creation. The public key is stored in plaintext locally, allowing
    encryption of files without a password from the local machine. The private key is
    stored encrypted both locally and in the remote repository, requiring the password to
    decrypt files.

    Note: the "public" key is not stored in plain text other than on the local machine. It
    should still be kept secret (not stored in plain text anywhere else) to ensure other
    actors cannot upload valid objects to the repository. See the threat model documentation
    for more information.

    The private key is encrypted using the NaCl SecretBox abstraction.
    This construction uses:
    * XSalsa20 stream cipher for encryption
    * Poly1305 MAC for authentication

    The key used for the SecretBox is derived from the user's password using the
    argon2id key derivation function. The iterations and memory parameters use the
    recommended values for "sensitive" configurations. At the time of writing, this
    is 4 iterations and 1024MiB of memory, and takes around 3.5 seconds on a typical machine.

    Additionally, objects are identified and addressed using a hash of their unencrypted
    contents. The hash algorithm used is blake2b with a 32 byte hash size.
    The key used is the sealed box's public key.
    """

    DEFAULT_OPSLIMIT = nacl.pwhash.argon2id.OPSLIMIT_SENSITIVE
    DEFAULT_MEMLIMIT = nacl.pwhash.argon2id.MEMLIMIT_SENSITIVE

    def __init__(self, config: NaclConfig):
        super().__init__(config)
        self.privkey: nacl.public.PrivateKey | None = None

    @classmethod
    def get_config_class(cls):
        return NaclConfig

    def get_recovery_state(self) -> dict[str, Any]:
        return {
            "salt": self.config.salt.hex(),
            "ops": self.config.ops,
            "mem": self.config.mem,
            "key": self.config.privkey.hex(),
        }

    @classmethod
    def from_recovery_state(cls, rstate: dict[str, Any], password: str) -> Self:
        salt = bytes.fromhex(rstate["salt"])
        ops = rstate["ops"]
        mem = rstate["mem"]
        pwkey = cls._derive_symmetric_key(
            password,
            salt,
            ops=ops,
            mem=mem,
        )
        encrypted_privkey_bytes = bytes.fromhex(rstate["key"])
        try:
            privkey_bytes = nacl.secret.SecretBox(pwkey).decrypt(encrypted_privkey_bytes)
        except nacl.exceptions.CryptoError as e:
            raise CorruptedRepository(
                "Could not decrypt repository key. Bad password or corrupted repository"
            ) from e
        privkey = nacl.public.PrivateKey(privkey_bytes)
        pubkey = privkey.public_key
        self = cls(
            NaclConfig.model_construct(
                salt=salt,
                ops=ops,
                mem=mem,
                pubkey=bytes(pubkey),
                privkey=encrypted_privkey_bytes,
            )
        )
        self.privkey = privkey
        return self

    @property
    def pubkey(self) -> nacl.public.PublicKey:
        return nacl.public.PublicKey(self.config.pubkey)

    @staticmethod
    def _derive_symmetric_key(password: str, salt: bytes, ops: int, mem: int) -> bytes:
        """Derives the key used to encrypt the private part of the public/private key

        Note, this is a CPU intensive operation. By design, it will take a couple seconds
        or so.

        """
        logger.debug("Deriving key from password. This may take a moment...")
        return nacl.pwhash.argon2id.kdf(
            nacl.secret.SecretBox.KEY_SIZE,
            password.encode("utf-8"),
            salt=salt,
            opslimit=ops,
            memlimit=mem,
        )

    @classmethod
    def new(cls, password: str) -> "NaclEncrypter":
        salt = nacl.utils.random(nacl.pwhash.argon2id.SALTBYTES)
        ops = cls.DEFAULT_OPSLIMIT
        mem = cls.DEFAULT_MEMLIMIT

        # This key is derived from the password, and is used to encrypt the private
        # part of the public/private keypair.
        symmetric_key = cls._derive_symmetric_key(password, salt, ops, mem)

        # This is the public/private keypair, used to encrypt the backup data
        # before uploading
        key = nacl.public.PrivateKey.generate()
        encrypted_key = nacl.secret.SecretBox(symmetric_key).encrypt(bytes(key))

        # The state is saved locally in plain text, and is everything we need in order
        # to perform unattended backups.
        # The private key is stored encrypted, so restores are only possible with the
        # password.
        state = NaclConfig.model_construct(
            salt=salt,
            ops=ops,
            mem=mem,
            pubkey=bytes(key.public_key),
            privkey=encrypted_key,
        )

        instance = cls(state)
        instance.privkey = key
        return instance

    def unlock(self, password: str):
        if self.privkey is not None:
            return
        symmetric_key = self._derive_symmetric_key(
            password,
            self.config.salt,
            self.config.ops,
            self.config.mem,
        )
        try:
            privkey_bytes = nacl.secret.SecretBox(symmetric_key).decrypt(
                self.config.privkey
            )
        except nacl.exceptions.CryptoError as e:
            raise CorruptedRepository(
                "Could not decrypt repository key. Bad password or corrupted repository"
            ) from e

        self.privkey = nacl.public.PrivateKey(privkey_bytes)

    def encrypt(self, buf: Buffer) -> Payload:
        sealed_box = nacl.public.SealedBox(self.pubkey)
        encrypted_bytes = sealed_box.encrypt(bytes(buf))
        hasher = hashlib.sha1()
        hasher.update(encrypted_bytes)
        return Payload(
            buf=encrypted_bytes,
            size=len(encrypted_bytes),
            sha1=hasher.digest(),
        )

    def decrypt(self, buf: IO[bytes]) -> Buffer:
        if self.privkey is None:
            raise KeyNotDecrypted

        sealed_box = nacl.public.SealedBox(self.privkey)
        decrypted_bytes = sealed_box.decrypt(buf.read())
        return decrypted_bytes

    def make_objid(self, buf: Buffer) -> ObjIDType:
        with perf_block("make_objid"):
            h = hashlib.blake2b(digest_size=32, key=bytes(self.pubkey))
            h.update(buf)
            return cast(ObjIDType, h.digest())
