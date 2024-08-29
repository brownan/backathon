import hashlib
import io
import logging
from typing import IO, Annotated, Any, cast

import nacl.public
import nacl.pwhash.argon2id
import nacl.secret
import nacl.utils
from pydantic import BaseModel, EncodedBytes
from typing_extensions import Buffer, Self

from backathon.encryption.base import (
    EncrypterBase,
    KeyNotDecrypted,
    Payload,
    UnlockCallback,
)
from backathon.models import BytesHexEncoder, ObjIDType

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
        privkey_bytes = nacl.secret.SecretBox(pwkey).decrypt(rstate["key"])
        privkey = nacl.public.PrivateKey(privkey_bytes)
        pubkey = privkey.public_key
        self = cls(
            NaclConfig.model_construct(
                salt=salt,
                ops=ops,
                mem=mem,
                pubkey=bytes(pubkey),
                privkey=privkey_bytes,
            )
        )
        self.privkey = privkey
        return self

    @property
    def pubkey(self) -> nacl.public.PublicKey:
        return nacl.public.PublicKey(self.config.pubkey)

    @staticmethod
    def _derive_symmetric_key(password: str, salt: bytes, ops: int, mem: int) -> bytes:
        """Derives the key used to encrypt the private part of the public/private key"""
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
        privkey_bytes = nacl.secret.SecretBox(symmetric_key).decrypt(self.config.privkey)
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

    def decrypt(
        self, buf: IO[bytes], unlock_callback: UnlockCallback | None = None
    ) -> IO[bytes]:
        if self.privkey is None and unlock_callback is not None:
            unlock_callback(self.unlock)
        if self.privkey is None:
            raise KeyNotDecrypted

        sealed_box = nacl.public.SealedBox(self.privkey)
        decrypted_bytes = sealed_box.decrypt(buf.read())
        return io.BytesIO(decrypted_bytes)

    def make_objid(self, buf: Buffer) -> ObjIDType:
        h = hashlib.blake2b(digest_size=32, key=bytes(self.pubkey))
        h.update(buf)
        return cast(ObjIDType, h.digest())
