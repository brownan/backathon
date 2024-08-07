import hashlib
import hmac
import io
from typing import IO, Annotated

import nacl.public
import nacl.pwhash.argon2id
import nacl.secret
import nacl.utils
from pydantic import BaseModel, EncodedBytes

from backathon.encryption.base import EncrypterBase, KeyNotDecrypted, Payload
from backathon.models import BytesHexEncoder, ObjIDType


class NaclState(BaseModel):
    salt: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]
    ops: int
    mem: int
    pubkey: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]
    privkey: Annotated[bytes, EncodedBytes(encoder=BytesHexEncoder)]


class NaclEncrypter(EncrypterBase):
    DEFAULT_OPSLIMIT = nacl.pwhash.argon2id.OPSLIMIT_SENSITIVE
    DEFAULT_MEMLIMIT = nacl.pwhash.argon2id.MEMLIMIT_SENSITIVE

    def __init__(self, state: NaclState):
        self.state = state
        self.privkey: nacl.public.PrivateKey | None = None

    @property
    def pubkey(self) -> nacl.public.PublicKey:
        return nacl.public.PublicKey(self.state.pubkey)

    @staticmethod
    def _derive_symmetric_key(password: str, salt: bytes, ops: int, mem: int) -> bytes:
        """Derives the key used to encrypt the private part of the public/private key"""
        return nacl.pwhash.argon2id.kdf(
            nacl.secret.SecretBox.KEY_SIZE,
            password.encode("utf-8"),
            salt=salt,
            opslimit=ops,
            memlimit=mem,
        )

    @classmethod
    def initialize(cls, password: str) -> "NaclEncrypter":
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
        # to perform unattended backups.#
        # The private key is stored encrypted, so restores are only possible with the
        # password.
        state = NaclState(
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
            self.state.salt,
            self.state.ops,
            self.state.mem,
        )
        privkey_bytes = nacl.secret.SecretBox(symmetric_key).decrypt(self.state.privkey)
        self.privkey = nacl.public.PrivateKey(privkey_bytes)

    def encrypt(self, buf: IO[bytes]) -> Payload:
        sealed_box = nacl.public.SealedBox(self.pubkey)
        encrypted_bytes = sealed_box.encrypt(buf.read())
        hasher = hashlib.sha1()
        hasher.update(encrypted_bytes)
        return Payload(
            buf=io.BytesIO(encrypted_bytes),
            size=len(encrypted_bytes),
            sha1=hasher.digest(),
        )

    def decrypt(self, buf: IO[bytes]) -> IO[bytes]:
        if self.privkey is None:
            raise KeyNotDecrypted

        sealed_box = nacl.public.SealedBox(self.privkey)
        decrypted_bytes = sealed_box.decrypt(buf.read())
        return io.BytesIO(decrypted_bytes)

    def make_objid(self, buf: IO[bytes]) -> ObjIDType:
        h = hmac.new(bytes(self.pubkey), digestmod="sha256")
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            h.update(chunk)
        buf.seek(0)
        return ObjIDType(h.digest())
