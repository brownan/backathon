from __future__ import annotations

import hashlib
import io
from typing import IO, Any, cast

from pydantic import BaseModel
from typing_extensions import Self

from backathon.encryption.base import EncrypterBase, Payload, UnlockCallback
from backathon.models import ObjIDType
from backathon.repoobject import COPY_BUFSIZE


class NullConfig(BaseModel):
    pass


class NullEncrypter(EncrypterBase[NullConfig]):
    def __init__(self, config: NullConfig | None = None):
        super().__init__(config or NullConfig())

    @classmethod
    def get_config_class(cls):
        return NullConfig

    def get_recovery_state(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_recovery_state(cls, rstate: dict[str, Any], password: str) -> Self:
        return cls(NullConfig())

    def unlock(self, password: str):
        pass

    def encrypt(self, buf: IO[bytes]) -> Payload:
        hasher = hashlib.sha1()
        size = 0
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            size += len(chunk)
            hasher.update(chunk)
        buf.seek(0)
        return Payload(buf=buf, size=size, sha1=hasher.digest())

    def decrypt(
        self, buf: IO[bytes], unlock_callback: UnlockCallback | None = None
    ) -> IO[bytes]:
        return buf

    def make_objid(self, buf: IO[bytes]) -> ObjIDType:
        pos = buf.tell()
        buf.seek(0)
        hasher = hashlib.blake2b(digest_size=32)
        while chunk := buf.read(COPY_BUFSIZE):
            hasher.update(chunk)
        buf.seek(pos)
        return cast(ObjIDType, hasher.digest())
