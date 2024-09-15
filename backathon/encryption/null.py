from __future__ import annotations

import hashlib
from typing import IO, Any, cast

from pydantic import BaseModel
from typing_extensions import Buffer, Self

from backathon.encryption.base import EncrypterBase, Payload
from backathon.models import ObjIDType


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

    def encrypt(self, buf: Buffer) -> Payload:
        hasher = hashlib.sha1()
        hasher.update(buf)
        size = len(memoryview(buf))
        return Payload(buf=buf, size=size, sha1=hasher.digest())

    def decrypt(self, buf: IO[bytes]) -> Buffer:
        return buf.read()

    def make_objid(self, buf: Buffer) -> ObjIDType:
        hasher = hashlib.blake2b(digest_size=32)
        hasher.update(buf)
        return cast(ObjIDType, hasher.digest())
