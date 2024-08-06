from __future__ import annotations

from abc import ABC, abstractmethod
from typing import IO, Any, NamedTuple

from backathon.models import ObjIDType


class Payload(NamedTuple):
    buf: IO[bytes]
    size: int
    sha1: bytes


class KeyNotDecrypted(Exception):
    pass


class EncrypterBase(ABC):
    @abstractmethod
    def __init__(self, state: dict[str, Any]):
        ...

    @abstractmethod
    def unlock(self, password: str):
        ...

    @abstractmethod
    def encrypt(self, buf: IO[bytes]) -> Payload:
        ...

    @abstractmethod
    def decrypt(self, buf: IO[bytes]) -> IO[bytes]:
        ...

    @abstractmethod
    def make_objid(self, buf: IO[bytes]) -> ObjIDType:
        ...


class DecryptionError(Exception):
    pass
