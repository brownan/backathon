from __future__ import annotations

from abc import ABC, abstractmethod
from typing import IO, Any, Callable, Generic, NamedTuple, Type, TypeVar

from pydantic import BaseModel
from typing_extensions import Self

from backathon.models import ObjIDType


class Payload(NamedTuple):
    buf: IO[bytes]
    size: int
    sha1: bytes


class KeyNotDecrypted(Exception):
    pass


UnlockCallback = Callable[[Callable[[str], None]], None]

C = TypeVar("C", bound=BaseModel)


class EncrypterBase(ABC, Generic[C]):
    config: C

    def __init__(self, config: C):
        self.config = config

    @classmethod
    @abstractmethod
    def get_config_class(cls) -> Type[C]:
        ...

    @abstractmethod
    def get_recovery_state(self) -> dict[str, Any] | None:
        """Parameters that should be saved to the remote repository"""
        return None

    @classmethod
    @abstractmethod
    def from_recovery_state(cls, rstate: dict[str, Any], password: str) -> Self:
        ...

    @abstractmethod
    def unlock(self, password: str):
        ...

    @abstractmethod
    def encrypt(self, buf: IO[bytes]) -> Payload:
        ...

    @abstractmethod
    def decrypt(
        self, buf: IO[bytes], unlock_callback: UnlockCallback | None = None
    ) -> IO[bytes]:
        ...

    @abstractmethod
    def make_objid(self, buf: IO[bytes]) -> ObjIDType:
        ...


class DecryptionError(Exception):
    pass
