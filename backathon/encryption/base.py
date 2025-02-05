from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import IO
from typing import Any
from typing import Callable
from typing import Generic
from typing import NamedTuple
from typing import Type
from typing import TypeVar

from pydantic import BaseModel
from typing_extensions import Buffer
from typing_extensions import Self

from backathon.models import ObjIDType


class Payload(NamedTuple):
    buf: Buffer
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
    def encrypt(self, buf: Buffer) -> Payload:
        ...

    @abstractmethod
    def decrypt(self, buf: IO[bytes]) -> Buffer:
        ...

    @abstractmethod
    def make_objid(self, buf: Buffer) -> ObjIDType:
        ...


class DecryptionError(Exception):
    pass
