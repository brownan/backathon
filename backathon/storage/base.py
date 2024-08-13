from abc import ABC, abstractmethod
from os import PathLike
from typing import IO, Generic, Type, TypeVar

from pydantic import BaseModel

from backathon.encryption.base import Payload

C = TypeVar("C", bound=BaseModel)


class StorageBase(ABC, Generic[C]):
    config: C

    def __init__(self, config: C):
        self.config = config

    @classmethod
    @abstractmethod
    def get_config_class(cls) -> Type[C]:
        ...

    @abstractmethod
    def put_object(self, path: str | PathLike[str], payload: Payload):
        ...

    @abstractmethod
    def get_object(self, path: str | PathLike[str]) -> IO[bytes]:
        ...
