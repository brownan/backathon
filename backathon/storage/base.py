from abc import ABC, abstractmethod
from os import PathLike
from typing import IO, Generic, Iterator, NamedTuple, Type, TypeVar

from pydantic import BaseModel

from backathon.encryption.base import Payload

C = TypeVar("C", bound=BaseModel)


class DownloadedFile(NamedTuple):
    # Remote repo path that was downloaded
    path: str
    # Number of bytes that were downloaded
    size: int
    # Stream open for reading
    stream: IO[bytes]
    # sha1 of the downloaded bytes
    sha1: bytes | None


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
    def get_object(self, path: str | PathLike[str]) -> DownloadedFile:
        ...

    @abstractmethod
    def delete_object(self, path: str | PathLike[str]):
        ...

    @abstractmethod
    def list_dir(self, path: str | PathLike[str]) -> Iterator[str]:
        ...
