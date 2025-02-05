from abc import ABC
from abc import abstractmethod
from os import PathLike
from typing import IO
from typing import Generic
from typing import Iterator
from typing import NamedTuple
from typing import Type
from typing import TypeVar

from pydantic import BaseModel

from backathon.encryption.base import Payload

C = TypeVar("C", bound=BaseModel)


class DownloadedFileContext:
    def __init__(self, f: "DownloadedFile"):
        self._f = f

    def __enter__(self):
        return self._f

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._f.stream.close()


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
    def get_object(self, path: str | PathLike[str]) -> DownloadedFileContext:
        ...

    @abstractmethod
    def delete_object(self, path: str | PathLike[str]):
        ...

    @abstractmethod
    def list_dir(self, path: str | PathLike[str]) -> Iterator[str]:
        ...
