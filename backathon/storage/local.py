import pathlib
from os import PathLike
from typing import IO

from pydantic import BaseModel

from backathon.encryption.base import Payload
from backathon.storage.base import StorageBase


class LocalStorageConfig(BaseModel):
    base_path: pathlib.Path


class LocalStorage(StorageBase[LocalStorageConfig]):
    config: LocalStorageConfig

    @classmethod
    def get_config_class(cls):
        return LocalStorageConfig

    def _make_full_path(self, rel_path: str | PathLike[str]) -> pathlib.Path:
        base_path = self.config.base_path
        return base_path / rel_path

    def put_object(self, path: str | PathLike[str], payload: Payload):
        full_path = self._make_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with full_path.open("wb") as fobj:
            fobj.write(payload.buf)

    def get_object(self, path: str | PathLike[str]) -> IO[bytes]:
        full_path = self._make_full_path(path)
        return full_path.open("rb")

    def delete_object(self, path: str | PathLike[str]):
        full_path = self._make_full_path(path)
        full_path.unlink(missing_ok=True)
