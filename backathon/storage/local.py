import pathlib
import shutil
from os import PathLike
from typing import Any, IO, Type

from pydantic import BaseModel

from backathon.storage.base import StorageBase
from backathon.encryption.base import Payload


class LocalStorageConfig(BaseModel):
    base_path: pathlib.Path


class LocalStorage(StorageBase):
    config: LocalStorageConfig

    @classmethod
    def get_config_class(cls) -> Type[BaseModel]:
        return LocalStorageConfig

    def put_object(self, path: PathLike, payload: Payload):
        base_path = self.config.base_path
        path = base_path / path
        with path.open("wb") as fobj:
            shutil.copyfileobj(payload.buf, fobj)
