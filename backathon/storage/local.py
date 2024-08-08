import pathlib
import shutil
from os import PathLike

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

    def put_object(self, path: PathLike, payload: Payload):
        base_path = self.config.base_path
        path = base_path / path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fobj:
            shutil.copyfileobj(payload.buf, fobj)
