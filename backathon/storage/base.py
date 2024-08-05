from abc import ABC, abstractmethod
from os import PathLike
from typing import Any, Type

from pydantic import BaseModel

from backathon.encryption.base import Payload


class StorageBase(ABC):
    def __init__(self, config: dict[str, Any]):
        self.config = self._unpack_config(config)

    def _unpack_config(self, config: dict[str, Any]):
        return self.get_config_class().model_validate(config)

    @classmethod
    @abstractmethod
    def get_config_class(cls) -> Type[BaseModel]: ...

    @abstractmethod
    def put_object(self, path: PathLike, payload: Payload): ...
