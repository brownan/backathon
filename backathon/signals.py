import abc
from typing import AsyncIterator
from typing import Type
from typing import TypeVar

import pydantic

import backathon.asyncutils


class SignalType(pydantic.BaseModel, abc.ABC):
    pass


S = TypeVar("S", bound=SignalType)


class SignalBus:
    def __init__(self):
        self._bus = backathon.asyncutils.Bus()

    def send(self, item: SignalType):
        self._bus.send(item)

    async def listen(self, signal_type: Type[S]) -> AsyncIterator[S]:
        async for item in self._bus.listen():
            if isinstance(item, signal_type):
                yield item
        raise AssertionError()

    async def wait(self, signal_type: Type[S]) -> S:
        async for item in self.listen(signal_type):
            return item
        raise AssertionError()
