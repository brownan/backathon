from __future__ import annotations

import abc
from typing import AsyncIterator
from typing import Type
from typing import TypeVar
from typing import overload

import pydantic

import backathon.asyncutils


class SignalType(pydantic.BaseModel, abc.ABC):
    pass


class ConfigChange(SignalType):
    key: str


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

    @overload
    async def wait(self, signal: S) -> S:
        ...

    @overload
    async def wait(self, signal: Type[S]) -> S:
        ...

    async def wait(self, signal: Type[S] | S) -> S:
        if isinstance(signal, SignalType):
            async for item in self.listen(type(signal)):
                if item == signal:
                    return item
        else:
            async for item in self.listen(signal):
                return item
        raise AssertionError()
