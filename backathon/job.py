import asyncio
import contextlib
import logging
from abc import ABC
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Generic
from typing import TypeVar

MessageType = TypeVar("MessageType")

HandlerType = Callable[[MessageType | None], Awaitable[None]]

logger = logging.getLogger("backathon.job")


class Channel(Generic[MessageType]):
    """Super simple in-memory single-process messaging channel

    Uses context managers on listeners to un-register listeners when they go
    out of context.

    """

    def __init__(self):
        self.listeners: list[asyncio.Queue[MessageType | None]] = []

    def send(self, message: MessageType | None):
        for q in self.listeners:
            q.put_nowait(message)

    @staticmethod
    async def _listen_task(
        queue: asyncio.Queue[MessageType | None], handler: HandlerType
    ):
        while True:
            item = await queue.get()
            try:
                await handler(item)
            except Exception:
                logger.error("Error in listener handler", exc_info=True)
                pass

    @contextlib.asynccontextmanager
    async def listen(self, handler: Callable[[MessageType | None], Awaitable[None]]):
        q = asyncio.Queue()
        listener_task = asyncio.create_task(self._listen_task(q, handler))
        try:
            self.listeners.append(q)
            listener_task.add_done_callback(lambda _: self.listeners.remove(q))
            yield
        finally:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass


class Job(ABC, Generic[MessageType]):
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.channel = Channel[MessageType]()
        self.progress: MessageType | None = None

    def set_task(self, task: asyncio.Task):
        if self.task is not None:
            logger.warning(
                "New task set on job, but last task was not finished: %s", self.task
            )
            self.task.cancel()
        self.task = task
        self.task.add_done_callback(self._finish)

    def progress_callback(self, message: MessageType):
        self.progress = message
        self.channel.send(message)

    def is_running(self) -> bool:
        return self.task is not None

    def _finish(self, task: asyncio.Task):
        if self.task is task:
            # No other task is running. This job is now idle
            self.task = None
            self.progress = None
            self.channel.send(None)
        else:
            # Another task is replacing this one, which generally shouldn't
            # happen. A warning would have been emitted by set_task()
            pass
