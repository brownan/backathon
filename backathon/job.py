from __future__ import annotations

import asyncio
import dataclasses
import logging
from abc import ABC
from typing import TYPE_CHECKING
from typing import Generic
from typing import TypeVar

import pydantic

if TYPE_CHECKING:
    import backathon.backup
    import backathon.scan

MessageType = TypeVar("MessageType")

logger = logging.getLogger("backathon.job")


class StatusListener(Generic[MessageType]):
    def __init__(self):
        self._listeners: list[asyncio.Future] = []
        self.current_message: MessageType | None = None

    def update(self, message: MessageType | None):
        self.current_message = message
        self._notify_listeners()

    def _notify_listeners(self):
        while self._listeners:
            fut = self._listeners.pop()
            if not fut.cancelled():
                fut.set_result(None)

    async def wait_get(self) -> MessageType | None:
        fut = asyncio.Future()
        self._listeners.append(fut)
        await fut
        return self.current_message


class Job(ABC, Generic[MessageType]):
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.status: StatusListener[MessageType] = StatusListener()

    def set_task(self, task: asyncio.Task):
        if self.task is not None:
            logger.warning(
                "New task set on job, but last task was not finished: %s", self.task
            )
            self.task.cancel()
        self.task = task
        self.task.add_done_callback(self._finish)

    def progress_callback(self, message: MessageType):
        if not self.task:
            logger.error(
                "Progress callback called but no task was set. This is a bug",
                stack_info=True,
            )
        self.status.update(message)

    def is_running(self) -> bool:
        return self.task is not None

    def _finish(self, task: asyncio.Task):
        if self.task is task:
            # No other task is running. This job is now idle
            self.task = None
            logger.debug("Task done. Clearing status")
            self.status.update(None)
        else:
            # Another task is replacing this one, which generally shouldn't
            # happen. A warning would have been emitted by set_task()
            pass


@dataclasses.dataclass
class JobCollection:
    scan: Job[backathon.scan.ScanProgress] = dataclasses.field(default_factory=Job)
    backup: Job[backathon.backup.BackupProgress] = dataclasses.field(default_factory=Job)

    def asdict(self) -> dict[str, Job[pydantic.BaseModel]]:
        return vars(self)

    def any_is_running(self) -> bool:
        return any(j.is_running() for j in self.asdict().values())

    async def wait_for_change(self):
        jobs = self.asdict()
        tasks = {}
        async with asyncio.TaskGroup() as tg:
            for name in jobs.keys():
                tasks[name] = tg.create_task(jobs[name].status.wait_get())
            await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
            for task in tasks.values():
                task.cancel()

    def make_status_message(self):
        return {
            name: job.status.current_message.model_dump(mode="json")
            if job.status.current_message is not None
            else None
            for name, job in self.asdict().items()
        }
