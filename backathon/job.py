from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import TYPE_CHECKING
from typing import Generic
from typing import TypeVar

import pydantic

from backathon.signals import JobStatusChange
from backathon.signals import SignalBus

if TYPE_CHECKING:
    import backathon.backup
    import backathon.scan

MessageType = TypeVar("MessageType")

logger = logging.getLogger("backathon.job")


class Job(Generic[MessageType]):
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.status: MessageType | None = None
        self.signal_bus: SignalBus | None = None
        self.job_name: str = ""

    def _update_status(self, message: MessageType | None):
        self.status = message
        assert self.signal_bus is not None
        self.signal_bus.send(
            JobStatusChange(job=self.job_name)  # pyright: ignore [reportArgumentType]
        )

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
        self._update_status(message)

    def is_running(self) -> bool:
        return self.task is not None

    def _finish(self, task: asyncio.Task):
        if self.task is task:
            # No other task is running. This job is now idle
            self.task = None
            logger.debug("Task done. Clearing status")
            self._update_status(None)
        else:
            # Another task is replacing this one, which generally shouldn't
            # happen. A warning would have been emitted by set_task()
            pass


@dataclasses.dataclass
class JobCollection:
    scan: Job[backathon.scan.ScanProgress] = dataclasses.field(default_factory=Job)
    backup: Job[backathon.backup.BackupProgress] = dataclasses.field(default_factory=Job)

    def set_signal_bus(self, bus: SignalBus):
        for name, job in self.asdict().items():
            job.signal_bus = bus
            job.job_name = name

    def asdict(self) -> dict[str, Job[pydantic.BaseModel]]:
        return vars(self)

    def any_is_running(self) -> bool:
        return any(j.is_running() for j in self.asdict().values())

    def make_status_message(self):
        return {
            name: job.status.model_dump(mode="json") if job.status is not None else None
            for name, job in self.asdict().items()
        }
