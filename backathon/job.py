from __future__ import annotations

import asyncio
import datetime
import enum
import json
import logging
import traceback
from typing import Any
from typing import Awaitable
from typing import Callable

from pydantic import BaseModel
from pydantic import JsonValue

from backathon.asyncutils import Bus
from backathon.db import Database

logger = logging.getLogger("backathon.job")


class JobTypes(enum.StrEnum):
    SCAN = "scan"
    BACKUP = "backup"
    RESTORE = "restore"
    GC = "gc"
    REPAIR = "repair"


class Status(enum.StrEnum):
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobInfo(BaseModel):
    """Info about a job for sending on the wire to the frontend, plus methods for
    saving and loading to/from the db.

    """

    id: int
    job_type: JobTypes
    start_time: datetime.datetime
    end_time: datetime.datetime | None = None
    kwargs: dict[str, JsonValue] = {}
    status: Status
    error_msg: str | None = None
    traceback: str | None = None
    progress: dict[str, JsonValue] | None = None

    @classmethod
    def insert(
        cls,
        db: Database,
        job_event_bus: Bus[JobInfo],
        job_type: JobTypes,
        start_time: datetime.datetime,
        kwargs: dict[str, JsonValue],
        status: Status,
    ) -> JobInfo:
        with db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (job_type, start_time, kwargs, status) VALUES (?,?,?,?) RETURNING id
            """,
                (
                    job_type,
                    start_time,
                    json.dumps(kwargs),
                    status,
                ),
            )
            job_id = cursor.fetchone()[0]

        job_obj = cls.model_construct(
            id=job_id,
            job_type=job_type,
            start_time=start_time,
            kwargs=kwargs,
            status=status,
        )
        job_event_bus.send(job_obj)
        return job_obj

    def update(self, db: Database, job_event_bus: Bus[JobInfo]):
        with db.cursor() as cursor:
            cursor.execute(
                """
            UPDATE jobs SET end_time=?, status=?, error_msg=?, traceback=?, progress=? WHERE id=?
            """,
                (
                    self.end_time,
                    self.status,
                    self.error_msg,
                    self.traceback,
                    json.dumps(self.progress),
                    self.id,
                ),
            )
        job_event_bus.send(self)

    @classmethod
    def get(cls, db: Database, job_id: int):
        return db.get(cls, "SELECT * FROM job WHERE id=%s", (job_id,))


class JobAlreadyRunning(Exception):
    pass


class JobNotRunning(Exception):
    pass


JOB_COROUTINES: dict[JobTypes, Callable[..., Awaitable]] = {
    # TODO
}


class Job:
    """This class's purpose is to serve as a handle for code executing a job to
    maniuplate the status and progress of the job

    The underlying _info holds the actual job state that's sent to frontend clients
    and persisted to the database. This class holds the machinery to keep all that
    updated

    """

    def __init__(self, supervisor: JobSupervisor, task: asyncio.Task, info: JobInfo):
        self.task: asyncio.Task | None = task
        self._info = info
        self._supervisor = supervisor

    def update_progress(self, progress_info: dict[str, JsonValue]):
        if self._supervisor._current_job is not self:
            raise JobNotRunning()
        self._info.progress = progress_info
        self._info.update(self._supervisor._db, self._supervisor.job_updates)


class JobSupervisor:
    """Handles the launching and finalization of jobs"""

    def __init__(self, db: Database):
        self._current_job: Job | None = None
        self._db = db
        self.job_updates = Bus[JobInfo]()

    def get_current_job(self) -> Job | None:
        return self._current_job

    def launch_job(self, job_type: JobTypes, kwargs: dict[str, Any]) -> Job:
        if self._current_job is not None:
            raise JobAlreadyRunning()
        coro = JOB_COROUTINES[job_type]
        info = JobInfo.insert(
            self._db,
            self.job_updates,
            job_type=job_type,
            start_time=datetime.datetime.now(tz=datetime.timezone.utc),
            kwargs=kwargs,
            status=Status.RUNNING,
        )
        self._current_job = Job(
            self,
            asyncio.ensure_future(coro(**kwargs)),
            info,
        )

        def on_done(task: asyncio.Task):
            if self._current_job is None:
                return
            info = self._current_job._info
            if info.status == Status.RUNNING:
                info.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                if task.cancelled():
                    info.status = Status.CANCELLED
                elif (exc := task.exception()) is not None:
                    info.status = Status.FAILED
                    info.error_msg = str(exc)
                    info.traceback = "\n".join(traceback.format_exception(exc))
                else:
                    info.status = Status.DONE

                try:
                    info.update(self._db, self.job_updates)
                except Exception:
                    logger.exception("Error finalizing job %s", info)

            self._current_job.task = None
            self._current_job = None

        assert self._current_job.task is not None
        self._current_job.task.add_done_callback(on_done)
        return self._current_job
