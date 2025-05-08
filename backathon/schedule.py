import asyncio
import datetime
import enum
import logging
from typing import TYPE_CHECKING

import dateutil.relativedelta
import pydantic

from backathon.job import JobEnd
from backathon.signals import ConfigChange

if TYPE_CHECKING:
    from backathon import Backathon

logger = logging.getLogger("backathon.schedule")


class ScheduleModes(enum.Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ScheduleSettings(pydantic.BaseModel):
    enable: bool = True
    mode: ScheduleModes = ScheduleModes.HOURLY
    nextRunTime: datetime.datetime | None = None


def calculate_next_run_time(
    mode: ScheduleModes, last_run_time: datetime.datetime
) -> datetime.datetime:
    """Calculates the next runtime of the schedule"""

    # Perform calculation in localtime
    last_run_time.astimezone()

    time_delta = None
    match mode:
        case ScheduleModes.HOURLY:
            time_delta = dateutil.relativedelta.relativedelta(hours=1)
        case ScheduleModes.DAILY:
            time_delta = dateutil.relativedelta.relativedelta(days=1)
        case ScheduleModes.WEEKLY:
            time_delta = dateutil.relativedelta.relativedelta(weeks=1)
        case ScheduleModes.MONTHLY:
            time_delta = dateutil.relativedelta.relativedelta(months=1)

    assert time_delta

    next_run_time = last_run_time + time_delta

    return next_run_time


async def scheduler(repo: "Backathon"):
    """Main scheduler task

    Responsibilities:
    * If next backup time is in the past, kick off a backup immediately
    * If next backup time is in the future, sleep until scheduled
    * When a backup finishes, update the next run time
    * If launching a backup but a job is currently running:
      - if it's already a backup job, then skip the current schedule and
        update next run time
      - otherwise, set up a callback signal to launch the backup as soon as
        the current job is done
    * When waiting, monitor for schedule config changes, and restart above
      process if schedule is updated.
    """
    async with asyncio.TaskGroup() as tg:
        while True:
            logger.debug("Creating schedule process task")
            schedule_process_task = tg.create_task(_schedule_process(repo))
            await asyncio.wait(
                [
                    schedule_process_task,
                    tg.create_task(
                        repo.signals.wait(ConfigChange(key="schedule_settings"))
                    ),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if schedule_process_task.done():
                await schedule_process_task
                raise RuntimeError("Schedule task exited unexpectedly with no error")

            logger.debug("Cancelling scheduler process task due to config change")
            schedule_process_task.cancel()
            try:
                await schedule_process_task
            except asyncio.CancelledError:
                pass


async def _schedule_process(repo: "Backathon"):
    schedule = repo.db.config.schedule_settings
    if schedule.nextRunTime is not None:
        logger.debug("Scheduled next run time is %s", schedule.nextRunTime)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        time_remaining = (schedule.nextRunTime - now).total_seconds()
        if time_remaining > 0:
            logger.debug(
                "Schedule configured to run at %s. Sleeping for %s seconds",
                schedule.nextRunTime,
                time_remaining,
            )
            await asyncio.sleep(time_remaining)
        await _do_backup(repo)

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        while schedule.nextRunTime < now:
            schedule.nextRunTime = calculate_next_run_time(
                schedule.mode, schedule.nextRunTime
            )
        logger.debug("Backup finished. Next run time is %s", schedule.nextRunTime)
        repo.db.config.schedule_settings = schedule
        repo.db.config.save(repo)

    # Parent task will cancel this one and re-launch if/when the schedule
    # changes
    while True:
        await asyncio.sleep(9999)


async def _do_backup(repo: "Backathon"):
    logger.debug("Schedule _do_backup() entered")
    if repo.jobs.backup.is_running():
        logger.debug("Backup is already running. Skipping")
        return

    if repo.jobs.any_is_running():
        # Wait for job to finish
        logger.debug("Waiting for existing job to finish")
        await repo.signals.wait(JobEnd)

    logger.info("Launching scan")
    await repo.scan_async()
    logger.info("Scan finished. Launching backup")
    await repo.backup_async()
    logger.info("Backup finished")
