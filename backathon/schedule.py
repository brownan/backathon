import datetime
import enum
from typing import TYPE_CHECKING

import dateutil.relativedelta
import pydantic

if TYPE_CHECKING:
    from backathon import Backathon


class ScheduleModes(enum.Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ScheduleSettings(pydantic.BaseModel):
    enable: bool = True
    mode: ScheduleModes = ScheduleModes.HOURLY
    nextRunTime: datetime.datetime | None = None


def get_next_run_time(
    mode: ScheduleModes, last_run_time: datetime.datetime
) -> datetime.datetime:
    """Calculates the next runtime of the schedule"""

    time_delta = None
    match mode:
        case ScheduleModes.HOURLY:
            time_delta = datetime.timedelta(hours=1)
        case ScheduleModes.DAILY:
            time_delta = datetime.timedelta(days=1)
        case ScheduleModes.WEEKLY:
            time_delta = datetime.timedelta(weeks=1)
        case ScheduleModes.MONTHLY:
            time_delta = dateutil.relativedelta.relativedelta(months=1)

    assert time_delta

    next_run_time = last_run_time + time_delta

    return next_run_time


class Schedule:
    def __init__(self, repo: "Backathon"):
        self.repo = repo
        self.db = repo.db

    def set_schedule(self, mode: ScheduleModes):
        pass  # TODO
