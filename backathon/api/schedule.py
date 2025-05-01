import datetime

import fastapi
import pydantic

from backathon.api.params import RepoDependency
from backathon.settings import ScheduleModes

api = fastapi.APIRouter()


class ScheduleInfo(pydantic.BaseModel):
    enable: bool
    mode: ScheduleModes
    nextRunTime: datetime.datetime | None


@api.get("/schedule")
async def get_schedule_info(repo: RepoDependency) -> ScheduleInfo:
    return ScheduleInfo.model_construct(
        enable=repo.db.config.schedule_enable,
        mode=repo.db.config.schedule_mode,
        nextRunTime=repo.db.config.schedule_next_run_time,
    )


@api.post("/schedule")
async def modify_schedule(repo: RepoDependency, info: ScheduleInfo):
    pass  # TODO
