import fastapi

from backathon.api.events import depends_on_config_keys
from backathon.api.params import RepoDependency
from backathon.schedule import ScheduleSettings

api = fastapi.APIRouter()


@api.get("/schedule")
@depends_on_config_keys("schedule_settings")
async def get_schedule(repo: RepoDependency) -> ScheduleSettings:
    return repo.db.config.schedule_settings


@api.post("/schedule")
async def set_schedule(repo: RepoDependency, sched: ScheduleSettings):
    repo.db.config.schedule_settings = sched
    repo.db.config.save(repo)
