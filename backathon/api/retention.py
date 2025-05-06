import fastapi

from backathon.api.params import RepoDependency
from backathon.retention import RetentionSettings
from backathon.signals import ConfigChange

api = fastapi.APIRouter()


@api.get("/retention/settings")
async def get_retention_settings(repo: RepoDependency) -> RetentionSettings:
    return repo.db.config.retention_settings


@api.post("/retention/settings")
async def set_retention_settings(repo: RepoDependency, settings: RetentionSettings):
    repo.db.config.retention_settings = settings
    repo.db.config.save(repo.db)
    repo.signals.send(ConfigChange(key="get_retention_settings"))
