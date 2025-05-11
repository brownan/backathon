import fastapi

from backathon.api.params import RepoDependency
from backathon.retention import RetentionSettings

api = fastapi.APIRouter()


@api.get("/retention")
async def get_retention_settings(repo: RepoDependency) -> RetentionSettings:
    return repo.db.config.retention_settings


@api.post("/retention")
async def set_retention_settings(repo: RepoDependency, settings: RetentionSettings):
    repo.db.config.retention_settings = settings
    repo.db.save_config()
