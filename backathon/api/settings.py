import fastapi

from backathon.api.params import RepoDependency
from backathon.settings import Settings

api = fastapi.APIRouter()


@api.get("/settings")
async def get_settings(repo: RepoDependency) -> Settings:
    return repo.db.config
