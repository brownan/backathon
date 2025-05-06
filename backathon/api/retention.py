import fastapi

from backathon.api.params import RepoDependency
from backathon.retention import RetentionSettings

api = fastapi.APIRouter()


@api.get("/retention/settings")
def get_retention_settings(repo: RepoDependency) -> RetentionSettings:
    return repo.db.config.retention_settings
