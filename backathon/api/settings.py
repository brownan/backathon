from typing import Annotated

import fastapi
import pydantic

from backathon.api.params import RepoDependency
from backathon.settings import Settings

api = fastapi.APIRouter()


@api.get("/settings")
async def get_settings(repo: RepoDependency) -> Settings:
    return repo.db.config


@api.post("/settings/{key}")
async def set_setting(
    repo: RepoDependency, key: str, value: Annotated[str, fastapi.Body()]
):
    try:
        adapter = pydantic.TypeAdapter(Settings.model_fields[key].annotation)
    except KeyError:
        raise fastapi.HTTPException(status_code=404, detail="Unknown setting")

    value_deserialized = adapter.validate_json(value)

    setattr(repo.db.config, key, value_deserialized)
    repo.db.save_config()
