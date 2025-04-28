import fastapi
import pydantic

from backathon.api.params import RepoDependency

api = fastapi.APIRouter()


class RepoInfo(pydantic.BaseModel):
    numObjects: int
    uploadedSize: int
    numSnapshots: int


@api.get("/repository/info")
async def repository_info(repo: RepoDependency) -> RepoInfo:
    """Gets information about the repository"""
    info = RepoInfo.model_construct()
    with repo.db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*), SUM(uploaded_size) FROM objects")
        info.numObjects, info.uploadedSize = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT timestamp FROM snapshots)")
        info.numSnapshots = cursor.fetchone()[0]

    return info
