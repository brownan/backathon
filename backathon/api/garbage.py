import asyncio
import threading

import fastapi
import pydantic

from backathon.api.params import RepoDependency
from backathon.api.repoinfo import RepoInfo
from backathon.api.repoinfo import repository_info
from backathon.garbage import BloomFilter

api = fastapi.APIRouter()


class GarbageCollectionInfo(pydantic.BaseModel):
    repoInfo: RepoInfo
    unreachableCount: int
    unreachableSize: int


@api.get("/garbage")
async def garbage_info(repo: RepoDependency) -> GarbageCollectionInfo:
    repo_info = await repository_info(repo)

    cancel_event = threading.Event()

    unreachable_count = 0
    unreachable_size = 0

    def thread():
        nonlocal unreachable_size, unreachable_count
        local_db = repo.db.clone()
        bloom = BloomFilter.build_filter(local_db, cancel_event=cancel_event)

        for obj in bloom.iter_unreachable(local_db):
            if cancel_event.is_set():
                return
            unreachable_count += 1
            unreachable_size += obj.uploaded_size or 0

    async with asyncio.TaskGroup() as tg:
        try:
            await tg.create_task(asyncio.to_thread(thread))
        finally:
            cancel_event.set()

    return GarbageCollectionInfo.model_construct(
        repoInfo=repo_info,
        unreachableCount=unreachable_count,
        unreachableSize=unreachable_size,
    )
