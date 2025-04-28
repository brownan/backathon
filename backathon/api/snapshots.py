import asyncio
import datetime
import pathlib
import threading
from typing import Annotated

import fastapi
import pydantic
from fastapi import Depends
from fastapi import HTTPException

import backathon.garbage
from backathon import Database
from backathon.api.events import send_config_change_event
from backathon.api.params import RepoDependency
from backathon.api.utils import url_for_func
from backathon.asyncutils import non_reentrant
from backathon.models import ObjIDType
from backathon.models import Snapshot
from backathon.types import PrintablePath

api = fastapi.APIRouter()


async def _get_snapshot(repo: RepoDependency, id: int) -> Snapshot:
    snapshot = next(
        repo.db.query(Snapshot, "SELECT * FROM snapshots WHERE id = ?", (id,)), None
    )
    if snapshot is None:
        raise HTTPException(status_code=404)
    return snapshot


SnapshotParam = Annotated[Snapshot, Depends(_get_snapshot)]


class SnapshotInfo(pydantic.BaseModel):
    id: int
    path: PrintablePath
    root: ObjIDType
    timestamp: datetime.datetime


@api.get("/snapshots")
async def get_snapshots(repo: RepoDependency) -> list[Snapshot]:
    return list(repo.db.query(Snapshot, "SELECT * FROM snapshots ORDER BY timestamp"))


@api.get("/snapshots/{id}")
async def get_snapshot_info(
    repo: RepoDependency, snapshot: SnapshotParam
) -> SnapshotInfo:
    info = SnapshotInfo.model_construct(
        id=snapshot.id,
        path=pathlib.Path(snapshot.path),
        root=snapshot.root,
        timestamp=snapshot.timestamp,
    )

    return info


class SnapshotExtendedInfo(pydantic.BaseModel):
    numObjects: int
    uploadedSize: int
    fileSize: int
    exclusiveObjs: int
    exclusiveSize: int
    sharedSize: int


@non_reentrant
async def _compute_exclusive_info(db: Database, snapshot_id: int) -> SnapshotExtendedInfo:
    info = SnapshotExtendedInfo.model_construct()

    # Do work items in a separate thread with a new DB connection because it's
    # quite expensive and we wouldn't want to block the main event loop
    # for this long. That's also why this is a separate API call from
    # the regular snapshot info. So that one can return easy data quickly.

    all_object_count = 0
    all_object_size = 0

    def thread1():
        nonlocal all_object_size, all_object_count
        thread_local_db = db.clone()
        with thread_local_db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), SUM(uploaded_size) FROM objects")
            all_object_count, all_object_size = cursor.fetchone()

    def thread2():
        nonlocal info
        thread_local_db = db.clone()
        with thread_local_db.cursor() as cursor:
            # Aggregate info on all objects related to this snapshot
            cursor.execute(
                """
            WITH RECURSIVE reachable(id) AS (
                SELECT root FROM snapshots WHERE id=?
                UNION
                SELECT child FROM object_relations
                INNER JOIN reachable ON reachable.id=parent
            ) SELECT COUNT(*), SUM(uploaded_size), SUM(file_size) FROM objects WHERE objid IN reachable
            """,
                (snapshot_id,),
            )
            info.numObjects, info.uploadedSize, info.fileSize = cursor.fetchone()

    unreachable_objs = 0
    unreachable_size = 0

    def thread3():
        nonlocal unreachable_objs, unreachable_size
        thread_local_db = db.clone()
        bloom = backathon.garbage.BloomFilter.build_filter(
            thread_local_db,
            [snapshot_id],
            cancel_event=cancel_event,
        )
        if cancel_event.is_set():
            return

        for obj in bloom.iter_unreachable(thread_local_db):
            unreachable_objs += 1
            unreachable_size += obj.uploaded_size if obj.uploaded_size else 0
            if cancel_event.is_set():
                return

    cancel_event = threading.Event()

    async with asyncio.TaskGroup() as tg:
        try:
            await asyncio.gather(
                tg.create_task(asyncio.to_thread(thread1)),
                tg.create_task(asyncio.to_thread(thread2)),
                tg.create_task(asyncio.to_thread(thread3)),
            )
        finally:
            cancel_event.set()

    info.exclusiveObjs = all_object_count - unreachable_objs
    info.exclusiveSize = min(info.uploadedSize, all_object_size - unreachable_size)
    info.sharedSize = min(0, info.uploadedSize - info.exclusiveSize)
    return info


@api.get("/snapshots/{id}/extended")
async def get_snapshot_exclusive_info(
    repo: RepoDependency, snapshot: SnapshotParam
) -> SnapshotExtendedInfo:
    return await _compute_exclusive_info(repo.db, snapshot.id)


@api.delete("/snapshots/{id}")
async def delete_snapshot(repo: RepoDependency, snapshot: SnapshotParam):
    with repo.db.cursor() as cursor:
        cursor.execute("DELETE FROM snapshots WHERE id=?", (snapshot.id,))
    send_config_change_event(url_for_func(get_snapshots))
