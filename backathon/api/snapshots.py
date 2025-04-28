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
    numObjects: int
    uploadedSize: int
    fileSize: int


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

    # Now for some fun recursive queries to get some juicy info
    with repo.db.cursor() as cursor:
        # Aggregate info on all objects related to this snapshot
        cursor.execute(
            """
        WITH RECURSIVE reachable(id) AS (
            VALUES (?)
            UNION ALL
            SELECT child FROM object_relations
            INNER JOIN reachable ON reachable.id=parent
        ) SELECT COUNT(*), SUM(uploaded_size), SUM(file_size) FROM objects WHERE objid IN reachable
        """,
            (snapshot.root,),
        )
        info.numObjects, info.uploadedSize, info.fileSize = cursor.fetchone()

    return info


@non_reentrant
async def _compute_exclusive_info(db: Database, snapshot_id: int):
    exclusive_objs = 0
    exclusive_size = 0

    with db.cursor() as cursor:
        cursor.execute("SELECT id FROM snapshots")
        all_snapshot_ids = [row[0] for row in cursor]

    # Do this in a thread with a new DB connection because it's quite
    # expensive and we wouldn't want to block the main event loop
    # for this long. That's also why this is a separate API call from
    # the regular snapshot info. So that one can return easy data quickly.
    def thread():
        nonlocal exclusive_objs, exclusive_size
        thread_local_db = db.clone()
        bloom = backathon.garbage.BloomFilter.build_filter(
            thread_local_db,
            [s for s in all_snapshot_ids if s != snapshot_id],
            cancel_event=cancel_event,
        )
        if cancel_event.is_set():
            return

        for obj in bloom.iter_unreachable(thread_local_db):
            exclusive_objs += 1
            exclusive_size += obj.uploaded_size if obj.uploaded_size else 0
            if cancel_event.is_set():
                return

    cancel_event = threading.Event()

    try:
        await asyncio.to_thread(thread)
    except asyncio.CancelledError:
        cancel_event.set()
        raise

    return SnapshotExtendedInfo.model_construct(
        exclusiveObjs=exclusive_objs,
        exclusiveSize=exclusive_size,
    )


class SnapshotExtendedInfo(pydantic.BaseModel):
    exclusiveObjs: int
    exclusiveSize: int


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
