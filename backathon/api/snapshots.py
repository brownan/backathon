import asyncio
import datetime
import json
import logging
import pathlib
import threading
from typing import Annotated

import fastapi
import pydantic
from fastapi import Depends
from fastapi import HTTPException

import backathon.garbage
from backathon import Database
from backathon.api.params import RepoDependency
from backathon.asyncutils import non_reentrant
from backathon.models import ObjIDType
from backathon.models import Snapshot
from backathon.signals import ConfigChange
from backathon.types import PrintablePath

logger = logging.getLogger("backathon.api.snapshots")

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
    return list(
        repo.db.query(Snapshot, "SELECT * FROM snapshots ORDER BY timestamp DESC")
    )


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

    otherObjects: int
    otherUploadedSize: int
    otherFileSize: int

    exclusiveObjs: int
    exclusiveSize: int
    sharedSize: int


@non_reentrant
async def _compute_exclusive_info(db: Database, snapshot_id: int) -> SnapshotExtendedInfo:
    info = SnapshotExtendedInfo.model_construct()

    # Get the list of all snapshots in the database so we can do calculations
    # involving those snapshots' roots
    with db.cursor() as cursor:
        cursor.execute("SELECT id FROM snapshots")
        all_snapshot_ids = set(row[0] for row in cursor)
        other_snapshot_ids = list(all_snapshot_ids.difference([snapshot_id]))

    # Work is performed in a separate threads so that it doesn't block the main
    # loop and also can be parallelized a bit.

    def thread1():
        # Computes the size of all objects reachable from this snapshot
        nonlocal info
        thread_local_db = db.clone()
        with thread_local_db.cursor() as cursor:
            # Aggregate info on all objects related to this snapshot
            cursor.execute(
                """
            WITH RECURSIVE reachable(id) AS (
                SELECT root FROM snapshots WHERE id=?
                UNION ALL
                SELECT child FROM object_relations
                INNER JOIN reachable ON reachable.id=parent
            ) SELECT COUNT(*), SUM(uploaded_size), SUM(file_size) FROM objects WHERE objid IN reachable
            """,
                (snapshot_id,),
            )
            info.numObjects, info.uploadedSize, info.fileSize = cursor.fetchone()

    def thread2():
        # Computes the size of objects reachable from all snapshots BUT the
        # given snapshot
        nonlocal info
        thread_local_db = db.clone()
        with thread_local_db.cursor() as cursor:
            cursor.execute(
                """
            WITH RECURSIVE reachable(id) AS (
                SELECT root FROM snapshots WHERE id IN (SELECT value FROM json_each(?))
                UNION ALL
                SELECT child FROM object_relations
                INNER JOIN reachable ON reachable.id=parent
            ) SELECT COUNT(*), SUM(uploaded_size), SUM(file_size) FROM objects WHERE objid IN reachable
            """,
                (json.dumps(other_snapshot_ids),),
            )
            (
                info.otherObjects,
                info.otherUploadedSize,
                info.otherFileSize,
            ) = cursor.fetchone()

    exclusive_objs = 0
    exclusive_size = 0

    def thread3():
        # Computes the exclusive size of the snapshot. This is essentially
        # a set difference between the given snapshot's objects and all other
        # snapshots' objects.
        nonlocal exclusive_objs, exclusive_size
        thread_local_db = db.clone()
        bloom = backathon.garbage.BloomFilter.build_filter(
            thread_local_db,
            other_snapshot_ids,
            cancel_event=cancel_event,
        )
        if cancel_event.is_set():
            return

        for obj in bloom.iter_unreachable(thread_local_db, snapshot_ids=[snapshot_id]):
            exclusive_objs += 1
            exclusive_size += obj.uploaded_size if obj.uploaded_size else 0
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

    logger.debug("Unreachable size: %s", exclusive_size)
    info.exclusiveObjs = exclusive_objs
    info.exclusiveSize = exclusive_size
    info.sharedSize = info.uploadedSize - info.exclusiveSize
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
    repo.signals.send(ConfigChange(key="get_snapshots"))
