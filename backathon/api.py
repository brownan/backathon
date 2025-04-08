import asyncio
import contextlib
import json
import logging.config
import os
import pathlib
import time
from operator import attrgetter
from typing import Annotated

import anyio
import natsort
import sse_starlette
from fastapi import Body
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Path
from fastapi import Request
from fastapi.responses import StreamingResponse
from pydantic import Base64Bytes
from pydantic import BaseModel
from starlette.routing import Mount

import backathon.backup
import backathon.encryption
import backathon.models
import backathon.repository
import backathon.scan
from backathon import Backathon
from backathon import Database
from backathon.models import FSEntry
from backathon.models import Object
from backathon.models import ObjectType
from backathon.models import Snapshot
from backathon.models import decode_objid
from backathon.models import make_path_printable
from backathon.restore import stream_dir
from backathon.restore import stream_file

logger = logging.getLogger("backathon.api")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API lifecycle started")

    async with asyncio.TaskGroup():
        db_path = os.environ["BACKATHON_DB_PATH"]
        db = Database(db_path)
        repo = Backathon(db)
        yield {"db": db, "repo": repo}


async def database(req: Request) -> Database:
    return req.state.db


DatabaseDependency = Annotated[Database, Depends(database)]


async def repo(req: Request) -> Backathon:
    return req.state.repo


RepoDependency = Annotated[Backathon, Depends(repo)]

ObjIdParam = Annotated[str, Path(pattern=r"[0-9a-fA-F]{2}+")]

api = FastAPI()

dev_app = FastAPI(
    lifespan=lifespan,
    routes=[
        Mount("/api", api),
    ],
    openapi_url=None,
)


@api.get("/")
async def top(db: DatabaseDependency, repo: RepoDependency):
    return {"message": "Hello, world!", "db": repr(db), "repo": repr(repo)}


@api.get("/roots/")
async def list_roots(repo: RepoDependency) -> list[FSEntry]:
    return repo.get_roots()


@api.post("/roots/")
async def add_root(
    repo: RepoDependency, path: Annotated[str, Body(embed=True)]
) -> FSEntry:
    entry = repo.add_root(pathlib.Path(path))
    return entry


@api.get("/roots/{id}")
async def get_root(repo: RepoDependency, id: int) -> FSEntry:
    entry = next(
        repo.db.query(FSEntry, "SELECT * FROM fsentry WHERE id = ?", (id,)), None
    )
    if entry is None:
        raise HTTPException(status_code=404)

    if entry.parent is not None:
        raise HTTPException(status_code=404)

    return entry


@api.delete("/roots/{id}")
async def del_root(repo: RepoDependency, id: int):
    entry = await get_root(repo, id)
    with repo.db.cursor() as cursor:
        cursor.execute("DELETE FROM fsentry WHERE id = ?", (entry.id,))


@api.get("/excludes/")
async def get_excludes(
    repo: RepoDependency,
) -> list[str]:
    return repo.db.config.excludes


@api.post("/excludes/")
async def set_excludes(
    repo: RepoDependency, new_excludes: Annotated[list[str], Body()]
) -> list[str]:
    repo.db.config.excludes = new_excludes
    repo.db.config.save(repo.db)
    return new_excludes


@api.get("/snapshots")
async def get_snapshots(repo: RepoDependency) -> list[Snapshot]:
    return list(repo.db.query(Snapshot, "SELECT * FROM snapshots ORDER BY timestamp"))


@api.get("/objects/{objid}")
async def get_object(repo: RepoDependency, objid: ObjIdParam) -> Object:
    obj = next(
        repo.db.query(
            Object, "SELECT * FROM objects WHERE objid = ?", (bytes.fromhex(objid),)
        ),
        None,
    )
    if obj is None:
        raise HTTPException(status_code=404)
    return obj


class DirListEntry(BaseModel):
    id: Base64Bytes
    name: str
    obj: Object


dir_list_entry_sort_key = natsort.os_sort_keygen(attrgetter("id"))


@api.get("/objects/{objid}/ls")
async def get_directory_contents(
    repo: RepoDependency, objid: ObjIdParam
) -> list[DirListEntry]:
    """Given a tree-type object, list the contents of the directory"""
    ret = []
    with repo.db.cursor() as cursor:
        cursor.execute(
            """
        SELECT child, name
        FROM object_relations
        WHERE parent = ?
        """,
            (bytes.fromhex(objid),),
        )
        for child, name in cursor:
            obj = next(
                repo.db.query(Object, "SELECT * FROM objects WHERE objid = ?", (child,))
            )
            ret.append(
                DirListEntry.model_construct(
                    id=name,
                    name=make_path_printable(name),
                    obj=obj,
                )
            )
    ret.sort(key=dir_list_entry_sort_key)
    return ret


@api.get("/objects/{objid}/download")
async def download_object(
    repo: RepoDependency, objid: ObjIdParam, name: str
) -> StreamingResponse:
    obj = next(
        repo.db.query(
            Object, "SELECT * FROM objects WHERE objid=?", (decode_objid(objid),)
        )
    )
    obj_getter = repo.make_obj_getter(None)
    if obj.type == ObjectType.FILE:
        try:
            header, body_iter = await stream_file(decode_objid(objid), obj_getter)
        except backathon.encryption.KeyNotDecrypted:
            raise HTTPException(status_code=403, detail="Decryption key needed")
        except backathon.encryption.DecryptionError as e:
            raise HTTPException(status_code=500, detail=f"Decryption error: {e}")
        if header.type != backathon.models.ObjectType.FILE:
            raise HTTPException(status_code=404, detail="Object exists but is not a file")
        if header.stats is None:
            raise HTTPException(
                status_code=500, detail="Object header missing stats info"
            )

        return StreamingResponse(
            content=body_iter, headers={"Content-Length": str(header.stats.size)}
        )

    elif obj.type == ObjectType.TREE:
        try:
            stream_iter = stream_dir(
                decode_objid(objid), obj_getter, name=name.encode("utf-8")
            )
        except backathon.encryption.KeyNotDecrypted:
            raise HTTPException(status_code=403, detail="Decryption key needed")
        except backathon.encryption.DecryptionError as e:
            raise HTTPException(status_code=500, detail=f"Decryption error: {e}")

        return StreamingResponse(
            content=stream_iter, headers={"Content-Type": "application/x-tar"}
        )
    else:
        raise HTTPException(status_code=404, detail=f"Unknown object type: {obj.type}")


@api.get("/scan")
async def scan(repo: RepoDependency) -> backathon.scan.ScanProgress | None:
    return repo.jobs.scan.status.current_message


@api.post("/scan")
async def scan_start(repo: RepoDependency):
    logger.debug("Starting scan")
    try:
        repo.scan_async()
    except Exception as e:
        return {"status": "Failed to start scan", "error": str(e)}
    return {"status": "Scan Started"}


@api.get("/backup")
async def backup(repo: RepoDependency) -> backathon.backup.BackupProgress | None:
    return repo.jobs.backup.status.current_message


@api.post("/backup")
async def backup_start(repo: RepoDependency):
    logger.debug("Starting backup")
    try:
        repo.backup_async()
    except Exception as e:
        return {"status": "Failed to start backup", "error": str(e)}
    return {"status": "Backup Started"}


@api.get("/events")
async def events(repo: RepoDependency) -> sse_starlette.EventSourceResponse:
    send_stream, recv_stream = anyio.create_memory_object_stream(0)

    logger.info("Starting SSE event stream task")

    # Push an initial status into the object stream so the client gets an
    # immediate status
    asyncio.create_task(send_stream.send(json.dumps(repo.jobs.make_status_message())))

    async def job_status_watcher():
        try:
            last_updated = 0
            while True:
                await repo.jobs.wait_for_change()

                now = time.monotonic()
                if now < last_updated + 0.1:
                    await asyncio.sleep(last_updated + 0.1 - now)

                status_msg = repo.jobs.make_status_message()
                await send_stream.send(
                    sse_starlette.ServerSentEvent(
                        json.dumps(status_msg), event="statusUpdate"
                    )
                )
                last_updated = now

        except asyncio.CancelledError:
            logger.debug("SSE event listener cancelled and closing")
            raise

    return sse_starlette.EventSourceResponse(
        recv_stream, data_sender_callable=job_status_watcher
    )
