import asyncio
import base64
import contextlib
import json
import logging.config
import os
import pathlib
import time
from operator import attrgetter
from typing import Annotated

import anyio
import fastapi
import natsort
import pydantic
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
from pydantic import BeforeValidator
from pydantic import PlainSerializer
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

# This pydantic type serializes a pathlib.Path into an opaque object that
# will preserve un-decodable bytes in the path without hitting decode errors
# in serialization
PathType = Annotated[
    pathlib.Path,
    PlainSerializer(
        lambda x: base64.urlsafe_b64encode(FSEntry.encode_path(x)).decode("ascii"),
        return_type=str,
    ),
    BeforeValidator(
        lambda x: pathlib.Path(os.fsdecode(base64.urlsafe_b64decode(x.encode("ascii"))))
    ),
]

# Strips unprintable characters for user display
PrintablePath = Annotated[
    pathlib.Path, PlainSerializer(lambda x: make_path_printable(os.fsencode(x)))
]


class RootBrowseReturn(pydantic.BaseModel):
    path: PrintablePath
    key: PathType
    root: bool
    excluded: bool

    # If this node is the parent of some root, then it should be partially
    # checked in the UI
    parentOfRoot: bool

    # If this node is the child of a root, then it is implicitly checked,
    # UNLESS it is excluded or the child of an excluded node
    childOfRoot: bool

    @staticmethod
    def closest_common_path(
        base_paths: list[pathlib.Path], relative_path: pathlib.Path
    ) -> int | None:
        distances: list[int] = []
        for base_path in base_paths:
            for step, path in enumerate([relative_path] + list(relative_path.parents)):
                if path == base_path:
                    distances.append(step)
                    break
        return min(distances) if distances else None

    @classmethod
    def from_path(
        cls, path: pathlib.Path, roots: list[pathlib.Path], excludes: list[pathlib.Path]
    ):
        closest_root = cls.closest_common_path(roots, path)
        closest_exclude = cls.closest_common_path(excludes, path)

        parent_of_root = (
            any(path == p for root in roots for p in root.parents) and closest_root != 0
        )

        return cls.model_construct(
            path=path,
            key=path,
            root=closest_root == 0,
            excluded=closest_exclude == 0,
            parentOfRoot=parent_of_root,
            childOfRoot=closest_root is not None
            and (closest_exclude is None or closest_exclude > closest_root),
        )


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
async def list_roots(repo: RepoDependency) -> list[RootBrowseReturn]:
    roots = repo.get_roots()
    root_paths = [entry.decoded_path for entry in roots]
    exclude_paths = []
    return [
        RootBrowseReturn.from_path(path, root_paths, exclude_paths) for path in root_paths
    ]


@api.post("/roots/")
async def add_root(
    repo: RepoDependency, path: Annotated[PathType, Body(embed=True)]
) -> FSEntry:
    entry = repo.add_root(path)
    return entry


@api.get("/roots/browse")
async def root_browse(repo: RepoDependency, key: PathType) -> list[RootBrowseReturn]:
    path = pathlib.Path(key)
    if not path.is_dir():
        raise fastapi.HTTPException(status_code=404, detail="Path not found")

    root_paths = [entry.decoded_path for entry in repo.get_roots()]
    exclude_paths = []

    items: list[RootBrowseReturn] = []
    for subpath in path.iterdir():
        if subpath.is_dir():
            items.append(
                RootBrowseReturn.from_path(
                    subpath,
                    root_paths,
                    exclude_paths,
                )
            )

    return natsort.os_sorted(items, key=lambda x: x.path)


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


_config_change_listeners: list[asyncio.Queue[str]] = []


def send_config_change_event(key: str):
    for q in _config_change_listeners:
        q.put_nowait(key)


@api.get("/events")
async def events(repo: RepoDependency) -> sse_starlette.EventSourceResponse:
    send_stream, recv_stream = anyio.create_memory_object_stream(0)

    logger.info("Starting SSE event stream task")

    async def config_change_watcher():
        my_queue = asyncio.Queue()
        _config_change_listeners.append(my_queue)
        try:
            while True:
                key = await my_queue.get()
                await send_stream.send(
                    sse_starlette.ServerSentEvent(
                        json.dumps({"key": key}), event="configChange"
                    )
                )
        except asyncio.CancelledError:
            logger.debug("event status watcher canceled and closing")
            raise
        finally:
            _config_change_listeners.remove(my_queue)

    async def job_status_watcher():
        try:
            # Push an initial status into the object stream so the client gets an
            # immediate status
            await send_stream.send(json.dumps(repo.jobs.make_status_message()))

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

    async def data_sender_task():
        async with asyncio.TaskGroup() as tg:
            tg.create_task(job_status_watcher())
            tg.create_task(config_change_watcher())

    return sse_starlette.EventSourceResponse(
        recv_stream, data_sender_callable=data_sender_task
    )
