import asyncio
import contextlib
import logging.config
import os
import pathlib
from operator import attrgetter

import blacknoise
import fastapi
import natsort
import pydantic
import starlette.types
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import Base64Bytes
from pydantic import BaseModel
from starlette.routing import Mount

import backathon.api.events
import backathon.api.garbage
import backathon.api.repoinfo
import backathon.api.retention
import backathon.api.schedule
import backathon.api.snapshots
import backathon.backup
import backathon.encryption
import backathon.garbage
import backathon.models
import backathon.repository
import backathon.scan
from backathon import Backathon
from backathon import Database
from backathon.api.events import depends_on_config_keys
from backathon.api.params import DatabaseDependency
from backathon.api.params import ObjIdParam
from backathon.api.params import RepoDependency
from backathon.api.types import Browse
from backathon.api.types import FSEntryType
from backathon.api.types import PathInfo
from backathon.models import FSEntry
from backathon.models import Object
from backathon.models import ObjectType
from backathon.models import decode_objid
from backathon.models import make_path_printable
from backathon.restore import stream_dir
from backathon.restore import stream_file
from backathon.types import PathType

logger = logging.getLogger("backathon.api")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logger.debug("API lifecycle started")

    async with asyncio.TaskGroup() as tg:
        db_path = os.environ["BACKATHON_DB_PATH"]
        db = Database(db_path)
        repo = Backathon(db)

        config_signal_task = tg.create_task(
            backathon.api.events.config_signal_to_api_reload(repo)
        )

        try:
            yield {"db": db, "repo": repo}
        finally:
            config_signal_task.cancel()


api = FastAPI()
api.include_router(backathon.api.events.api)
api.include_router(backathon.api.repoinfo.api)
api.include_router(backathon.api.snapshots.api)
api.include_router(backathon.api.garbage.api)
api.include_router(backathon.api.schedule.api)
api.include_router(backathon.api.retention.api)

dev_app = FastAPI(
    lifespan=lifespan,
    openapi_url=None,
    routes=[
        Mount("/api", api),
    ],
)


def make_static_app():
    async def not_found(*args):
        raise fastapi.HTTPException(status_code=404, detail="Path not found")

    app = blacknoise.BlackNoise(not_found)
    app.add("backathon-web/dist", "/")

    async def wrapper(scope: starlette.types.Scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/":
            scope["path"] = "/index.html"
        await app(scope, receive, send)

    return wrapper


prod_app = FastAPI(
    lifespan=lifespan,
    openapi_url=None,
    routes=[
        Mount("/api", api),
        Mount("/", make_static_app()),
    ],
)


@api.get("/")
async def top(db: DatabaseDependency, repo: RepoDependency):
    return {"message": "Hello, world!", "db": repr(db), "repo": repr(repo)}


@api.get("/roots/")
async def list_roots(repo: RepoDependency) -> list[PathInfo]:
    roots = repo.get_roots()
    root_paths = [entry.decoded_path for entry in roots]
    exclude_paths = repo.db.config.excludes
    pathinfos = [
        PathInfo.from_path(path, root_paths, exclude_paths) for path in root_paths
    ]
    pathinfos.sort(key=natsort.os_sort_keygen(attrgetter("path")))
    return pathinfos


@api.put("/roots/{key}")
async def add_root(repo: RepoDependency, key: PathType) -> FSEntryType:
    entry = repo.add_root(key)
    repo.signals.send(backathon.api.events.APIReloadSignal(name="list_roots"))
    return FSEntryType.from_fsentry(entry)


@api.delete("/roots/{key}")
async def delete_root(repo: RepoDependency, key: PathType):
    repo.del_root(key)
    repo.signals.send(backathon.api.events.APIReloadSignal(name="list_roots"))


@api.get("/browse/")
async def browse_root(repo: RepoDependency) -> Browse:
    return await browse(repo, pathlib.Path("/"))


@api.get("/browse/{key}")
async def browse(repo: RepoDependency, key: PathType | None = None) -> Browse:
    path = key
    if not path:
        path = pathlib.Path("/")
    if not path.is_dir():
        raise fastapi.HTTPException(status_code=404, detail="Path not found")

    root_paths = [entry.decoded_path for entry in repo.get_roots()]
    exclude_paths = repo.db.config.excludes

    children: list[PathInfo] = []
    try:
        query_path_info = PathInfo.from_path(
            path,
            root_paths,
            exclude_paths,
        )
        for subpath in path.iterdir():
            if subpath.is_dir():
                children.append(
                    PathInfo.from_path(
                        subpath,
                        root_paths,
                        exclude_paths,
                    )
                )
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission Denied")

    return Browse.model_construct(
        info=query_path_info, children=natsort.os_sorted(children, key=lambda x: x.path)
    )


@api.get("/excludes/")
@depends_on_config_keys("excludes")
async def get_excludes(
    repo: RepoDependency,
) -> list[PathInfo]:
    root_paths = [entry.decoded_path for entry in repo.get_roots()]
    return [
        PathInfo.from_path(path, roots=root_paths, excludes=repo.db.config.excludes)
        for path in repo.db.config.excludes
    ]


@api.put("/excludes/{key}")
async def put_exclude(repo: RepoDependency, key: PathType):
    repo.db.config.excludes.add(key)
    repo.db.config.save(repo)


@api.delete("/excludes/{key}")
async def delete_exclude(repo: RepoDependency, key: PathType):
    repo.db.config.excludes.remove(key)
    repo.db.config.save(repo)


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
    return repo.jobs.scan.status


@api.post("/scan")
async def scan_start(repo: RepoDependency):
    logger.debug("Starting scan")
    try:
        task = repo.scan_async()
    except Exception as e:
        return {"status": "Failed to start scan", "error": str(e)}
    task.add_done_callback(
        lambda _: repo.signals.send(
            backathon.api.events.APIReloadSignal(name="scan_info")
        )
    )
    return {"status": "Scan Started"}


class ScanInfo(pydantic.BaseModel):
    unscanned: list[FSEntryType]
    outdatedCount: int
    outdatedSize: int
    totalCount: int
    totalSize: int


@api.get("/scan/info")
async def scan_info(repo: RepoDependency) -> ScanInfo:
    """Gets information about the backup set"""
    needs_scan = repo.db.query(FSEntry, "SELECT * FROM fsentry WHERE new")

    with repo.db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*), SUM(st_size) FROM fsentry WHERE objid IS NULL")
        needs_backup, backup_size = cursor.fetchone()
        cursor.execute("SELECT COUNT(*), SUM(st_size) FROM fsentry")
        total_backup, total_size = cursor.fetchone()

    return ScanInfo.model_construct(
        unscanned=[FSEntryType.from_fsentry(e) for e in needs_scan],
        outdatedCount=needs_backup,
        outdatedSize=backup_size or 0,
        totalCount=total_backup,
        totalSize=total_size or 0,
    )


@api.get("/backup")
async def backup(repo: RepoDependency) -> backathon.backup.BackupProgress | None:
    return repo.jobs.backup.status


@api.post("/backup")
async def backup_start(repo: RepoDependency):
    logger.debug("Starting backup")
    try:
        task = repo.backup_async()
    except Exception as e:
        return {"status": "Failed to start backup", "error": str(e)}
    task.add_done_callback(
        lambda _: repo.signals.send(
            backathon.api.events.APIReloadSignal(name="repository_info")
        )
    )
    task.add_done_callback(
        lambda _: repo.signals.send(
            backathon.api.events.APIReloadSignal(name="scan_info")
        )
    )
    return {"status": "Backup Started"}
