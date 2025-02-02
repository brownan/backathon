import asyncio
import contextlib
import os
import pathlib
from typing import Annotated

from fastapi import (
    Body,
    Depends,
    FastAPI,
    HTTPException,
    Path,
    Request,
)

from backathon import Backathon, Database
from backathon.models import FSEntry, Object


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
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

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def top(db: DatabaseDependency, repo: RepoDependency):
    return {"message": "Hello, world!", "db": repr(db), "repo": repr(repo)}


@app.get("/roots/")
async def list_roots(repo: RepoDependency) -> list[FSEntry]:
    return repo.get_roots()


@app.post("/roots/")
async def add_root(
    repo: RepoDependency, path: Annotated[str, Body(embed=True)]
) -> FSEntry:
    entry = repo.add_root(pathlib.Path(path))
    return entry


@app.get("/roots/{id}")
async def get_root(repo: RepoDependency, id: int) -> FSEntry:
    entry = next(
        repo.db.query(FSEntry, "SELECT * FROM fsentry WHERE id = ?", (id,)), None
    )
    if entry is None:
        raise HTTPException(status_code=404)

    if entry.parent is not None:
        raise HTTPException(status_code=404)

    return entry


@app.delete("/roots/{id}")
async def del_root(repo: RepoDependency, id: int):
    entry = await get_root(repo, id)
    with repo.db.cursor() as cursor:
        cursor.execute("DELETE FROM fsentry WHERE id = ?", (entry.id,))


@app.get("/objects/{objid}")
async def get_object(
    repo: RepoDependency, objid: Annotated[str, Path(pattern=r"[0-9a-fA-F]+")]
) -> Object:
    obj = next(
        repo.db.query(
            Object, "SELECT * FROM objects WHERE objid = ?", (bytes.fromhex(objid),)
        ),
        None,
    )
    if obj is None:
        raise HTTPException(status_code=404)
    return obj


@app.get("/excludes/")
async def get_excludes(
    repo: RepoDependency,
) -> list[str]:
    return repo.db.config_get_json("excludes", [])


@app.post("/excludes/")
async def set_excludes(
    repo: RepoDependency, new_excludes: Annotated[list[str], Body()]
) -> list[str]:
    repo.db.config_set_json("excludes", new_excludes)
    return new_excludes
