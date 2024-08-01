import hashlib
import io
import pathlib
import shutil
from abc import ABC, abstractmethod
from typing import IO

from backathon import models
from backathon.backup import ObjectRequest
from backathon.db import Database


class UploaderBase(ABC):
    @abstractmethod
    def upload(self, request: ObjectRequest) -> models.Object:
        ...


class FilesystemUploader(UploaderBase):
    def __init__(self, db: Database, path: pathlib.Path):
        self.db = db
        self.path = path

    def _make_objid(self, buf: IO[bytes]) -> tuple[bytes, bytes, int]:
        hasher = hashlib.sha256()
        hasher_sha1 = hashlib.sha1()
        size = 0
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            size += len(chunk)
            hasher.update(chunk)
            hasher_sha1.update(chunk)
        buf.seek(0)
        return hasher.digest(), hasher_sha1.digest(), size

    def upload(self, request: ObjectRequest) -> models.Object:
        objid, sha1_digest, size = self._make_objid(request.payload)

        # Check if this object is already in the database
        with self.db.cursor(retdict=True) as cursor:
            cursor.execute("SELECT * FROM objects WHERE objid=?", (objid,))
            row = cursor.fetchone()
            if row is not None:
                return models.Object.model_validate(row)

        objid_hex = objid.hex()
        path = self.path / "objects" / objid_hex[:2] / objid_hex
        with path.open("wb") as fobj:
            shutil.copyfileobj(request.payload, fobj)
        with self.db.atomic(), self.db.cursor() as cursor:
            cursor.execute(
                """INSERT INTO objects
                (objid, type, uploaded_size, file_size, last_modified_time, sha1)
                VALUES (?,?,?,?,?,?)""",
                (
                    objid,
                    request.type,
                    size,
                    request.file_size,
                    request.last_modified_time,
                    sha1_digest,
                ),
            )
            cursor.executemany(
                "INSERT INTO object_relations (parent, child, name) VALUES (?,?,?)",
                ((objid, c[0], c[1]) for c in request.children),
            )
        return models.Object(
            objid=objid,
            type=request.type,
            uploaded_size=size,
            file_size=request.file_size,
            last_modified_time=request.last_modified_time,
            sha1=sha1_digest,
        )
