import datetime
import hashlib
import io
import pathlib
import shutil
import uuid
from abc import ABC, abstractmethod
from typing import IO, Callable, NamedTuple

import msgpack

from backathon import models
from backathon.backup import ObjectRequest
from backathon.db import Database


class FilesystemUploader(UploaderBase):
    def __init__(self, context: UploaderContext, path: pathlib.Path):
        super().__init__(context)
        self.path = path

    def _make_objid(self, buf: IO[bytes]) -> bytes:
        hasher = hashlib.sha256()
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            hasher.update(chunk)
        buf.seek(0)
        return hasher.digest()

    def _get_sha1_and_size(self, buf: IO[bytes]) -> tuple[bytes, int]:
        size = 0
        hasher = hashlib.sha1()
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            size += len(chunk)
            hasher.update(chunk)
        buf.seek(0)
        return hasher.digest(), size

    def upload(self, request: ObjectRequest) -> models.Object:
        # TODO: objid could also be calculated by the caller
        objid = self.make_objid(request.payload)

        # Check if this object is already in the database
        # TODO: this should be done before calling this method. The caller should have
        # everything it needs.
        with self.db.cursor(retdict=True) as cursor:
            cursor.execute("SELECT * FROM objects WHERE objid=?", (objid,))
            row = cursor.fetchone()
            if row is not None:
                return models.Object.model_validate(row)

        objid_hex = objid.hex()

        # TODO: in fact, does the uploader's upload() method need to do any of this?
        # It really should just take an objid and an encrypted payload and push it to the remote
        # storage
        encrypted_buffer, size, sha1_digest = self.encrypt(request.payload)

        path = self.path / "objects" / objid_hex[:2] / objid_hex
        with path.open("wb") as fobj:
            shutil.copyfileobj(encrypted_buffer, fobj)

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

    def put_snapshot(self, path: bytes, root_objid: bytes, date: datetime.datetime):
        snapshot_name = self.path / "snapshots" / str(uuid.uuid4())
        packer = msgpack.Packer()
        buf = io.BytesIO()
        buf.write(packer.pack("snapshot"))
        buf.write(
            packer.pack(
                {
                    "date": date,
                    "root": root_objid,
                    "path": path,
                }
            )
        )
        buf.seek(0)
        with snapshot_name.open("wb") as fobj:
            shutil.copyfileobj(buf, fobj)
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO snapshots (path, root, date) VALUES (?,?,?)",
                (path, root_objid, datetime.datetime),
            )
