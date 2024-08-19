import asyncio
import hashlib
import hmac
import io
import json
import logging
import os.path
import pathlib
import secrets
import sqlite3
from typing import IO, Awaitable, Callable, Type

from typing_extensions import Self

import backathon.backup
import backathon.garbage
import backathon.restore
from backathon import models, repoobject
from backathon.backup import ObjectRequest
from backathon.db import Database
from backathon.encryption.base import EncrypterBase, Payload
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullEncrypter
from backathon.exceptions import CorruptedRepository
from backathon.models import ObjectHeader, ObjIDType
from backathon.restore import GetObject
from backathon.storage.base import StorageBase
from backathon.storage.local import LocalStorage

logger = logging.getLogger("backathon.repository")

Compressor = Callable[[io.BytesIO], io.BytesIO]


class Backathon:
    """This class represents the high level interface to all operations"""

    def __init__(self, db: Database):
        self.db = db

    @classmethod
    def initialize(
        cls,
        db_path: str | os.PathLike[str],
        storage: StorageBase,
        encrypter: EncrypterBase,
    ) -> Self:
        # Try and write to the remote repo before we do anything else
        recovery_params = encrypter.get_recovery_state()
        marker_data = {
            "name": "Backathon Repository",
            "version": backathon.__version__,
            "encryption": recovery_params,
        }
        json_data = json.dumps(marker_data, indent=4).encode("utf-8")
        payload = Payload(
            io.BytesIO(json_data), len(json_data), hashlib.sha1(json_data).digest()
        )
        storage.put_object(pathlib.PurePath("backathon.json"), payload)

        # Set up the local database
        db = Database(db_path, create=True)
        db.config_set("storage", storage.__class__.__name__)
        db.config_set_json("storage-config", storage.config)

        db.config_set("encrypter", encrypter.__class__.__name__)
        db.config_set_json("encrypter-config", encrypter.config)

        return cls(db)

    def scan(self, skip_existing=False, progress=None, rescan_dirs: bool = False):
        """Scans the backup set

        The backup set is the set of files and directories starting at the
        root paths.

        See more info in the backathon.scan module
        """
        from backathon import scan

        scan.scan(
            self.db,
            progress=progress,
            skip_existing=skip_existing,
            rescan_dirs=rescan_dirs,
        )

    def add_root(self, root_path: pathlib.Path):
        """Adds a new root path to the backup set

        This just adds the root. The caller may want to call
        scan(skip_existing=True) afterwards to update the local filesystem
        cache.

        If this entry is already a root or is a descendant of an existing
        root, this call raises an IntegrityError
        """
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO fsentry (path) VALUES (?)",
                (models.FSEntry.encode_path(root_path),),
            )

    def del_root(self, root_path: pathlib.Path):
        with self.db.cursor() as cursor:
            cursor.execute(
                "DELETE FROM fsentry WHERE path=? AND parent IS NULL",
                (models.FSEntry.encode_path(root_path),),
            )

    def get_roots(self) -> list[models.FSEntry]:
        return list(
            self.db.query(models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL")
        )

    def _make_obj_putter(
        self,
        compressor: Compressor | None,
        encrypter: EncrypterBase,
        storage: StorageBase,
    ) -> Callable[[ObjectRequest], Awaitable[models.Object]]:
        return make_obj_putter(self.db, compressor, encrypter, storage)

    def _make_snapshot_putter(self, encrypter: EncrypterBase, storage: StorageBase):
        return make_snapshot_putter(self.db, encrypter, storage)

    def _make_obj_getter(
        self, encrypter: EncrypterBase, storage: StorageBase
    ) -> GetObject:
        return make_obj_getter(encrypter, storage)

    def backup(self):
        """Perform a backup

        See documentation in the backathon.backup module

        """
        compressor: Compressor | None = None
        if self.db.config_get("enable-compression", True):
            compressor = repoobject.compress_payload
        encrypter: EncrypterBase = self.get_encrypter()
        storage: StorageBase = self.get_storage()

        put_object = self._make_obj_putter(compressor, encrypter, storage)
        put_snapshot = self._make_snapshot_putter(encrypter, storage)

        backup = backathon.backup.Backup(self.db, put_object, put_snapshot)
        logger.debug("Starting event loop")
        asyncio.run(backup.backup(), debug=logger.isEnabledFor(logging.DEBUG))

    def get_encrypter(self) -> EncrypterBase:
        encrypter_cls_name = self.db.config_get("encrypter")
        if encrypter_cls_name == "NaclEncrypter":
            encrypter_cls = NaclEncrypter
        elif encrypter_cls_name == "NullEncrypter":
            encrypter_cls = NullEncrypter
        else:
            raise RuntimeError(
                f"Invalid or unknown encryption backend: {encrypter_cls_name}"
            )
        config_cls = encrypter_cls.get_config_class()
        config = config_cls.model_validate(self.db.config_get_json("encrypter-config"))
        # pyright seems to not properly detect that the config type and encrypter type will
        # always correspond here
        return encrypter_cls(config)  # pyright: ignore [reportArgumentType]

    def get_storage(self) -> StorageBase:
        storage_cls_name = self.db.config_get("storage")
        storage_cls: Type[StorageBase]
        if storage_cls_name == "LocalStorage":
            storage_cls = LocalStorage
        else:
            raise RuntimeError(f"Invalid or unknown storage backend: {storage_cls_name}")

        config_cls = storage_cls.get_config_class()
        config = config_cls.model_validate(self.db.config_get_json("storage-config"))
        return storage_cls(config)

    def restore(
        self,
        root_objid: ObjIDType,
        restoredir: str | os.PathLike[str],
        password: str | None,
    ):
        encrypter = self.get_encrypter()
        if password is not None:
            encrypter.unlock(password)
        storage = self.get_storage()

        get_object = self._make_obj_getter(encrypter, storage)

        asyncio.run(
            backathon.restore.restore_obj(
                root_objid,
                pathlib.Path(restoredir),
                get_object,
            )
        )

    def collect_garbage(self):
        """Initiates the garbage collection process"""
        storage = self.get_storage()
        backathon.garbage.collect_garbage(self.db, storage)


def make_obj_putter(
    db: Database,
    compressor: Compressor | None,
    encrypter: EncrypterBase,
    storage: StorageBase,
) -> Callable[[ObjectRequest], Awaitable[models.Object]]:
    """Returns an object putter function

    The object putter takes care of coordinating the operations that must be done
    to upload an object to the remote repository:

    * Creating the objid via the encryption backend
    * Checking whether the object already exists
    * Encrypting and compressing the object payload via the given encrypter and compressor
    * Upload the object via the storage backend
    * Add a row to the objects table in the database
    * Add rows to the object_relations table
    * Returning an Object model instance

    The object putter is a coroutine and must be called in the main thread (because
    it accesses the database)

    """

    async def put_object(obj_req: ObjectRequest) -> models.Object:
        raw_payload = repoobject.make_obj_payload(obj_req)

        # Make the objid
        objid = await asyncio.to_thread(encrypter.make_objid, raw_payload)

        # Check if this object already exists
        with db.cursor(retdict=True) as cursor:
            cursor.execute("SELECT * FROM objects WHERE objid=?", (objid,))
            row = cursor.fetchone()
            if row is not None:
                return models.Object.model_validate(row)

        # Compress
        if compressor is not None:
            compressed_payload = await asyncio.to_thread(compressor, raw_payload)
        else:
            compressed_payload = raw_payload
        del raw_payload

        # Encrypt
        encrypted_payload = await asyncio.to_thread(encrypter.encrypt, compressed_payload)
        del compressed_payload

        await asyncio.to_thread(
            storage.put_object, repoobject.make_object_path(objid), encrypted_payload
        )

        with db.atomic(), db.cursor() as cursor:
            try:
                cursor.execute(
                    """
                INSERT INTO objects
                (objid, type, uploaded_size, file_size, last_modified_time, sha1)
                VALUES (?,?,?,?,?,?)
                """,
                    (
                        objid,
                        obj_req.header.type,
                        encrypted_payload.size,
                        obj_req.header.file_size,
                        obj_req.header.last_modified_time,
                        encrypted_payload.sha1,
                    ),
                )
            except sqlite3.IntegrityError:
                # This can happen if two backup threads try to upload an identical
                # object, which isn't too unlikely in practice. Since they are
                # cryptographically guaranteed to be identical (including relations),
                # we can just query that one back out and return it.
                # The fact that the object was uploaded twice is an unfortunate
                # inefficiency but I believe it won't be too bad overall.
                return next(
                    db.query(
                        models.Object, "SELECT * FROM objects WHERE objid=?", (objid,)
                    )
                )

            # Add object relations
            children: list[tuple[ObjIDType, ObjIDType, bytes | None]] = []
            if obj_req.header.blobs:
                children.extend((objid, b.objid, None) for b in obj_req.header.blobs)
            if obj_req.header.entries:
                children.extend((objid, e.objid, e.name) for e in obj_req.header.entries)
            cursor.executemany(
                "INSERT INTO object_relations (parent, child, name) VALUES (?,?,?)",
                children,
            )

            return models.Object(
                objid=objid,
                type=obj_req.header.type,
                uploaded_size=encrypted_payload.size,
                file_size=obj_req.header.file_size,
                last_modified_time=obj_req.header.last_modified_time,
                sha1=encrypted_payload.sha1,
            )

    return put_object


def make_snapshot_putter(
    db: Database, encrypter: EncrypterBase, storage: StorageBase
) -> Callable[[models.Snapshot], None]:
    """Returns a snapshot putter function

    The snapshot putter is called to upload a final snapshot definition to the remote
    repository. It coordinates the following operations:

    * Creating the snapshot metadata
    * Encrypting the metadata via the given encrypter
    * Uploading the metadata to the remote repository via the given storage backend
    * Updating the local database with the snapshot metadata
    """

    def put_snapshot(snapshot: models.Snapshot):
        snapshot_path = pathlib.Path("snapshots", secrets.token_urlsafe())
        buf = io.BytesIO()
        buf.write(snapshot.model_dump_json(indent=4).encode("utf-8"))
        buf.seek(0)

        payload = encrypter.encrypt(buf)
        storage.put_object(snapshot_path, payload)

        # Update database
        with db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO snapshots (path, root, timestamp) VALUES (?,?,?)",
                (snapshot.path, snapshot.root, snapshot.timestamp),
            )

    return put_snapshot


def make_obj_getter(encrypter: EncrypterBase, storage: StorageBase) -> GetObject:
    """Returns an object getter function

    The object getter's job is to retrieve, decrypt, decompress, verify, and deserialize
    an object from a remote repository. The object getter returns a tuple of
    (ObjectHeader, IO[bytes]).

    The ObjectHeader is the deserialized header from the object, and the byte stream
    is the object body (bytes following the header).

    The byte stream is a file-like object open for reading. The file's position may
    not be at byte 0; it may just point to the position in the object where the body
    starts. The exact details of what kind of object the byte stream is depends on the
    storage and encryption implementation. Callers can assume it's seekable, and that
    the number of bytes in the body matches the header's length.

    Callers MUST close the file-like object when finished reading. Callers should not rely
    on garbage collection to close the byte stream.

    """

    async def get_object(objid: ObjIDType) -> tuple[ObjectHeader, IO[bytes]]:
        raw_stream = await asyncio.to_thread(
            storage.get_object, repoobject.make_object_path(objid)
        )
        decrypted = encrypter.decrypt(raw_stream)
        decompressed = repoobject.decompress_payload(decrypted)
        if raw_stream is not decompressed:
            raw_stream.close()

        # Check obj id
        actual_objid = encrypter.make_objid(decompressed)
        if not hmac.compare_digest(actual_objid, objid):
            raise CorruptedRepository(f"Corrupted Object: {objid.hex()}")

        header = models.ObjectHeader.from_stream(decompressed)

        # Verify the body length matches the length in the header
        pos = decompressed.tell()
        decompressed.seek(0, io.SEEK_END)
        length = decompressed.tell() - pos
        decompressed.seek(pos)

        if length != header.length:
            raise CorruptedRepository(f"Object length mismatch: {objid.hex()}")

        return header, decompressed

    return get_object
