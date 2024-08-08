import asyncio
import hashlib
import io
import json
import logging
import os.path
import pathlib
import secrets
import zlib
from typing import Awaitable, Callable, Type

from typing_extensions import Self

import backathon.backup
from backathon import models
from backathon.backup import ObjectRequest
from backathon.db import Database
from backathon.encryption.base import EncrypterBase, Payload
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullEncrypter
from backathon.models import ObjIDType
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
        cls, db_path: os.PathLike, storage: StorageBase, encrypter: EncrypterBase
    ) -> Self:
        # Try and write to the remote repo before we do anything else
        recovery_params = encrypter.get_recovery_state()
        json_data = json.dumps(recovery_params, indent=4).encode("utf-8")
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
            self.db.get_objects(
                models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL"
            )
        )

    def _make_obj_payload(self, obj_req: ObjectRequest) -> io.BytesIO:
        # Build the object contents
        if (
            isinstance(obj_req.body, io.BytesIO)
            and len(obj_req.body.getbuffer()) != obj_req.header.length
        ):
            raise RuntimeError("Size mismatch between header and body")

        raw_payload = io.BytesIO()
        raw_payload.write(obj_req.header.model_dump_msgpack())
        if obj_req.body is not None:
            if isinstance(obj_req.body, io.BytesIO):
                raw_payload.write(obj_req.body.getbuffer())
            else:
                raw_payload.write(obj_req.body.read())
        raw_payload.seek(0)
        return raw_payload

    def _compress_payload(self, raw: io.BytesIO) -> io.BytesIO:
        buf = raw.getbuffer()
        compressed_bytes = zlib.compress(buf)
        if len(compressed_bytes) < len(buf):
            return io.BytesIO(compressed_bytes)
        else:
            return raw

    def _decompress_payload(self, compressed: io.BytesIO) -> io.BytesIO:
        buf = compressed.getbuffer()
        if buf[0] == 0x78:
            # zlib identification marker
            return io.BytesIO(zlib.decompress(buf))
        else:
            return compressed

    def _make_obj_putter(
        self,
        compressor: Compressor | None,
        encrypter: EncrypterBase,
        storage: StorageBase,
    ) -> Callable[[ObjectRequest], Awaitable[models.Object]]:
        async def put_object(obj_req: ObjectRequest) -> models.Object:
            """Called from the backup code to upload an object to the remote repository

            Responsible for:
            * Uploading the object
            * Adding the entry to the objects table
            * Adding any object relations to the relations table
            * creating the models.Object instance and returning it
            """

            raw_payload = self._make_obj_payload(obj_req)

            # Make the objid
            objid = encrypter.make_objid(raw_payload)

            # Check if this object already exists
            with self.db.cursor(retdict=True) as cursor:
                cursor.execute("SELECT * FROM objects WHERE objid=?", (objid,))
                row = cursor.fetchone()
                if row is not None:
                    return models.Object.model_validate(row)

            # TODO: dispatch the following cpu-heavy operations to a thread pool (after
            # measuring whether it will actually improve performance, of course)

            # Compress
            if compressor is not None:
                compressed_payload = compressor(raw_payload)
            else:
                compressed_payload = raw_payload
            del raw_payload

            # Encrypt
            encrypted_payload = encrypter.encrypt(compressed_payload)
            del compressed_payload

            objid_hex = objid.hex()
            path = pathlib.Path("objects", objid_hex[:3], objid_hex)

            storage.put_object(path, encrypted_payload)

            with self.db.atomic(), self.db.cursor() as cursor:
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

                # Add object relations
                children: list[tuple[ObjIDType, ObjIDType, bytes | None]] = []
                if obj_req.header.blobs:
                    children.extend((objid, b.objid, None) for b in obj_req.header.blobs)
                if obj_req.header.entries:
                    children.extend(
                        (objid, e.objid, e.name) for e in obj_req.header.entries
                    )
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

    def _make_snapshot_putter(self, encrypter: EncrypterBase, storage: StorageBase):
        def put_snapshot(snapshot: models.Snapshot):
            snapshot_path = pathlib.Path("snapshot", secrets.token_urlsafe())
            buf = io.BytesIO()
            buf.write(snapshot.model_dump_json(indent=4).encode("utf-8"))
            buf.seek(0)

            payload = encrypter.encrypt(buf)
            storage.put_object(snapshot_path, payload)

        return put_snapshot

    def backup(self):
        """Perform a backup

        See documentation in the backathon.backup module

        """
        compressor: Compressor | None = None
        if self.db.config_get_json("enable_compression", True):
            compressor = self._compress_payload
        encrypter: EncrypterBase = self.get_encrypter()
        storage: StorageBase = self.get_storage()

        put_object = self._make_obj_putter(compressor, encrypter, storage)
        put_snapshot = self._make_snapshot_putter(encrypter, storage)

        backup_coro = backathon.backup.backup(self.db, put_object, put_snapshot)
        logger.debug("Starting event loop")
        asyncio.run(backup_coro, debug=logger.isEnabledFor(logging.DEBUG))

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
