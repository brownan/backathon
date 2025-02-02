import asyncio
import hashlib
import io
import json
import logging
import os.path
import pathlib
import secrets
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, Type, cast

from typing_extensions import Buffer, Self

import backathon.backup
import backathon.garbage
import backathon.recover
import backathon.restore
import backathon.scan
from backathon import models, repoobject
from backathon.backup import BackupProgressReport, ObjectRequest
from backathon.db import Database
from backathon.encryption.base import EncrypterBase, Payload
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullEncrypter
from backathon.exceptions import CorruptedRepository
from backathon.models import FSEntry, ObjIDType
from backathon.proftools import perf_block
from backathon.repoobject import RawPayload
from backathon.storage.base import StorageBase
from backathon.storage.local import LocalStorage

logger = logging.getLogger("backathon.repository")

Compressor = Callable[[Buffer], Buffer]


class Backathon:
    """This class represents the high level interface to all operations"""

    def __init__(self, db: Database):
        self.db = db

        self.scan_task: asyncio.Task | None = None
        self.scan_progress: backathon.scan.ScanProgress | None = None

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
            "encrypter": encrypter.__class__.__name__,
            "encrypter-params": recovery_params,
        }
        json_data = json.dumps(marker_data, indent=4).encode("utf-8")
        payload = Payload(json_data, len(json_data), hashlib.sha1(json_data).digest())
        storage.put_object(pathlib.PurePath("backathon.json"), payload)

        # Set up the local database
        db = Database(db_path, create=True)
        db.config_set("storage", storage.__class__.__name__)
        db.config_set_json("storage-config", storage.config)

        db.config_set("encrypter", encrypter.__class__.__name__)
        db.config_set_json("encrypter-config", encrypter.config)

        return cls(db)

    @classmethod
    async def recover(
        cls,
        db_path: str | os.PathLike[str],
        storage: StorageBase,
        password: str | None = None,
    ) -> Self:
        """Initializes a new local database from an existing remote repository

        If the remote repository requires encryption, the appropriate encrypter will be
        initialized with the given password

        """
        db_path = pathlib.Path(db_path)
        if db_path.exists():
            raise FileExistsError(f"Local config database already exists: {db_path}")

        with await asyncio.to_thread(storage.get_object, "backathon.json") as data:
            marker_data = json.load(data.stream)

        if (
            not isinstance(marker_data, dict)
            or marker_data.get("name") != "Backathon Repository"
        ):
            raise CorruptedRepository("This does not look like a Backathon repository")

        encrypter_name = cast(str, marker_data.get("encrypter"))
        encryption_recovery_params = cast(dict, marker_data.get("encrypter-params"))

        if encrypter_name == "NullEncrypter":
            enc_cls = NullEncrypter
        elif encrypter_name == "NaclEncrypter":
            enc_cls = NaclEncrypter
            logger.info(
                "Repository found. Encryption parameters: %s", encryption_recovery_params
            )
            logger.info("Decrypting keys using the given password...")
        else:
            raise CorruptedRepository(f"Unknown encrypter: {encrypter_name}")

        encrypter = enc_cls.from_recovery_state(
            encryption_recovery_params,
            password or "",
        )

        logger.info("Creating local database")
        return cls.initialize(
            db_path,
            storage,
            encrypter,
        )

    def close(self):
        self.db.close()

    def scan_async(
        self,
        skip_existing: bool = False,
        rescan_dirs: bool = False,
        progress_callback: Callable[[backathon.scan.ScanProgress], None] | None = None,
    ) -> asyncio.Future:
        """Launches a scan in a separate thread. Returns a Future
        which completes when the scan is finished.

        """
        if self.scan_task is not None:
            raise RuntimeError("Scan already running")

        loop = asyncio.get_running_loop()

        def report_progress(progress):
            self.scan_progress = progress
            if progress_callback is not None:
                loop.call_soon_threadsafe(progress_callback, progress)

        def scan_thread():
            db_clone = self.db.clone()
            from backathon import scan

            scan.scan(
                db_clone,
                progress_callback=report_progress,
                skip_existing=skip_existing,
                rescan_dirs=rescan_dirs,
            )

        task = asyncio.ensure_future(asyncio.to_thread(scan_thread))
        self.scan_task = task

        def on_done(_):
            self.scan_task = None
            self.scan_progress = None

        task.add_done_callback(on_done)
        return task

    def scan(
        self,
        skip_existing=False,
        progress: None | Callable[[int, int | None, str], None] = None,
        rescan_dirs: bool = False,
    ):
        """Scans the backup set

        The backup set is the set of all local files and directories starting at the
        root paths.

        This method scans all local files that are to be backed up, and marks in the
        local database all files and directories that have changed since last backup.
        This informs the backup routines of what files need backing up.

        This should be run before every backup.

        See more info in the backathon.scan module
        """

        def progress_shim(p: backathon.scan.ScanProgress):
            assert progress is not None
            progress(p.scanned, p.total, p.last_path or "")

        backathon.scan.scan(
            self.db,
            progress_callback=progress_shim if progress else None,
            skip_existing=skip_existing,
            rescan_dirs=rescan_dirs,
        )

    def add_root(self, root_path: pathlib.Path) -> FSEntry:
        """Adds a new root path to the backup set

        This just adds the root. The caller may want to call
        scan(skip_existing=True) afterwards to update the local filesystem
        cache.

        If this entry is already a root or is a descendant of an existing
        root, this call raises an IntegrityError
        """
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO fsentry (path) VALUES (?) RETURNING id",
                (models.FSEntry.encode_path(root_path),),
            )
            new_id = cursor.fetchone()[0]
            return next(
                self.db.query(
                    models.FSEntry, "SELECT * FROM fsentry WHERE id = ?", (new_id,)
                )
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
    ) -> "ObjGetter":
        return make_obj_getter(encrypter, storage)

    def backup(
        self, progress: None | Callable[[BackupProgressReport], None] = None
    ) -> BackupProgressReport:
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

        backup = backathon.backup.Backup(
            self.db, put_object, put_snapshot, progress=progress
        )
        logger.debug("Starting event loop")
        with asyncio.Runner() as runner:
            loop = runner.get_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=os.cpu_count() or 4))
            runner.run(backup.backup())
        return backup.progress

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

    def upload_payload(buf: Buffer, path: pathlib.PurePosixPath) -> Payload:
        with perf_block("upload_payload_inner"):
            if compressor is not None:
                with perf_block("upload_payload_inner.compressor"):
                    buf = compressor(buf)

            with perf_block("upload_payload_inner.encrypter"):
                payload = encrypter.encrypt(buf)

            with perf_block("upload_payload_inner.put_object"):
                storage.put_object(path, payload)
            return payload

    async def put_object(obj_req: ObjectRequest) -> models.Object:
        # Create the raw payload, including calculation of the objid
        # While this is a CPU-bound routine, we perform it in the main thread with
        # the event loop. From experimentation, I've found the blake2b hash algorithm is
        # so fast that trying to parallelize it doesn't overcome the overhead of dispatching
        # to a separate thread pool. Whether this is due to actual thread synchronization
        # overhead or just contention in the default threadpool executor I'm not sure.
        raw_payload = repoobject.RawPayload.from_obj_req(obj_req, encrypter)
        objid = raw_payload.objid

        # Check if this object already exists
        with db.cursor(retdict=True) as cursor:
            cursor.execute("SELECT * FROM objects WHERE objid=?", (objid,))
            row = cursor.fetchone()
            if row is not None:
                obj = models.Object.model_validate(row)
                obj.from_cache = True
                return obj

        with perf_block("put_object.upload_payload to_thread"):
            obj = await asyncio.to_thread(raw_payload.upload, storage)

        # The child relations
        children = raw_payload.get_children()

        try:
            obj.add_to_database(db, children)
        except sqlite3.IntegrityError:
            # This can happen if two backup threads try to upload an identical
            # object, which isn't too unlikely in practice. Since they are
            # cryptographically guaranteed to be identical (including relations),
            # we know it and the child relations have already been added.
            # The fact that the payload was uploaded twice is an unfortunate
            # inefficiency but I believe it won't be too bad overall.
            pass
        return obj

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

        payload = encrypter.encrypt(buf.getbuffer())
        storage.put_object(snapshot_path, payload)

        # Update database
        with db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO snapshots (path, root, timestamp) VALUES (?,?,?)",
                (snapshot.path, snapshot.root, snapshot.timestamp),
            )

    return put_snapshot


ObjGetter = Callable[[ObjIDType], Awaitable[RawPayload]]


def make_obj_getter(encrypter: EncrypterBase, storage: StorageBase) -> ObjGetter:
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

    def get_and_decrypt(objid: ObjIDType) -> RawPayload:
        with storage.get_object(repoobject.make_object_path(objid)) as downloaded_file:
            return RawPayload.from_encrypted_payload(
                objid,
                downloaded_file.stream,
                encrypter,
            )

    async def get_object(objid: ObjIDType) -> RawPayload:
        return await asyncio.to_thread(get_and_decrypt, objid)

    return get_object
