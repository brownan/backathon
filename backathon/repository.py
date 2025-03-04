import asyncio
import hashlib
import io
import json
import logging
import os.path
import pathlib
import secrets
import sqlite3
from typing import Awaitable
from typing import Callable
from typing import cast

from typing_extensions import Buffer
from typing_extensions import Self

import backathon.backup
import backathon.garbage
import backathon.recover
import backathon.restore
import backathon.scan
from backathon import models
from backathon import repoobject
from backathon.backup import ObjectRequest
from backathon.db import Database
from backathon.encryption.base import EncrypterBase
from backathon.encryption.base import Payload
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullEncrypter
from backathon.exceptions import CorruptedRepository
from backathon.job import Job
from backathon.models import FSEntry
from backathon.models import ObjIDType
from backathon.proftools import perf_block
from backathon.repoobject import RawPayload
from backathon.storage.base import StorageBase

logger = logging.getLogger("backathon.repository")

Compressor = Callable[[Buffer], Buffer]


class Backathon:
    """This class represents the high level interface to all operations"""

    def __init__(self, db: Database):
        self.db = db

        self.scan_job: Job[backathon.scan.ScanProgress] = Job()
        self.backup_job: Job[backathon.backup.BackupProgress] = Job()

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
        db.config.storage = storage.__class__
        db.config.storage_config = storage.config

        db.config.encrypter = encrypter.__class__
        db.config.encrypter_config = encrypter.config

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

    async def scan_async(
        self,
        skip_existing: bool = False,
        rescan_dirs: bool = False,
    ):
        """Launches a scan in a separate thread. Returns a Future
        which completes when the scan is finished.

        """
        if self.scan_job.is_running():
            raise RuntimeError("Scan already running")
        if self.backup_job.is_running():
            raise RuntimeError("Backup is running")

        loop = asyncio.get_running_loop()

        def report_progress(progress):
            loop.call_soon_threadsafe(self.scan_job.progress_callback, progress)

        def scan_thread():
            db_clone = self.db.clone()

            try:
                backathon.scan.scan(
                    db_clone,
                    progress_callback=report_progress,
                    skip_existing=skip_existing,
                    rescan_dirs=rescan_dirs,
                )
            finally:
                db_clone.close()

        task = asyncio.ensure_future(asyncio.to_thread(scan_thread))
        self.scan_job.set_task(task)
        await task

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
    ) -> Callable[[ObjectRequest], Awaitable[models.Object]]:
        compressor: Compressor | None = None
        if self.db.config.enable_compression:
            compressor = repoobject.compress_payload
        encrypter: EncrypterBase = self.get_encrypter()
        storage: StorageBase = self.get_storage()
        return make_obj_putter(self.db, compressor, encrypter, storage)

    def _make_snapshot_putter(self):
        encrypter: EncrypterBase = self.get_encrypter()
        storage: StorageBase = self.get_storage()
        return make_snapshot_putter(self.db, encrypter, storage)

    def make_obj_getter(self, password: str | None) -> "ObjGetter":
        encrypter = self.get_encrypter()
        if password is not None:
            encrypter.unlock(password)

        return make_obj_getter(encrypter, self.get_storage())

    async def backup_async(
        self,
    ):
        """Launches a backup task in the current event loop and returns the
        Task object

        """
        if self.scan_job.is_running():
            raise RuntimeError("Scan is running")
        if self.backup_job.is_running():
            raise RuntimeError("Backup already running")

        db_clone = self.db.clone()
        backup = backathon.backup.Backup(
            db_clone,
            self._make_obj_putter(),
            self._make_snapshot_putter(),
            progress=self.backup_job.progress_callback,
        )
        task = asyncio.create_task(backup.backup())
        self.backup_job.set_task(task)

        await task

    def get_encrypter(self) -> EncrypterBase:
        encrypter_cls = self.db.config.encrypter
        assert issubclass(encrypter_cls, EncrypterBase)
        config_cls = encrypter_cls.get_config_class()
        config = config_cls.model_validate(self.db.config.encrypter_config)
        # pyright seems to not properly detect that the config type and encrypter type will
        # always correspond here
        return encrypter_cls(config)  # pyright: ignore [reportArgumentType]

    def get_storage(self) -> StorageBase:
        storage_cls = self.db.config.storage
        assert issubclass(storage_cls, StorageBase)
        config_cls = storage_cls.get_config_class()
        config = config_cls.model_validate(self.db.config.storage_config)
        return storage_cls(config)

    def restore(
        self,
        root_objid: ObjIDType,
        restoredir: str | os.PathLike[str],
        password: str | None,
    ):
        get_object = self.make_obj_getter(password)

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
            obj = await asyncio.to_thread(raw_payload.upload, storage, compressor)

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
    an object from a remote repository. The object getter returns a RawPayload
    object.

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
