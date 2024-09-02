import asyncio
import dataclasses
import datetime
import io
import os
import signal
import stat
import threading
import time
from contextlib import ExitStack, contextmanager
from logging import getLogger
from operator import attrgetter, itemgetter
from typing import (
    IO,
    Awaitable,
    Callable,
    Coroutine,
    NamedTuple,
    cast,
)

import rich.filesize

from backathon import chunker, models
from backathon.asyncutils import BoundedTaskGroup
from backathon.db import Database
from backathon.exceptions import DependencyError
from backathon.models import (
    BlobRef,
    EntryRef,
    ObjectHeader,
    ObjectStats,
    ObjectType,
    ObjIDType,
)
from backathon.proftools import perf_block

logger = getLogger("backathon.backup")


class ObjectRequest(NamedTuple):
    """Used by the backup code to pass information about an object to be uploaded to the upload
    code

    """

    header: ObjectHeader
    body: IO[bytes] | None


@dataclasses.dataclass
class _EntryProgress:
    path: str
    started: bool = False
    bytes_backed_up: int = 0
    bytes_total: int = 0


@dataclasses.dataclass
class BackupProgressReport:
    count_progress: int = 0
    count_total: int = 0
    size_progress: int = 0
    size_total: int = 0
    actual_uploaded: int = 0
    current_entries: list[_EntryProgress] = dataclasses.field(default_factory=list)
    objects_processed: int = 0
    objects_uploaded: int = 0


class _ProcessingResult(NamedTuple):
    obj: models.Object
    stat_result: os.stat_result


class _TuningParams(NamedTuple):
    # File sizes below this will be inlined into the "file" object's body, rather than
    # referenced in separate "blob" objects
    inline_threshold: int

    # File sizes below this but above the inline threshold will be saved to a single
    # "blob" object separate from the "file" object, enabling deduplication
    # chunk_threshold should be above the inline_threshold
    chunk_threshold: int

    # If a file size exceeds the chunk_threshold, then each chunk will be at most
    # chunk_size large.
    # Note that this may be smaller than the chunk_threshold. chunk_threshold sets the
    # file size below which it's not worth chunking at all. Once a file is worth chunking,
    # chunk_size determines how large each chunk is.
    chunk_size: int

    max_backup_workers: int


@contextmanager
def install_sigint_handler(handler):
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, handler)
        yield
    finally:
        loop.remove_signal_handler(signal.SIGINT)


class Backup:
    def __init__(
        self,
        db: Database,
        put_object: Callable[[ObjectRequest], Awaitable[models.Object]],
        put_snapshot: Callable[[models.Snapshot], None],
        progress: None | Callable[[BackupProgressReport], None] = None,
    ):
        self.db = db
        self.put_object = put_object
        self.put_snapshot = put_snapshot
        self.progress_callback = progress

        # Get some config items
        inline_threshold = max(0, db.config_get("inline-threshold", 2**20))
        self.params = _TuningParams(
            inline_threshold=inline_threshold,
            chunk_threshold=max(
                0, inline_threshold, db.config_get("chunk-threshold", 30 * 2**20)
            ),
            chunk_size=max(2**16, db.config_get("chunk-size", 10 * 2**20)),
            max_backup_workers=max(1, db.config_get("max-backup-workers", 1)),
        )

        self.progress = BackupProgressReport()

        # Tracks tasks launched to process a single FSEntry.
        # Tasks are removed from this set when they have finished
        # Maps fsentry paths to the task submitted to process that path
        self._processing_tasks: dict[bytes, asyncio.Task] = dict()

    def _backup_items_remain(self) -> bool:
        with self.db.cursor() as cursor:
            cursor.execute("SELECT 1 FROM fsentry WHERE objid IS NULL LIMIT 1")
            return bool(cursor.fetchone())

    async def backup(self):
        with ExitStack() as exitstack:
            cursor = exitstack.enter_context(self.db.cursor())

            def on_cancel(exc_type, exc_val, tb):
                if exc_type and isinstance(exc_val, asyncio.CancelledError):
                    logger.info("Backup cancelled")

            exitstack.push(on_cancel)

            cursor.execute("SELECT COUNT(*) FROM fsentry WHERE objid IS NULL")
            self.progress.count_total = cursor.fetchone()[0] or 0
            self.progress.count_progress = 0

            cursor.execute(
                "SELECT SUM(st_size) FROM fsentry WHERE objid IS NULL AND st_mode & ?",
                (stat.S_IFREG,),
            )
            self.progress.size_total = cursor.fetchone()[0] or 0
            self.progress.size_progress = 0

            logger.info(
                "Starting backup. %s items to backup, totaling %s",
                self.progress.count_total,
                rich.filesize.decimal(self.progress.size_total),
            )

            async with BoundedTaskGroup() as taskgroup:
                while self._backup_items_remain():
                    with self.db.atomic(immediate=True):
                        await self._backup_batch(taskgroup)

                    logger.debug("Checkpointing database")
                    with self.db.cursor() as cursor:
                        cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
                        cursor.execute("PRAGMA optimize")

                # All entries to be backed up have been dispatched. Some tasks may still
                # be running, so here we gather and finalize remaining entries as they finish.

                # Start a new transaction. Once we yield to the event loop, processing
                # tasks resume and may try to upload something, which may involve
                # database writes. While I don't think it would hurt to have each of those
                # be in separate transactions, it's better for performance if they are.
                if self._processing_tasks:
                    with self.db.atomic(immediate=True):
                        await asyncio.wait(
                            self._processing_tasks.values(),
                            return_when=asyncio.ALL_COMPLETED,
                        )
            logger.debug("All tasks done")

        # Add a snapshot object for each root
        logger.debug("Backup finished. Creating snapshot objects")
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.db.atomic(immediate=True), self.db.cursor() as cursor:
            for entry in self.db.query(
                models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL"
            ):
                if entry.objid is None:
                    raise RuntimeError(f"Root not backed up {entry}")
                logger.debug(
                    "Snapshot: %s: %s (%s)",
                    now,
                    entry.objid.hex(),
                    entry.printable_path,
                )
                self.put_snapshot(
                    models.Snapshot.model_construct(
                        path=entry.printable_path, root=entry.objid, timestamp=now
                    )
                )
            cursor.execute("PRAGMA optimize")
        with self.db.cursor() as cursor:
            cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
        logger.info("Backup finished. %s entries backed up", self.progress.count_progress)

    async def _backup_batch(self, taskgroup: BoundedTaskGroup):
        """Fetches a batch of fsentry rows which need backing up, and dispatches tasks
        to process them

        This function will usually return early after backing up some, but not all,
        of what needs backing up. The reason is to let the caller commit the transaction,
        saving progress and preventing the write-ahead-log from growing unbounded.

        Tasks dispatched by this function may still be running when this function returns.

        """
        ct = 0
        time_start = time.monotonic()
        logger.debug(
            "Fetching next batch of entries to backup. Current progress: %s/%s",
            self.progress.count_progress,
            self.progress.count_total,
        )

        with self.db.savepoint():
            entry_iterator = self.db.query(
                models.FSEntry,
                """SELECT * FROM fsentry WHERE
                objid IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM fsentry AS children WHERE
                    children.parent = fsentry.id
                    AND children.objid IS NULL
                ) """,
            )
            try:
                for entry in entry_iterator:
                    ct += 1
                    if entry.objid is not None:
                        raise RuntimeError(
                            f"Backup query received entry already backed up! {entry}"
                        )
                    if entry.path in self._processing_tasks:
                        # This entry has already been submitted by a previous iteration. Skip it.
                        # This can happen if an entry was started by a previous call into _backup_batch()
                        # but it had not yet finished when called again. So the entry re-appears
                        # in the query to get entries which haven't yet been backed up.
                        continue
                    entry_task = await taskgroup.create_task(
                        self._dispatch(
                            entry,
                        )
                    )
                    self._processing_tasks[entry.path] = entry_task

                    # Avoid late binding problem by adding the cleanup callback in a function
                    def add_done_callback(t: asyncio.Task, e: models.FSEntry):
                        t.add_done_callback(lambda _: self._processing_tasks.pop(e.path))

                    add_done_callback(entry_task, entry)

                    if time.monotonic() - time_start > 30:
                        logger.debug(
                            "Breaking to checkpoint. %s tasks pending",
                            len(self._processing_tasks),
                        )
                        return

                # Exited the for loop with no new entries fetched from the database AND
                # nothing currently being processed? This is an error and could indicate
                # some kind of dependency loop or other bug
                if ct == 0 and not self._processing_tasks:
                    raise RuntimeError("Backup loop found no items")

                # Loop exited normally, meaning there are no other items to back up
                # at the moment. (There may be later once some current tasks finish though)
                # Since the loop in backup() doesn't know that we couldn't add any new
                # tasks, we want to make sure at least some state has changed before returning.
                # That way we don't get caught in a busy loop until a task finishes.
                await asyncio.wait(
                    self._processing_tasks.values(),
                    timeout=None,
                    return_when=asyncio.FIRST_COMPLETED,
                )

            finally:
                # Make sure this is closed, even if we exited the loop early. Otherwise,
                # the sqlite cursor will stay open and the caller won't be able to commit
                # the transaction.
                entry_iterator.close()

    async def _dispatch(self, entry: models.FSEntry):
        """This is the launch point of the asyncio Task to process a single FSEntry
        for backup

        This coroutine's job is to set up the parameters and handle the return from
        the _process_entry coroutine, calling _finalize_entry() on the result.

        """
        put_object = self.put_object
        params = self.params

        child_entries = list(
            self.db.query(
                models.FSEntry,
                "SELECT * FROM fsentry WHERE parent=?",
                (entry.id,),
            )
        )
        # Order child entries consistently so database ordering differences
        # doesn't result in different tree objects
        child_entries.sort(key=attrgetter("name"))

        # This coroutine is called from _process_entry() to perform an upload
        async def upload(req: ObjectRequest) -> models.Object:
            with perf_block("put_object"):
                obj = await put_object(req)
            self.progress.objects_processed += 1
            if obj.uploaded_size and not obj.from_cache:
                self.progress.actual_uploaded += obj.uploaded_size
                self.progress.objects_uploaded += 1
            return obj

        progress = _EntryProgress(entry.printable_path)
        self.progress.current_entries.append(progress)

        logger.log(5, "Dispatching process_entry call for %s", entry)
        try:
            with perf_block("_process_entry"):
                result = await _process_entry(
                    entry, child_entries, upload, params, progress
                )
        finally:
            self.progress.current_entries.remove(progress)
        self._finalize_entry(entry, result)
        logger.log(5, "process_entry finished for %s", entry)

    def _finalize_entry(self, e: models.FSEntry, result: _ProcessingResult | None):
        self.progress.count_progress += 1
        if e.st_mode and e.st_size and stat.S_ISREG(e.st_mode):
            self.progress.size_progress += e.st_size

        with self.db.savepoint(), self.db.cursor() as cursor:
            if result is None:
                # Item was not backed up. We need to delete its entry
                cursor.execute("DELETE FROM fsentry WHERE id=?", (e.id,))
            else:
                # Update the fsentry
                if result.obj.objid is None:
                    raise RuntimeError(
                        f"process_entry() returned an object with no id: {result.obj}"
                    )
                e.update(
                    self.db,
                    result.obj.objid,
                    new=False,
                    stat_result=result.stat_result,
                )

        if self.progress_callback is not None:
            self.progress_callback(self.progress)


async def _process_entry(
    entry: models.FSEntry,
    children: list[models.FSEntry],
    upload: Callable[[ObjectRequest], Coroutine[None, None, models.Object]],
    params: _TuningParams,
    progress: _EntryProgress,
) -> _ProcessingResult | None:
    """Prepares the payloads for an FSEntry and calls into the provided upload routine
    to perform the uploads.

    This function may call upload() one or more times to upload new objects to the remote
    repository. The upload() implementation is responsible for:
    * Uploading the object
    * Adding an entry to the objects table corresponding to the uploaded object
    * Adding any given child relations to the object_relations table
    * Creating the models.Object instance
    * Returning the models.Object from the context.upload() call

    When this function returns an Object, the caller is responsible for updating the FSEntry
    row in the database with the new Object's objid and new stat info.

    When this function returns None, the caller is responsible for deleting the FSEntry row from
    the database.

    To keep things simple, this is implemented as a function with no access to global state.
    All state needed is passed in, and all side effects are performed by the passed-in callables.
    """
    logger.log(5, "Begun processing %s in thread %s", entry, threading.current_thread())
    progress.started = True
    try:
        stat_result = os.lstat(entry.path)
    except (FileNotFoundError, NotADirectoryError):
        logger.info("%s: File disappeared", entry.printable_path)
        return None

    if stat.S_ISREG(stat_result.st_mode):
        # Regular file
        return await _process_file_entry(entry, stat_result, upload, params, progress)

    elif stat.S_ISDIR(stat_result.st_mode):
        # Directory
        if any(c.objid is None for c in children):
            raise DependencyError(
                "{} depends on these paths, but they haven't been backed up yet. This is "
                "a bug. {}".format(
                    entry.printable_path,
                    ", ".join(c.printable_path for c in children if c.objid is None),
                )
            )
        dir_obj = await upload(
            ObjectRequest(
                header=ObjectHeader(
                    type=ObjectType.TREE,
                    length=0,
                    stats=ObjectStats.from_stat_result(stat_result),
                    entries=[
                        EntryRef(
                            name=c.name,
                            objid=cast(ObjIDType, c.objid),
                        )
                        for c in children
                    ],
                ),
                body=None,
            )
        )
        return _ProcessingResult(
            obj=dir_obj,
            stat_result=stat_result,
        )

    elif stat.S_ISLNK(stat_result.st_mode):
        # Symlink
        link_target = os.readlink(entry.path)
        symlink_obj = await upload(
            ObjectRequest(
                header=ObjectHeader(
                    type=ObjectType.SYMLINK,
                    length=len(link_target),
                    stats=ObjectStats.from_stat_result(stat_result),
                ),
                body=io.BytesIO(link_target),
            )
        )
        return _ProcessingResult(
            obj=symlink_obj,
            stat_result=stat_result,
        )

    else:
        logger.warning("%s: Unknown file type. Ignoring.", entry.printable_path)
        return None


async def _process_file_entry(
    entry: models.FSEntry,
    stat_result: os.stat_result,
    upload: Callable[[ObjectRequest], Coroutine[None, None, models.Object]],
    params: _TuningParams,
    progress: _EntryProgress,
) -> _ProcessingResult | None:
    """Process a single file entry"""
    progress.bytes_total = stat_result.st_size
    payload: io.BytesIO | None
    blob_objs: list[tuple[int, models.Object]] = []

    async def submit_blob_upload(p: int, c: bytes):
        obj = await upload(
            ObjectRequest(
                header=ObjectHeader(
                    type=ObjectType.BLOB,
                    stats=None,
                    length=len(c),
                ),
                body=io.BytesIO(c),
            )
        )
        progress.bytes_backed_up += len(c)
        blob_objs.append((p, obj))

    try:
        with _open_file(entry.path) as fobj:
            if stat_result.st_size < params.inline_threshold:
                # Upload the file as a payload in this object
                # We read the entire file into memory here because it's not huge, and to
                # make sure the ObjectHeader length field is correct.
                payload = io.BytesIO(fobj.read())
            else:
                # Upload the file as blob objects and reference them from this one
                payload = None
                if stat_result.st_size <= params.chunk_threshold:
                    await submit_blob_upload(0, fobj.read())
                else:
                    chunk_iter = chunker.FixedChunker(fobj, params.chunk_size)
                    async with BoundedTaskGroup() as tg:
                        for pos, chunk in chunk_iter:
                            await tg.create_task(submit_blob_upload(pos, chunk))
                    blob_objs.sort(key=itemgetter(0))

    except FileNotFoundError:
        logger.info("%s: File disappeared", entry.printable_path)
        return None
    except OSError as e:
        logger.warning("%s: Error when reading: %s", entry.printable_path, e)
        return None

    file_obj = await upload(
        ObjectRequest(
            header=ObjectHeader(
                type=ObjectType.FILE,
                length=len(payload.getbuffer()) if payload is not None else 0,
                stats=ObjectStats.from_stat_result(stat_result),
                blobs=(
                    [BlobRef(objid=obj.objid, pos=pos) for pos, obj in blob_objs]
                    if blob_objs
                    else None
                ),
            ),
            body=payload,
        )
    )
    progress.bytes_backed_up = progress.bytes_total
    return _ProcessingResult(
        obj=file_obj,
        stat_result=stat_result,
    )


_has_noatime = True


def _open_file(path):
    """Opens this file for reading

    :returns: An open file object

    """
    flags = os.O_RDONLY

    # Add O_BINARY on windows
    flags |= getattr(os, "O_BINARY", 0)

    global _has_noatime
    if _has_noatime:
        try:
            flags_noatime = flags | os.O_NOATIME
        except AttributeError:
            _has_noatime = False
        else:
            # Add O_NOATIME if available. This may fail with permission denied,
            # so try again without it if failed
            try:
                return os.fdopen(os.open(path, flags_noatime), "rb")
            except PermissionError:
                _has_noatime = False

    return os.fdopen(os.open(path, flags), "rb")
