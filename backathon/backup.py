import asyncio
import concurrent.futures
import dataclasses
import datetime
import io
import os
import signal
import stat
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from logging import getLogger
from operator import attrgetter
from typing import (
    IO,
    Awaitable,
    Callable,
    NamedTuple,
    cast,
)

import rich.filesize

from backathon import chunker, models
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
    current_uploads: list[_EntryProgress] = dataclasses.field(default_factory=list)


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

        # Tracks tasks sent to the thread pool to process a single FSEntry
        # Tasks are removed from this set when they have been finalized
        # Maps fsentry paths to the task submitted to process that path
        self._processing_tasks: dict[
            bytes, asyncio.Future[tuple[models.FSEntry, _ProcessingResult | None]]
        ] = dict()

        # Set of upload tasks that the process tasks have submitted to request an
        # upload. Processing tasks will block waiting for these to complete, and
        # since these tasks run in the main thread's event loop, the event loop
        # must run in order for these to return and unblock the threads. Be careful
        # when cleaning up the thread pool to let the event loop run!
        self._upload_tasks: set[asyncio.Future] = set()

        self._shutdown: bool = False

    def _backup_items_remain(self) -> bool:
        with self.db.cursor() as cursor:
            cursor.execute("SELECT 1 FROM fsentry WHERE objid IS NULL LIMIT 1")
            return bool(cursor.fetchone())

    def _finalize_entry(self, e: models.FSEntry, result: _ProcessingResult | None):
        task = self._processing_tasks[e.path]
        assert task.done()

        self.progress.count_progress += 1
        if e.st_mode and e.st_size and stat.S_ISREG(e.st_mode):
            self.progress.size_progress += e.st_size

        with self.db.atomic(), self.db.cursor() as cursor:
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
            del self._processing_tasks[e.path]

        if self.progress_callback is not None:
            self.progress_callback(self.progress)

    async def backup(self):
        with ExitStack() as exitstack:
            cursor = exitstack.enter_context(self.db.cursor())

            def catch_cancel(exc_type, exc_val, tb):
                # Catch canceled errors and ignore, but also set the shutdown flag
                # so the snapshot code doesn't run
                if exc_type and isinstance(exc_val, asyncio.CancelledError):
                    self._shutdown = True
                    return True

            exitstack.push(catch_cancel)

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

            executor: ThreadPoolExecutor = exitstack.enter_context(
                ThreadPoolExecutor(thread_name_prefix="backup-thread")
            )

            def sigint_handler():
                logger.info("Ctrl-C caught. Stopping backup")
                self.shutdown()

            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, sigint_handler)

            try:
                while self._backup_items_remain() and not self._shutdown:
                    with self.db.atomic(immediate=True):
                        await self._backup_batch(executor)

                    logger.debug("Checkpointing database")
                    with self.db.cursor() as cursor:
                        cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
                        cursor.execute("PRAGMA optimize")

                # Loop exited with no error
                logger.debug(
                    "Backup loop exited without exception. Finalizing %s tasks",
                    len(self._processing_tasks),
                )
                logger.debug(
                    "Upload tasks still in progress: %s", len(self._upload_tasks)
                )
                with self.db.atomic(immediate=True):
                    for t in asyncio.as_completed(self._processing_tasks.values()):
                        self._finalize_entry(*(await t))
                logger.debug("All tasks done")

            finally:
                if sys.exc_info()[0]:
                    self._shutdown = True
                    logger.debug("Exiting via exception", exc_info=True)
                logger.debug("Cleaning up backup tasks")
                loop.remove_signal_handler(signal.SIGINT)
                logger.debug(
                    "Remaining processing tasks: %s", len(self._processing_tasks)
                )
                logger.debug("Remaining upload tasks: %s", len(self._upload_tasks))
                executor.shutdown(wait=False, cancel_futures=True)

                # yield to the event loop while we wait for all threads to finish. The
                # event loop must run so that callbacks into the main event loop won't
                # block threads from finishing, which is often the case for incomplete,
                # cancelled backups.
                logger.debug("Waiting for tasks to finish")
                await asyncio.gather(
                    *self._processing_tasks.values(),
                    *self._upload_tasks,
                    return_exceptions=True,
                )
                assert all(t.done for t in self._processing_tasks.values())
                assert all(t.done for t in self._upload_tasks)
                logger.debug("Shutting down thread pool")
                executor.shutdown(wait=True)

        # Add a snapshot object for each root
        if not self._shutdown:
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
            logger.info(
                "Backup finished. %s entries backed up", self.progress.count_progress
            )
        else:
            logger.info("Backup cancelled")

    def shutdown(self):
        self._shutdown = True

    async def _backup_batch(self, executor: ThreadPoolExecutor):
        """Fetches a batch of fsentry rows which need backing up, and dispatches tasks
        to a thread pool

        This function will usually return early after backing up some, but not all,
        of what needs backing up. The reason is to let the caller commit the transaction,
        saving progress and preventing the write-ahead-log from growing unbounded.

        Tasks dispatched by this function may still be running when this function returns.
        Callers are responsible for reaping any remaining tasks if this function exits,
        even via exception.

        A database transaction should be held by the caller for the duration of the
        call to this method.
        """
        ct = 0
        time_start = time.monotonic()
        logger.debug(
            "Fetching next batch of entries to backup. Current progress: %s/%s",
            self.progress.count_progress,
            self.progress.count_total,
        )

        with self.db.atomic(immediate=True):
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
                    if self._shutdown:
                        break
                    ct += 1
                    if entry.objid is not None:
                        raise RuntimeError(
                            f"Backup query received entry already backed up! {entry}"
                        )
                    if entry.path in self._processing_tasks:
                        # This entry has already been submitted by a previous iteration. Skip it.
                        continue
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

                    self._processing_tasks[entry.path] = asyncio.create_task(
                        self._dispatch(
                            executor,
                            entry,
                            child_entries,
                            self.put_object,
                            self.params,
                        )
                    )

                    if len(self._processing_tasks) >= 20:
                        task_set = self._processing_tasks.values()
                        done, _ = await asyncio.wait(
                            task_set,
                            timeout=None,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for task in done:
                            self._finalize_entry(*(await task))

                    if time.monotonic() - time_start > 30:
                        logger.debug("Breaking to checkpoint")
                        logger.debug(
                            "%s tasks and %s upload tasks pending",
                            len(self._processing_tasks),
                            len(self._upload_tasks),
                        )
                        return

                # Exited the for loop with no new entries fetched from the database AND
                # nothing currently being processed? This is an error and could indicate
                # some kind of dependency loop or other bug
                if ct == 0 and not self._processing_tasks:
                    raise RuntimeError("Backup loop found no items")

                # Loop exited normally, meaning there are no other items to back up
                # at the moment. (There may be later once some current tasks finish though)
                # Process at least one before returning, then let the caller loop us back
                # around to try for more entries, wait on more, or exit if everything's
                # done.
                task_set = self._processing_tasks.values()
                done, _ = await asyncio.wait(
                    task_set,
                    timeout=None,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    self._finalize_entry(*(await task))

            finally:
                entry_iterator.close()

    async def _dispatch(
        self,
        executor: ThreadPoolExecutor,
        entry: models.FSEntry,
        child_entries: list[models.FSEntry],
        put_object: Callable[[ObjectRequest], Awaitable[models.Object]],
        params: _TuningParams,
    ) -> tuple[models.FSEntry, _ProcessingResult | None]:
        """This is the launch point of the asyncio Task to process a single FSEntry
        for backup

        This function's job is to set up the upload callback and dispatch to the
        thread pool.

        """
        loop = asyncio.get_running_loop()

        # This function is called from the sub-threads to upload an object. This adds
        # a task to the main async event loop to perform the upload, blocking the
        # thread until the upload is done.
        def upload(req: ObjectRequest) -> models.Object:
            if self._shutdown:
                # Disallow any further uploads. This will bubble up through the
                # _process_entry() function and then out of _dispatch() where
                # it's caught by _backup_single_pass()
                raise asyncio.CancelledError
            fut = asyncio.run_coroutine_threadsafe(put_object(req), loop)
            obj = fut.result()
            if obj.uploaded_size and not obj.from_cache:
                self.progress.actual_uploaded += obj.uploaded_size
            return obj

        progress = _EntryProgress(entry.printable_path)
        self.progress.current_uploads.append(progress)

        logger.log(5, "Dispatching process_entry call for %s", entry)
        fut = loop.run_in_executor(
            executor, _process_entry, entry, child_entries, upload, params, progress
        )
        self._upload_tasks.add(fut)
        try:
            result = await fut
        finally:
            self._upload_tasks.remove(fut)
            self.progress.current_uploads.remove(progress)
        logger.log(5, "process_entry finished for %s", entry)
        return entry, result


def _process_entry(
    entry: models.FSEntry,
    children: list[models.FSEntry],
    upload: Callable[[ObjectRequest], models.Object],
    params: _TuningParams,
    progress: _EntryProgress,
) -> _ProcessingResult | None:
    """Prepares the payloads for an FSEntry and performs the calls to upload them

    This is designed to be run from a separate thread. This function therefore should not
    use any global state, and should only manipulate database / repo state via the provided
    context methods.

    This function may call context.upload() one or more times to upload new objects to the remote
    repository. The context.upload() implementation is responsible for:
    * Uploading the object
    * Adding an entry to the objects table corresponding to the uploaded object
    * Adding any given child relations to the object_relations table
    * Creating the models.Object instance
    * Returning the models.Object from the context.upload() call

    When this function returns an Object, the caller is responsible for updating the FSEntry
    row in the database with the new Object's objid and new stat info.

    When this function returns None, the caller is responsible for deleting the FSEntry row from
    the database.
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
        progress.bytes_total = stat_result.st_size
        payload: io.BytesIO | None
        blob_objs: list[tuple[int, models.Object]] = []

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
                        chunk_iter = [(0, fobj.read())]
                    else:
                        chunk_iter = chunker.FixedChunker(fobj, params.chunk_size)
                    for pos, chunk in chunk_iter:
                        chunk_obj = upload(
                            ObjectRequest(
                                header=ObjectHeader(
                                    type=ObjectType.BLOB,
                                    stats=None,
                                    length=len(chunk),
                                ),
                                body=io.BytesIO(chunk),
                            )
                        )
                        progress.bytes_backed_up += len(chunk)
                        blob_objs.append((pos, chunk_obj))

        except FileNotFoundError:
            logger.info("%s: File disappeared", entry.printable_path)
            return None
        except OSError as e:
            logger.warning("%s: Error when reading: %s", entry.printable_path, e)
            return None

        file_obj = upload(
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
        dir_obj = upload(
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
        symlink_obj = upload(
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


class DummyExecutor(concurrent.futures._base.Executor):
    """A dummy executor that implements the standard Executor interface but
    runs its tasks immediately

    Used as a drop in replacement for an Executor when single threaded
    execution is required. This is useful when running under a debugger.
    """

    def submit(self, fn, *args, **kwargs):
        f = concurrent.futures.Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except BaseException as e:
            f.set_exception(e)
        return f
