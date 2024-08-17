import asyncio
import concurrent.futures
import datetime
import io
import os
import signal
import stat
import threading
import time
from asyncio import Future
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
    ):
        self.db = db
        self.put_object = put_object
        self.put_snapshot = put_snapshot

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

        self.backup_count: int = 0
        self.backup_total: int = 0

        self._shutdown: bool = False

    def _backup_items_remain(self) -> bool:
        with self.db.cursor() as cursor:
            cursor.execute("SELECT 1 FROM fsentry WHERE objid IS NULL LIMIT 1")
            return bool(cursor.fetchone())

    def _finalize_entry(self, e: models.FSEntry, result: _ProcessingResult | None):
        self.backup_count += 1
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

    async def backup(self):
        with ExitStack() as exitstack:
            cursor = exitstack.enter_context(self.db.cursor())
            cursor.execute("SELECT COUNT(*) FROM fsentry WHERE objid IS NULL")
            self.backup_total = cursor.fetchone()[0]
            self.backup_count = 0

            logger.info("Starting backup. %s items to backup", self.backup_total)

            executor: ThreadPoolExecutor = exitstack.enter_context(
                ThreadPoolExecutor(thread_name_prefix="backup-thread-")
            )

            try:
                while self._backup_items_remain() and not self._shutdown:
                    await self._backup_single_pass(executor)

                    with self.db.cursor() as cursor:
                        cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
                        cursor.execute("PRAGMA optimize")

            finally:
                executor.shutdown(wait=True, cancel_futures=True)

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
            logger.info("Backup finished. %s entries backed up", self.backup_count)
        else:
            logger.info("Backup cancelled")

    def shutdown(self):
        self._shutdown = True

    async def _backup_single_pass(self, executor: ThreadPoolExecutor):
        """Fetches a batch of fsentry rows which need backing up, and dispatches tasks
        to a thread pool

        This function will usually return early after backing up some, but not all,
        of what needs backing up. The reason is to let the caller commit the transaction,
        saving progress and preventing the write-ahead-log from growing unbounded.

        This function makes the guarantee that all threadpool tasks will have finished
        when it returns, regardless of whether this function returns normally or via
        exception, including an asyncio.CanceledError.

        This guarantee is necessary to let the caller commit the transaction, since it
        knows no uploads will be in progress which still may need to update the database.

        This guarantee also lets the caller close the threadpool without worrying about
        the threads being blocked waiting on some event loop task to complete.
        """
        ct = 0
        time_start = time.monotonic()
        logger.debug(
            "Fetching next batch of entries to backup. Current progress: %s/%s",
            self.backup_count,
            self.backup_total,
        )

        # Threadpool tasks
        tasks: set[Future[tuple[models.FSEntry, _ProcessingResult | None]]] = set()

        # Tasks initiated by the threads calling back into the main event loop to
        # perform an upload. When these are canceled, the event loop must run in order
        # for the threads to get notified of the cancellation. If the event loop doesn't
        # have a chance to run e.g. during exception unwinding, then the threads will
        # hang. So care must be taken to catch ALL exceptions and cancel all tasks, then
        # yield to the event loop waiting for the tasks to finish. Only then can we
        # propagate the exception out of this function.
        upload_tasks: set[Future] = set()

        def sigint_handler():
            logger.info("Ctrl-C caught. Finishing current items and shutting down")
            self.shutdown()

        with ExitStack() as exitstack:
            exitstack.enter_context(self.db.atomic(immediate=True))
            exitstack.enter_context(install_sigint_handler(sigint_handler))
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

                    tasks.add(
                        asyncio.create_task(
                            _dispatch(
                                executor,
                                entry,
                                child_entries,
                                self.put_object,
                                self.params,
                                upload_tasks,
                            )
                        )
                    )

                    if len(tasks) >= 20:
                        done, tasks = await asyncio.wait(
                            tasks,
                            timeout=None,
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for task in done:
                            self._finalize_entry(*(await task))

                    if time.monotonic() - time_start > 30:
                        logger.debug("Breaking to checkpoint")
                        break
            except BaseException:
                entry_iterator.close()
                # Cancel all tasks
                for t in tasks:
                    if not t.done():
                        t.cancel()
                for t in upload_tasks:
                    if not t.done():
                        t.cancel()
                # Wait for all tasks to complete, regardless of whether they error or not
                # The important part is we yield to the event loop. Otherwise cancelled
                # tasks will never notify the threads and the thread workers will
                # never return.
                await asyncio.gather(*tasks, *upload_tasks, return_exceptions=True)
                raise
            else:
                entry_iterator.close()

                if ct == 0 and not tasks:
                    raise RuntimeError("Backup loop found no items")

                # Gather any remaining tasks before returning
                for t in asyncio.as_completed(tasks):
                    self._finalize_entry(*(await t))

                if upload_tasks:
                    raise RuntimeError(
                        "Upload tasks are still running, even though all"
                        " backup tasks returned"
                    )


async def _dispatch(
    executor: ThreadPoolExecutor,
    entry: models.FSEntry,
    child_entries: list[models.FSEntry],
    put_object: Callable[[ObjectRequest], Awaitable[models.Object]],
    params: _TuningParams,
    upload_tasks: set[asyncio.Future],
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
        fut = asyncio.run_coroutine_threadsafe(put_object(req), loop)
        return fut.result()

    logger.log(5, "Dispatching process_entry call for %s", entry)
    fut = loop.run_in_executor(
        executor, _process_entry, entry, child_entries, upload, params
    )
    upload_tasks.add(fut)
    fut.add_done_callback(lambda _: upload_tasks.remove(fut))
    result = await fut
    logger.log(5, "process_entry finished for %s", entry)
    return entry, result


def _process_entry(
    entry: models.FSEntry,
    children: list[models.FSEntry],
    upload: Callable[[ObjectRequest], models.Object],
    params: _TuningParams,
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
    try:
        stat_result = os.lstat(entry.path)
    except (FileNotFoundError, NotADirectoryError):
        logger.info("%s: File disappeared", entry.printable_path)
        return None

    if stat.S_ISREG(stat_result.st_mode):
        # Regular file
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
