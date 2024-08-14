import asyncio
import concurrent.futures
import datetime
import io
import os
import stat
import time
from asyncio import Future
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack
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


async def backup(
    db: Database,
    put_object: Callable[[ObjectRequest], Awaitable[models.Object]],
    put_snapshot: Callable[[models.Snapshot], None],
):
    """Performs a backup of all outstanding files in the backup set"""

    def backup_items_remain() -> bool:
        with db.cursor() as c:
            c.execute("SELECT 1 FROM fsentry WHERE objid IS NULL LIMIT 1")
            return bool(c.fetchone())

    def finalize_entry(e: models.FSEntry, result: _ProcessingResult | None):
        nonlocal backup_count
        backup_count += 1
        with db.atomic():
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
                    db,
                    result.obj.objid,
                    new=False,
                    stat_result=result.stat_result,
                )

    # Get some config items
    inline_threshold: int = max(0, db.config_get("inline-threshold", 2**20))
    chunk_threshold: int = max(
        0, inline_threshold, db.config_get("chunk-threshold", 30 * 2**20)
    )
    chunk_size: int = max(2**16, db.config_get("chunk-size", 10 * 2**20))

    with ExitStack() as exitstack:
        cursor = exitstack.enter_context(db.cursor())
        cursor.execute("SELECT COUNT(*) FROM fsentry WHERE objid IS NULL")
        backup_total = cursor.fetchone()[0]
        backup_count = 0

        logger.info("Starting backup. %s items to backup", backup_total)

        tasks: set[Future[tuple[models.FSEntry, _ProcessingResult | None]]] = set()

        # The backup routine MUST use a separate thread pool from the event loop's default,
        # because the threads will call back in to the main thread to upload objects,
        # and those calls may themselves call back into the default thread pool. That
        # could cause deadlocks if all threads in the pool are busy.
        executor = exitstack.enter_context(
            ThreadPoolExecutor(thread_name_prefix="backup-thread-")
        )

        exitstack.enter_context(db.atomic(immediate=True))
        while backup_items_remain():
            ct = 0
            last_checkpoint = time.monotonic()

            logger.debug(
                "Fetching next batch of entries to backup. Current progress: %s/%s",
                backup_count,
                backup_total,
            )
            entry_iterator = db.query(
                models.FSEntry,
                """SELECT * FROM fsentry WHERE
                objid IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM fsentry AS children WHERE
                    children.parent = fsentry.id
                    AND children.objid IS NULL
                ) """,
            )
            for entry in entry_iterator:
                ct += 1
                if entry.objid is not None:
                    raise RuntimeError(
                        f"Backup query received entry already backed up! {entry}"
                    )

                child_entries = list(
                    db.query(
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
                            put_object,
                            _TuningParams(inline_threshold, chunk_threshold, chunk_size),
                        )
                    )
                )

                if len(tasks) >= 100:
                    done, tasks = await asyncio.wait(
                        tasks,
                        timeout=None,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in done:
                        finalize_entry(*(await task))

                if time.monotonic() - last_checkpoint > 30:
                    # Perform a periodic checkpoint. We have to break out of the inner
                    # loop because the iterator holds a db cursor open.
                    logger.debug("Breaking to checkpoint")
                    break

            entry_iterator.close()
            # Checkpoint now that the object iterator cursor has closed
            cursor.execute("COMMIT")
            cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
            cursor.execute("PRAGMA optimize")
            cursor.execute("BEGIN IMMEDIATE")

            # Make sure we're making progress and at least one item was backed up this
            # iteration, or items are still being processed. Otherwise we may be caught in an
            # infinite loop.
            if ct == 0 and not tasks:
                raise RuntimeError("Backup loop found no items")

            # Collect any remaining tasks from this loop iteration before moving on to the next.
            # We have to make sure all tasks are finished before performing another query for
            # new ready-to-backup entries, because any entries currently being processed would
            # show up in that query again and get backed up a second time.
            # This flushes the queue and stalls the workers briefly, but it doesn't end up costing
            # all that much time compared to the time spent working. If this is turns out to be a
            # problem, then it may be better to find a different periodic-checkpoint strategy or
            # forego the periodic checkpointing altogether.
            logger.debug("Flushing queue")
            for task in asyncio.as_completed(tasks):
                finalize_entry(*(await task))
            tasks.clear()

    # Exiting the outer "while" loop and the context with the executor

    # Add a snapshot object for each root
    logger.debug("Backup finished. Creating snapshot objects")
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    with db.atomic(immediate=True), db.cursor() as cursor:
        for entry in db.query(
            models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL"
        ):
            if entry.objid is None:
                raise RuntimeError(f"Root not backed up {entry}")
            logger.debug(
                "Snapshot: %s: %s (%s)", now, entry.objid.hex(), entry.printable_path
            )
            put_snapshot(
                models.Snapshot.model_construct(
                    path=entry.printable_path, root=entry.objid, timestamp=now
                )
            )
        cursor.execute("PRAGMA optimize")
    with db.cursor() as cursor:
        cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
    logger.info("Backup finished. %s entries backed up", backup_count)


async def _dispatch(
    executor: ThreadPoolExecutor,
    entry: models.FSEntry,
    child_entries: list[models.FSEntry],
    put_object: Callable[[ObjectRequest], Awaitable[models.Object]],
    params: _TuningParams,
) -> tuple[models.FSEntry, _ProcessingResult | None]:
    """Shim to dispatch a sub-thread to process an entry

    Returns a tuple of the FSEntry that was processed and the ProcessingResult from
    process_entry()

    """
    loop = asyncio.get_running_loop()

    # Called from the sub-threads to upload an object, scheduling the upload code to run
    # in this thread
    def upload(req: ObjectRequest) -> models.Object:
        fut = asyncio.run_coroutine_threadsafe(put_object(req), loop)
        return fut.result()

    result: _ProcessingResult | None
    result = await loop.run_in_executor(
        executor, process_entry, entry, child_entries, upload, params
    )
    return entry, result


def process_entry(
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
