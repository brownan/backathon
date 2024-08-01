import concurrent.futures
import datetime
import io
import itertools
import os
import stat
import time
from contextlib import ExitStack
from logging import getLogger
from tempfile import SpooledTemporaryFile
from typing import (
    IO,
    Callable,
    Collection,
    Literal,
    NamedTuple,
    cast,
)

import msgpack

from backathon import chunker, models
from backathon.db import Database
from backathon.exceptions import DependencyError

logger = getLogger("backathon.backup")

NUM_WORKERS = os.cpu_count() or 2


class ObjectRequest(NamedTuple):
    type: Literal["inode", "blob", "tree", "symlink"]
    payload: IO[bytes]
    file_size: int | None = None
    last_modified_time: datetime.datetime | None = None
    children: Collection[tuple[bytes, str | None]] = ()


class ProcessingContext(NamedTuple):
    upload: Callable[[ObjectRequest], models.Object]
    inline_threshold: int = 2**20


class ProcessingResult(NamedTuple):
    obj: models.Object
    stat_result: os.stat_result


def backup(db: Database, context: ProcessingContext):
    """Performs a backup of all outstanding files in the backup set"""

    def backup_items_remain() -> bool:
        with db.cursor() as c:
            c.execute("SELECT 1 FROM fsentry WHERE objid IS NULL LIMIT 1")
            return bool(c.fetchone())

    def finalize_entry(t: concurrent.futures.Future):
        nonlocal backup_count
        backup_count += 1
        entry: models.FSEntry = entries_by_task.pop(t)
        result: ProcessingResult = t.result()
        with db.atomic():
            if result is None:
                # Item was not backed up. We need to delete its entry
                cursor.execute("DELETE FROM fsentry WHERE id=?", (entry.id,))
            else:
                # Update the fsentry
                if result.obj.objid is None:
                    raise RuntimeError(
                        f"process_entry() returned an object with no id: {result.obj}"
                    )
                entry.update(
                    db,
                    result.obj.objid,
                    new=False,
                    stat_result=result.stat_result,
                )

    with ExitStack() as exitstack:
        cursor = exitstack.enter_context(db.cursor())
        cursor.execute("SELECT COUNT(*) FROM fsentry WHERE objid IS NULL")
        backup_total = cursor.fetchone()[0]
        backup_count = 0

        tasks: set[concurrent.futures.Future] = set()
        entries_by_task: dict[concurrent.futures.Future, models.FSEntry] = {}

        executor = exitstack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS)
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
            obj_iterator = db.get_objects(
                models.Object,
                """SELECT * FROM fsentry WHERE
                objid IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM fsentry AS children WHERE
                    children.parent = fsentry.id
                    AND children.objid IS NULL
                ) """,
            )
            for entry in obj_iterator:
                ct += 1
                if entry.objid is not None:
                    raise RuntimeError(
                        f"Backup query received entry already backed up! {entry}"
                    )

                child_entries = list(
                    db.get_objects(
                        models.FSEntry,
                        "SELECT * FROM fsentry WHERE parent=?",
                        (entry.id,),
                    )
                )

                task = executor.submit(process_entry, entry, child_entries, context)
                entries_by_task[task] = entry
                tasks.add(task)

                if len(tasks) >= 100:
                    done, tasks = concurrent.futures.wait(
                        tasks,
                        timeout=None,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )

                    for task in done:
                        finalize_entry(task)

                if time.monotonic() - last_checkpoint > 30:
                    # Perform a periodic checkpoint. We have to break out of the inner
                    # loop because the iterator holds a db cursor open.
                    logger.debug("Breaking to checkpoint")
                    break

            obj_iterator.close()
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
            for task in concurrent.futures.as_completed(tasks):
                finalize_entry(task)
            tasks.clear()

    # Exiting the outer "while" loop and the context with the executor

    # Add a snapshot object for each root
    logger.debug("Backup finished. Creating snapshot objects")
    now = datetime.datetime.now(tz=datetime.UTC)
    with db.atomic(immediate=True), db.cursor() as cursor:
        for entry in db.get_objects(
            models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL"
        ):
            if entry.objid is None:
                raise RuntimeError(f"Root not backed up {entry}")
            cursor.execute(
                "INSERT INTO snapshots (path, root, date) VALUES (?,?,?)",
                (entry.path, entry.objid, now),
            )
        cursor.execute("PRAGMA optimize")
        cursor.execute("PRAGMA wal_checkpoint=PASSIVE")


def process_entry(
    entry: models.FSEntry, children: list[models.FSEntry], context: ProcessingContext
) -> ProcessingResult | None:
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

    if entry.st_mode != stat_result.st_mode:
        logger.warning(
            "%s: File has changed mode since scan. Ignoring.", entry.printable_path
        )
        return

    packer = msgpack.Packer()
    if stat.S_ISREG(stat_result.st_mode):
        # Regular file
        payload = SpooledTemporaryFile()
        child_chunks: list[models.Object] = []
        payload.write(packer.pack("inode"))
        payload.write(
            packer.pack(
                dict(
                    size=stat_result.st_size,
                    inode=stat_result.st_ino,
                    uid=stat_result.st_uid,
                    gid=stat_result.st_gid,
                    mode=stat_result.st_mode,
                    mtime=stat_result.st_mtime_ns,
                    atime=stat_result.st_atime_ns,
                )
            )
        )

        try:
            with _open_file(entry.path) as fobj:
                if stat_result.st_size < context.inline_threshold:
                    payload.write(packer.pack(("immediate", fobj.read())))
                else:
                    chunk_list: list[tuple[int, bytes]] = []
                    if stat_result.st_size <= 30 * 2**20:
                        chunk_iter = [(0, fobj.read())]
                    else:
                        chunk_iter = chunker.FixedChunker(fobj)
                    for pos, chunk in chunk_iter:
                        buf = SpooledTemporaryFile()
                        buf.write(packer.pack("blob"))
                        buf.write(packer.pack(chunk))
                        buf.seek(0)
                        chunk_obj = context.upload(
                            ObjectRequest(
                                type="blob",
                                payload=buf,
                            )
                        )
                        child_chunks.append(chunk_obj)
                        chunk_list.append((pos, chunk_obj.objid))
                    payload.write(packer.pack(("chunklist", chunk_list)))

        except FileNotFoundError:
            logger.info("%s: File disappeared", entry.printable_path)
            return None
        except OSError as e:
            logger.error("%s: Error when reading: %s", entry.printable_path, e)
            return None

        payload.seek(0)
        file_obj = context.upload(
            ObjectRequest(
                type="inode",
                file_size=stat_result.st_size,
                last_modified_time=datetime.datetime.fromtimestamp(
                    stat_result.st_mtime, datetime.UTC
                ),
                payload=payload,
                children=[(c.objid, None) for c in child_chunks],
            )
        )
        return ProcessingResult(
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
        buf = io.BytesIO()
        buf.write(packer.pack("tree"))
        buf.write(
            packer.pack(
                dict(
                    uid=stat_result.st_uid,
                    gid=stat_result.st_gid,
                    mode=stat_result.st_mode,
                    mtime=stat_result.st_mtime_ns,
                    atime=stat_result.st_atime_ns,
                )
            )
        )
        buf.write(packer.pack([((e.path, e.objid) for e in children)]))
        buf.seek(0)
        dir_obj = context.upload(
            ObjectRequest(
                type="tree",
                last_modified_time=datetime.datetime.fromtimestamp(
                    stat_result.st_mtime, tz=datetime.UTC
                ),
                payload=buf,
                children=[
                    (cast(bytes, c.objid), os.path.basename(c.printable_path))
                    for c in children
                ],
            )
        )
        return ProcessingResult(
            obj=dir_obj,
            stat_result=stat_result,
        )

    elif stat.S_ISLNK(stat_result.st_mode):
        # Symlink
        buf = io.BytesIO()
        buf.write(packer.pack("symlink"))
        buf.write(
            packer.pack(
                dict(
                    uid=stat_result.st_uid,
                    gid=stat_result.st_gid,
                    mode=stat_result.st_mode,
                    mtime=stat_result.st_mtime_ns,
                    atime=stat_result.st_atime_ns,
                )
            )
        )
        buf.write(packer.pack(os.readlink(entry.path)))
        buf.seek(0)
        symlink_obj = context.upload(
            ObjectRequest(
                type="symlink",
                last_modified_time=datetime.datetime.fromtimestamp(
                    stat_result.st_mtime, tz=datetime.UTC
                ),
                payload=buf,
            )
        )
        return ProcessingResult(
            obj=symlink_obj,
            stat_result=stat_result,
        )

    else:
        logger.warning("%s: Unknown file type. Ignoring.", entry.printable_path)
        return None


def _open_file(path):
    """Opens this file for reading

    :returns: An open file object

    """
    flags = os.O_RDONLY

    # Add O_BINARY on windows
    flags |= getattr(os, "O_BINARY", 0)

    try:
        flags_noatime = flags | os.O_NOATIME
    except AttributeError:
        return os.fdopen(os.open(path, flags), "rb")

    # Add O_NOATIME if available. This may fail with permission denied,
    # so try again without it if failed
    try:
        return os.fdopen(os.open(path, flags_noatime), "rb")
    except PermissionError:
        pass
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


def batcher(iterator, batchsize):
    """Yields tuples of items from the given iterator until the iterator is
    exhausted

    Yielded tuples are at most batchsize in length
    """
    it = iter(iterator)
    while True:
        batch = tuple(itertools.islice(it, batchsize))
        if not batch:
            return
        yield batch
