import concurrent.futures
import datetime
import io
import itertools
import os
import stat
import time
from collections.abc import ByteString
from contextlib import ExitStack
from logging import getLogger
from tempfile import SpooledTemporaryFile
from typing import (
    IO,
    Callable,
    Collection,
    Iterator,
    Literal,
    NamedTuple,
    cast,
)

import msgpack

import backathon.repository
from backathon import chunker, models
from backathon.exceptions import DependencyError

logger = getLogger("backathon.backup")

BATCH_SIZE = 100
NUM_WORKERS = os.cpu_count()


class Chunk(NamedTuple):
    fsentry: models.FSEntry
    chunk: ByteString


def get_chunks(repo: backathon.repository.Backathon) -> Iterator[Chunk]:
    """Yields chunks that need to be backed up"""
    db = repo.db
    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM fsentry WHERE obj IS NULL")
        backup_total: int = cursor.fetchone()[0]
        backup_count: int = 0

        while True:
            cursor.execute("SELECT 1 FROM fsentry WHERE obj IS NULL LIMIT 1")
            if not cursor.fetchone():
                break
            ct = 0  # How many items were backed up this iteration
            last_checkpoint = time.monotonic()

            # Iterate over all entries that have no dependencies that aren't yet
            # backed up. In other words, these are entries we can back up right now
            # without waiting on another entry. The entries that have to wait are generally
            # directories which don't yet have their files uploaded, so we can't yet build
            # the directory listing hashes.
            for entry in db.get_objects(
                models.FSEntry,
                """
                SELECT * FROM fsentry
                WHERE NOT EXISTS (
                    SELECT 1 FROM fsentry AS children
                    WHERE children.parent = fsentry.id
                    AND children.obj IS NULL
                )
            """,
            ):
                pass


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


def backup(repo, progress=None, single=False):
    """Perform a backup

    This is usually called from Repository.backup() and is tightly integrated
    with the Repository class. It lives in its own module for organizational
    reasons.

    :type repo: backathon.repository.Repository
    :param progress: A callback function that provides status updates on the
        scan
    :param single: If this parameter is true, the backup process will all
        happen in a single thread. This can help with debugging and profiling.

    The progress callable takes two parameters: the backup count and backup
    total.
    """
    if models.FSEntry.objects.using(repo.db).filter(new=True).exists():
        # This happens when a new root is added but hasn't been scanned yet.
        raise RuntimeError("You need to run a scan first")

    to_backup = models.FSEntry.objects.using(repo.db).filter(obj__isnull=True)

    # The ready_to_backup set is the set of all nodes whose children have all
    # already been backed up. In other words, these are the entries that we
    # can back up right now.
    ready_to_backup = to_backup.exclude(
        # The sub query selects the *parents* of entries that are not yet
        # backed up. Therefore, we're excluding entries whose children are
        # not yet backed up.
        id__in=to_backup.exclude(parent__isnull=True).values("parent_id")
    )

    # The two above querysets remain unevaluated. We therefore get new results
    # on each call to .exists() below. Calls to .iterator() always return new
    # results.

    backup_total = to_backup.count()
    backup_count = 0

    if single:
        executor = DummyExecutor()
    else:
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=NUM_WORKERS,
        )
        # SQLite connections should not be forked, according to the SQLite
        # documentation. Django and/or Python may have some protections
        # from this problem, but I'm not aware of any, so I'm taking caution and
        # closing all connections before forcing the process pool to immediately
        # launch the processes by submitting a dummy task.
        connections.close_all()
        executor.submit(time.time).result()

    tasks = set()

    contexts = ExitStack()
    with contexts:
        contexts.enter_context(executor)

        # Cancel all tasks that haven't been started yet
        def on_exit():
            for t in tasks:
                t.cancel()

        contexts.callback(on_exit)

        def catch_sigint(exc_type, exc_value, traceback):
            if exc_type and issubclass(exc_type, KeyboardInterrupt):
                print()
                print(
                    "Ctrl-C caught. Finishing the current batch of "
                    "uploads, please wait..."
                )

        contexts.push(catch_sigint)

        while to_backup.exists():
            ct = 0
            last_checkpoint = time.monotonic()

            iterator = ready_to_backup.iterator()
            for entry_batch in batcher(iterator, BATCH_SIZE):
                ct += 1

                # Assert our query is working correctly and that there are no
                # SQLite isolation problems (entries we've already backed up
                # re-appearing later in the same query)
                assert all(entry.obj_id is None for entry in entry_batch)

                tasks.add(executor.submit(backup_entry, repo, entry_batch))

                # Don't put the entire to_backup result set in the queue at
                # once, to save memory.
                # If there are too many unfinished tasks, wait for one to
                # finish.
                if len(tasks) >= NUM_WORKERS + 1 or single:
                    done, tasks = concurrent.futures.wait(
                        tasks,
                        timeout=None,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )

                    for f in done:
                        backup_count += f.result()
                        if progress is not None:
                            progress(backup_count, backup_total)

                # SQLite won't auto-checkpoint the write-ahead log while we
                # have the query iterator still open. So we force the inner
                # loop to exit every once in a while and force a WAL
                # checkpoint to keep the WAL from growing unbounded.
                if time.monotonic() - last_checkpoint > 30:
                    # Note: closing the iterator should close the cursor
                    # within it, but I think this is relying on reference
                    # counted garbage collection.
                    # If we run into problems, we'll have to find a different
                    # strategy to run checkpoints
                    iterator.close()
                    with connections[repo.db].cursor() as cursor:
                        cursor.execute("PRAGMA wal_checkpoint=RESTART")

            # Sanity check: if we entered the outer loop but the inner loop's
            # query didn't select anything, then we're not making progress and
            # may be caught in an infinite loop. In particular, this could happen
            # if we somehow got a cycle in the FSEntry tree in the database.
            # There would be entries needing backing up, but none of them have
            # all their dependent children backed up.
            assert ct > 0

            # Collect results for the rest of the tasks. We have to do this
            # at the end of each inner loop to guarantee a correct ordering
            # to backed up entries. Items selected next loop could depend on
            # items still in process in the pool.
            # This stalls the workers but it doesn't end up costing all that
            # much time compared to time spent working.
            for f in concurrent.futures.as_completed(tasks):
                backup_count += f.result()
                if progress is not None:
                    progress(backup_count, backup_total)
            tasks.clear()

    # End of outer "while" loop, and end of the contexts ExitStack. The
    # Executor is shut down at this point.

    # Now add the Snapshot object(s) to the database representing this backup
    # run. There's one snapshot per root, but we give them all the same datetime
    # so they can still be grouped together in queries.
    now = timezone.now()
    for root in models.FSEntry.objects.using(repo.db).filter(parent__isnull=True):
        assert root.obj_id is not None
        with atomic_immediate(using=repo.db):
            ss = models.Snapshot.objects.using(repo.db).create(
                path=root.path,
                root_id=root.obj_id,
                date=now,
            )
            repo.put_snapshot(ss)

    with connections[repo.db].cursor() as cursor:
        cursor.execute("ANALYZE")


_worker_repo = None


def backup_entry(repo, entry_batch):
    """Entry point for each worker process

    Takes a list of entry objects to backup

    :returns: the length of the given list
    """

    # Save this repo object between tasks. This way, all the calculated
    # properties on the repo instance don't have to be re-created from the
    # database or serialized each time. Also, the storage class can keep its
    # persistent http session open. Warning: this code only supports
    # ProcessPoolExecutors, and won't work with a ThreadPoolExecutor.
    global _worker_repo
    if _worker_repo is None or _worker_repo.db != repo.db:
        _worker_repo = repo
    else:
        repo = _worker_repo

    for entry in entry_batch:
        iterator = backup_iterator(
            entry,
            inline_threshold=repo.backup_inline_threshold,
        )

        try:
            yielded = next(iterator)
            while True:
                obj = repo.push_object(*yielded)
                yielded = iterator.send(obj)
        except StopIteration:
            pass

        # Sanity check: If a bug in the backup generator function doesn't
        # set one of these, the entry will be selected next iteration,
        # causing an infinite loop
        assert entry.obj_id is not None or entry.id is None

    return len(entry_batch)


def backup_iterator(fsentry, inline_threshold=2**21):
    """Back up an FSEntry object

    :type fsentry: models.FSEntry
    :param inline_threshold: Threshold in bytes below which file contents are
        inlined into the inode payload.

    This is a generator function. Its job is to take the given models.FSEntry
    object and create the models.Object object for the local cache database
    and corresponding payload to upload to the remote repository. Since some
    types of filesystem entries may be split across multiple objects (e.g.
    large files), this function may yield more than one Object and payload
    for a single FSEntry.

    This function's created Object and ObjectRelation instances are not saved to
    the database, as this function is not responsible for determining the
    object ids. Once yielded, the caller will generate the object id from the
    payload, and will do one of two things:

    1. If the objid does not yet exist in the Object table: Update the Object
    and ObjectRelation instances with the generated object id and save them
    to the database, atomically with uploading the payload to the repository.
    2. If the objid *does* exist in the Object table: do nothing

    Either way, the (saved or fetched) Object with a set objid is sent back
    into this generator function so it can be used in a subsequent
    ObjectRelation entry.

    This function is responsible for updating the FSEntry.obj foreign key field
    with the sent object after yielding a payload.

    Yields: (payload, Object, [ObjectRelation list])
    Caller sends: A models.Object instance with obj.objid set

    The payload is a file-like object ready for reading. Usually a BytesIO
    instance.

    For directories: yields a single payload for the directory entry.
    Raises a DependencyError if one or more children do not have an
    obj already. It's the caller's responsibility to call this function on
    entries in an order to avoid dependency issues.

    For files: yields one or more payloads for the file's contents,
    then finally a payload for the inode entry.

    IMPORTANT: every exit point from this function must either update
    this entry's obj field to a non-null value, OR delete the entry before
    returning. It is an error to leave an entry in the database with the
    obj field still null.
    """
    try:
        stat_result = os.lstat(fsentry.path)
    except (FileNotFoundError, NotADirectoryError):
        logger.info("File disappeared: {}".format(fsentry))
        fsentry.delete()
        return

    fsentry.update_stat_info(stat_result)

    obj = models.Object()
    relations = []  # type: list[models.ObjectRelation]

    if stat.S_ISREG(fsentry.st_mode):
        # Regular File

        # Fill in the Object
        obj.type = "inode"
        obj.file_size = stat_result.st_size
        obj.last_modified_time = datetime.datetime.fromtimestamp(
            stat_result.st_mtime,
            tz=pytz.UTC,
        )

        # Construct the payload
        inode_buf = io.BytesIO()
        umsgpack.pack("inode", inode_buf)
        info = dict(
            size=stat_result.st_size,
            inode=stat_result.st_ino,
            uid=stat_result.st_uid,
            gid=stat_result.st_gid,
            mode=stat_result.st_mode,
            mtime=stat_result.st_mtime_ns,
            atime=stat_result.st_atime_ns,
        )
        umsgpack.pack(info, inode_buf)

        try:
            with _open_file(fsentry.path) as fobj:
                if stat_result.st_size < inline_threshold:
                    # If the file size is below this threshold, put the contents
                    # as a blob right in the inode object. Don't bother with
                    # separate blob objects
                    umsgpack.pack(("immediate", fobj.read()), inode_buf)

                else:
                    # Break the file's contents into chunks and upload
                    # each chunk individually
                    chunk_list = []
                    for pos, chunk in chunker.FixedChunker(fobj):
                        buf = io.BytesIO()
                        umsgpack.pack("blob", buf)
                        umsgpack.pack(chunk, buf)
                        buf.seek(0)
                        chunk_obj = yield (buf, models.Object(type="blob"), [])
                        chunk_list.append((pos, chunk_obj.objid))
                        relations.append(models.ObjectRelation(child=chunk_obj))
                    umsgpack.pack(("chunklist", chunk_list), inode_buf)

        except FileNotFoundError:
            logger.info("File disappeared: {}".format(fsentry))
            fsentry.delete()
            return
        except OSError:
            # This happens with permission denied errors
            logger.error("Error in system call when reading file " "{}".format(fsentry))
            # In order to not crash the entire backup, we must delete
            # this entry so that the parent directory can still be backed
            # up. This code path may leave one or more objects saved to
            # the remote storage, but there's not much we can do about
            # that here. (Basically, since every exit from this method
            # must either acquire and save an obj or delete itself,
            # we have no choice)
            fsentry.delete()
            return

        inode_buf.seek(0)

        # Pass the object and payload to the caller for uploading
        fsentry.obj = yield (inode_buf, obj, relations)
        logger.info(
            "Backed up file into {} objects: {}".format(len(relations) + 1, fsentry)
        )

    elif stat.S_ISDIR(fsentry.st_mode):
        # Directory
        # Note: backing up a directory doesn't involve reading
        # from the filesystem aside from the lstat() call from above. All
        # the information we need is already in the database.
        children = list(fsentry.children.all())

        # This block asserts all children have been backed up before
        # entering this method. If they haven't, then the caller is in
        # error. The current backup strategy involves the caller
        # traversing nodes to back them up in an order that avoids
        # dependency issues.
        # A simplified backup strategy would be to make this method
        # recursive (using `yield from`) and then just call backup on the
        # root nodes. There's no reason I can think of that that wouldn't
        # work. Enforcing this here is just a sanity check for the current
        # backup strategy.
        if any(c.obj_id is None for c in children):
            raise DependencyError(
                "{} depends on these paths, but they haven't been "
                "backed up yet. This is a bug. {}"
                "".format(
                    fsentry.printablepath,
                    ", ".join(c.printablepath for c in children if c.obj_id is None),
                )
            )

        obj.type = "tree"
        obj.last_modified_time = datetime.datetime.fromtimestamp(
            stat_result.st_mtime,
            tz=pytz.UTC,
        )
        relations = [
            models.ObjectRelation(
                child_id=c.obj_id,
                # Names are stored in the object relation model for
                # purposes of searching and directory listing. It's stored in
                # a utf-8 encoding with invalid bytes removed to make
                # searching and indexing possible, but the payload has the
                # original filename in it.
                name=os.fsencode(c.name).decode("utf-8", errors="ignore"),
            )
            for c in children
        ]

        buf = io.BytesIO()
        umsgpack.pack("tree", buf)
        info = dict(
            uid=stat_result.st_uid,
            gid=stat_result.st_gid,
            mode=stat_result.st_mode,
            mtime=stat_result.st_mtime_ns,
            atime=stat_result.st_atime_ns,
        )
        umsgpack.pack(info, buf)
        umsgpack.pack(
            # We have to store the original binary representation of
            # the filename or msgpack will error at filenames with
            # bad encodings
            [(os.fsencode(c.name), c.obj_id) for c in children],
            buf,
        )
        buf.seek(0)

        fsentry.obj = yield (buf, obj, relations)

        logger.info("Backed up dir: {}".format(fsentry))

    elif stat.S_ISLNK(fsentry.st_mode):
        buf = io.BytesIO()
        umsgpack.pack("symlink", buf)
        info = dict(
            uid=stat_result.st_uid,
            gid=stat_result.st_gid,
            mode=stat_result.st_mode,
            mtime=stat_result.st_mtime_ns,
            atime=stat_result.st_atime_ns,
        )
        umsgpack.pack(info, buf)

        # Symlinks may be invalid utf-8 sequences. Make sure we re-encode
        # them to their original byte representation before saving
        umsgpack.pack(os.fsencode(os.readlink(fsentry.path)), buf)

        buf.seek(0)
        obj = models.Object()
        obj.type = "symlink"
        obj.last_modified_time = datetime.datetime.fromtimestamp(
            stat_result.st_mtime,
            tz=pytz.UTC,
        )
        fsentry.obj = yield (buf, obj, [])

    else:
        logger.warning("Unknown file type, not backing up {}".format(fsentry))
        fsentry.delete()
        return

    fsentry.save()
    return


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
