import itertools
import time
from contextlib import ExitStack
from logging import getLogger
import os
import stat
import io
import datetime
import concurrent.futures

from django.db.transaction import atomic
from django.db import connections
from django.utils import timezone
import pytz

import umsgpack

from . import models
from . import chunker
from .exceptions import DependencyError

logger = getLogger("backathon.backup")

BATCH_SIZE = 100
NUM_WORKERS = os.cpu_count()

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
        # from this problem that I'm not aware of, so I'm taking caution and
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
                print("Ctrl-C caught. Finishing the current batch of "
                      "uploads, please wait...")
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

                tasks.add(
                    executor.submit(backup_entry, repo, entry_batch)
                )

                # Don't put the entire to_backup result set in the queue at
                # once, to save memory.
                # If there are too many unfinished tasks, wait for one to
                # finish.
                if len(tasks) >= NUM_WORKERS+1 or single:
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
            # items still in process in the thread pool.
            # This stalls the workers but it doesn't end up costing all that
            # much time.
            for f in concurrent.futures.as_completed(tasks):
                backup_count += f.result()
                if progress is not None:
                    progress(backup_count, backup_total)
            tasks.clear()

    # End of outer "while" loop, and end of the contexts ExitStack. The
    # Executor is shut down at this point.

    # Now add the Snapshot object(s) to the database representing this backup
    # run. There's one snapshot per root, but they all have the same datetime
    # so they can still be grouped together in queries.
    now = timezone.now()
    for root in models.FSEntry.objects.using(repo.db).filter(
        parent__isnull=True
    ):
        assert root.obj_id is not None
        with atomic():
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
    """Entry point for each worker thread/process

    Takes a list of entry objects to backup

    :returns: the length of the given list
    """

    # Save this repo object between tasks. This way, all the attributes on
    # the repo instance don't have to be re-created from the database or
    # serialized each time. Also, the storage class can keep its persistent
    # requests session open.
    # Warning: this code assumes the Executor is a ProcessPoolExecutor,
    # not a ThreadPoolExecutor.
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


def backup_iterator(fsentry, inline_threshold=2 ** 21):
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

    Either way, the (saved or fetched) Object is sent back into this
    generator function so it can be used in a subsequent ObjectRelation entry.

    This function is responsible for updating the FSEntry.obj foreign key field
    with the sent object after yielding a payload.

    Yields: (payload, Object, [ObjectRelation list])
    Caller sends: The saved models.Object instance

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
    relations = [] # type: list[models.ObjectRelation]

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
                        relations.append(
                            models.ObjectRelation(child=chunk_obj)
                        )
                    umsgpack.pack(("chunklist", chunk_list), inode_buf)

        except FileNotFoundError:
            logger.info("File disappeared: {}".format(fsentry))
            fsentry.delete()
            return
        except OSError:
            # This happens with permission denied errors
            logger.exception("Error in system call when reading file "
                             "{}".format(fsentry))
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
        logger.info("Backed up file into {} objects: {}".format(
            len(relations)+1,
            fsentry
        ))

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
                    ", ".join(c.printablepath
                              for c in children if c.obj_id is None),
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

        logger.info("Backed up dir: {}".format(
            fsentry
        ))

    else:
        logger.warning("Unknown file type, not backing up {}".format(
            fsentry))
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
            f.set_result(
                fn(*args, **kwargs)
            )
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
