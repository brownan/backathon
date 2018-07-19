from logging import getLogger
import os
import stat
import io

from django.db.transaction import atomic
from django.db import connections
from django.utils import timezone

import umsgpack

from . import models
from . import chunker
from .exceptions import DependencyError

logger = getLogger("backathon.backup")

def backup(repo, progress=None):
    """Perform a backup

    This is usually called from Repository.backup() and is tightly integrated
    with the Repository class. It lives in its own module for organizational
    reasons.

    :type repo: backathon.repository.Repository
    :param progress: A callback function that provides status updates on the
        scan
    """
    if models.FSEntry.objects.using(repo.db).filter(new=True).exists():
        # This happens when a new root is added but hasn't been scanned yet.
        raise RuntimeError("You need to run a scan first")

    to_backup = models.FSEntry.objects.using(repo.db).filter(obj__isnull=True)

    # The ready_to_backup set is the set of all nodes whose children have all
    # already been backed up. In other words, these are the entries that we
    # can back up right now.
    ready_to_backup = to_backup.exclude(
        id__in=to_backup.exclude(parent__isnull=True).values("parent_id")
    )

    backup_total = to_backup.count()
    backup_count = 0

    while to_backup.exists():

        ct = 0
        for entry in ready_to_backup.iterator(): # type: models.FSEntry
            ct += 1

            # This sanity check is just making sure that our query works
            # correctly by only selecting entries that haven't been backed up
            # yet. Because we're modifying entries and iterating over a
            # result set at the same time, SQLite may return a row twice,
            # but since the modified rows don't match our query,
            # they shouldn't re-appear in this same query. However,
            # the SQLite documentation on isolation isn't clear on this. If I
            # see this assert statement getting hit in practice, then the
            # thing to do is to ignore the entry and move on.
            assert entry.obj_id is None

            iterator = _backup_iterator(
                entry,
                inline_threshold=repo.backup_inline_threshold,
            )

            try:
                obj_buf, obj_children = next(iterator)
                while True:
                    obj_buf, obj_children = iterator.send(
                        repo.push_object(obj_buf, obj_children)
                    )
            except StopIteration:
                pass

            # Sanity check: If a bug in the entry.backup() method doesn't set
            # one of these, the entry will be selected next iteration,
            # causing an infinite loop
            assert entry.obj_id is not None or entry.id is None

            backup_count += 1
            if progress is not None:
                progress(backup_count, backup_total)

        # Sanity check: if we entered the outer loop but the inner loop's
        # query didn't select anything, then we're not making progress and
        # may be caught in an infinite loop. In particular, this could happen
        # if we somehow got a cycle in the FSEntry objects in the database.
        # There would be entries needing backing up, but none of them have
        # all their dependent children backed up.
        assert ct > 0

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

def _backup_iterator(fsentry, inline_threshold=2 ** 21):
    """Back up an FSEntry object

    Reads this entry in from the file system, creates one or more object
    payloads, and yields them to the caller for uploading to the backing
    store. The caller is expected to send the Object instance
    back into this iterator function.

    Yields: (payload_buffer, list_of_child_Object_instances)
    Caller sends: models.Object instance of the last yielded payload

    The payload_buffer is a file-like object ready for reading.
    Usually a BytesIO instance.

    Note: this sequence of operations was chosen over having this
    method upload the objects itself so that the caller may choose to
    buffer and upload objects in batch. It's also more flexible in
    several ways. E.g. while a recursive algorithm would
    have to upload items in a post-order traversal of the tree, here
    the caller is free to do a SQL query to get items ordered by any
    criteria. Like, say, all small files first and pack them together into
    a single upload.

    For directories: yields a single payload for the directory entry.
    Raises a DependencyError if one or more children do not have an
    obj already. It's the caller's responsibility to call backup() on
    entries in an order to avoid dependency issues.

    For files: yields one or more payloads for the file's contents,
    then finally a payload for the inode entry.

    IMPORTANT: every exit point from this function must either update
    this entry's obj to a non-null value, OR delete the entry before
    returning.
    """
    try:
        stat_result = os.lstat(fsentry.path)
    except (FileNotFoundError, NotADirectoryError):
        logger.info("File disappeared: {}".format(fsentry))
        fsentry.delete()
        return

    fsentry.update_stat_info(stat_result)

    if stat.S_ISREG(fsentry.st_mode):
        # File

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

        childobjs = []
        chunks = []
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
                    for pos, chunk in chunker.FixedChunker(fobj):
                        buf = io.BytesIO()
                        umsgpack.pack("blob", buf)
                        umsgpack.pack(chunk, buf)
                        buf.seek(0)
                        chunk_obj = yield (buf, [])
                        childobjs.append(chunk_obj)
                        chunks.append((pos, chunk_obj.objid))

                    umsgpack.pack(("chunklist", chunks), inode_buf)

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

        fsentry.obj = yield (inode_buf, childobjs)
        logger.info("Backed up file into {} objects: {}".format(
            len(chunks)+1,
            fsentry
        ))

    elif stat.S_ISDIR(fsentry.st_mode):
        # Directory
        # Note: backing up a directory doesn't involve reading
        # from the filesystem aside from the lstat() call from above. All
        # the information we need is already in the database.
        children = list(fsentry.children.all().select_related("obj"))

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
        if any(c.obj is None for c in children):
            raise DependencyError(
                "{} depends on these paths, but they haven't been "
                "backed up yet. This is a bug. {}"
                "".format(
                    fsentry.printablepath,
                    ", ".join(c.printablepath
                              for c in children if c.obj is None),
                )
            )

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
            [(os.fsencode(c.name), c.obj.objid) for c in children],
            buf,
        )
        buf.seek(0)

        fsentry.obj = yield (buf, (c.obj for c in children))

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
    """Opens this file for reading"""
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
