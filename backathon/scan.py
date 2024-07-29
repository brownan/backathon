import logging
import os
import sqlite3
import stat
import time
from typing import Callable, NamedTuple

from backathon import models
from backathon.db import Database

logger = logging.getLogger("backathon.scan")


class ScanProgressCallbacks(NamedTuple):
    pass


def scan(
    db: Database,
    progress: None | Callable[[int, int | None, str], None] = None,
    skip_existing: bool = False,
    force_scan: bool = False,
):
    """Scans all FSEntry objects for changes

    The scan works in multiple passes. The first pass calls FSEntry.scan() on
    each existing FSEntry object in the database. During the scan, new FSEntries
    are added to the database for new directory entries found. Subsequent
    passes select new FSEntries from the database. This continues until no
    more new entries are found in the database. In effect, this is a breadth
    first search of the filesystem tree. From experimentation, this ends up
    being very quick since the database IO is relatively low; entries can be
    fetched in batch.

    :param progress: A callback function that provides status updates on the
        scan.
    :param skip_existing: Only scan new entries. This is used after adding a
        new root to just scan newly added files and directories.
    :param force_scan: Force a re-scan of all tracked files and directories, not
        just ones that appear to have changed.

    The progress callback function should have this signature:
    def progress(count, total, last_file_scanned):
        ...

    Where count is the number of entries processed so far, and total is the
    number of existing entries to scan. Total becomes None when we start to
    scan new entries.

    """

    scanned = 0

    if not skip_existing:
        # First pass, scan all existing non-new entries
        logger.info("Scanning known files for changes")
        with db.cursor() as cursor:
            total: int = cursor.execute(
                "SELECT COUNT(*) FROM fsentry WHERE NOT new"
            ).fetchone()[0]
        with db.atomic(immediate=True):
            for entry in db.get_objects(
                models.FSEntry, "SELECT * FROM fsentry WHERE NOT new"
            ):
                scan_entry(db, entry, force_scan=force_scan)
                if progress is not None:
                    scanned += 1
                    progress(scanned, total, entry.printable_path)

    # Now keep scanning for new objects until there are no more new objects
    logger.info("Scanning newly found files")
    with db.atomic(immediate=True):
        last_checkpoint = time.monotonic()
        while True:
            with db.cursor() as cursor:
                cursor.execute("SELECT 1 FROM fsentry WHERE new LIMIT 1")
                if not cursor.fetchone():
                    break

            obj_iterator = db.get_objects(
                models.FSEntry, "SELECT * FROM fsentry WHERE new"
            )
            for entry in obj_iterator:
                scan_entry(db, entry)
                if progress is not None:
                    scanned += 1
                    progress(scanned, None, entry.printable_path)

                # Detect infinite loops. Make sure entries are always marked as
                # not new
                if entry.new:
                    raise RuntimeError("An entry was not properly scanned. This is a bug")

                if time.monotonic() - last_checkpoint > 30:
                    logger.debug("CHECKPOINTING")
                    last_checkpoint = time.monotonic()
                    break

            obj_iterator.close()
            with db.cursor() as cursor:
                logger.debug("Checkpointing")
                cursor.execute("COMMIT")
                cursor.execute("PRAGMA wal_checkpoint=PASSIVE")
                cursor.execute("PRAGMA optimize")
                cursor.execute("BEGIN IMMEDIATE")
        with db.cursor() as cursor:
            # For any entries invalidated by the scan (had their obj field set to null),
            # also invalidate all parent entries recursively up to the root. This can be
            # done with a single recursive query.
            logger.info("All files scanned. Collecting files needing updating")
            cursor.execute(
                """
                WITH RECURSIVE ancestors(id) AS (
                    SELECT id FROM fsentry WHERE obj IS NULL
                    UNION ALL
                    SELECT fsentry.parent FROM fsentry
                        INNER JOIN ancestors ON (fsentry.id = ancestors.id)
                        INNER JOIN fsentry AS p ON (fsentry.parent = p.id)
                        WHERE fsentry.parent IS NOT NULL AND p.obj IS NOT NULL
                )
                UPDATE fsentry SET obj=NULL
                WHERE fsentry.id IN ancestors
            """
            )
            if logger.isEnabledFor(logging.INFO):
                cursor.execute(
                    "SELECT COUNT(*) FROM fsentry WHERE obj IS NULL AND st_mode & ?",
                    (stat.S_IFREG,),
                )
                n_files = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM fsentry WHERE obj IS NULL")
                n_entries = cursor.fetchone()[0]
                logger.info(
                    "{:,d} file{} and {:,d} {} need updating".format(
                        n_files,
                        "s" if n_files != 1 else "",
                        n_entries,
                        "total entries" if n_entries != 1 else "entry",
                    )
                )
            cursor.execute("ANALYZE fsentry")


def scan_entry(db: Database, entry: models.FSEntry, *, force_scan: bool = False):
    logger.debug("Scanning %s", entry.printable_path)
    times = [time.monotonic_ns()]
    with db.atomic(immediate=True):
        try:
            stat_result = os.lstat(entry.path)
        except (FileNotFoundError, NotADirectoryError):
            # NotADirectoryError can happen when scanning a file but one of its parent
            # directories is no longer a directory.
            logger.debug("\t...not found, deleting")
            with db.cursor() as cursor:
                cursor.execute("DELETE FROM fsentry WHERE id=?", (entry.id,))
            # Flag this entry as not new. Even though it's been deleted, the calling
            # scan() function makes sure scanned entries are no longer new, to guard against
            # infinite loops
            entry.new = False
            return
        times.append(time.monotonic_ns())
        if (
            entry.st_mode is not None
            and stat.S_ISDIR(entry.st_mode)
            and not stat.S_ISDIR(stat_result.st_mode)
        ):
            # Type of entry has changed from directory to something else. Normally,
            # directories when they are deleted will hit the FileNotFound case above,
            # which recursively cascades and deletes all children. But if a file is
            # recreated with the same name as the directory, it could leave orphaned
            # children in the database. While such children would be eventually cleaned
            # up as those entries are scanned, this code goes and cleans them up.
            entry.delete_children(db)

        if not force_scan and not entry.new and entry.compare_stat_info(stat_result):
            return

        if stat.S_ISDIR(stat_result.st_mode):
            children = entry.get_children(db)

            times.append(time.monotonic_ns())
            # Check the directory entries on the filesystem against the database.
            try:
                entries = set(os.listdir(entry.decoded_path))
            except PermissionError:
                logger.debug("\t...Permission denied")
                entries = set()

            times.append(time.monotonic_ns())
            # Create new entries
            new_names = entries.difference(c.decoded_path.name for c in children)
            for newname in new_names:
                if newname == "hermes":
                    continue
                newpath = entry.decoded_path / newname
                encoded_path = models.FSEntry.encode_path(newpath)
                try:
                    with db.atomic(), db.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO fsentry (path, parent, new) VALUES (?,?,?)",
                            (encoded_path, entry.id, True),
                        )

                except sqlite3.IntegrityError:
                    # This can happen if a new root was added to the database, but it was
                    # an ancestor of an existing root. In this case, we re-parent the
                    # existing root
                    with db.cursor() as cursor:
                        cursor.execute(
                            "UPDATE fsentry SET parent=? WHERE path=?",
                            (entry.id, models.FSEntry.encode_path(newpath)),
                        )
            if new_names:
                logger.debug(f"\t...Found {len(new_names)} new entries in this directory")

            times.append(time.monotonic_ns())
            # Delete old entries
            for child in children:
                if child.decoded_path.name not in entries:
                    with db.cursor() as cursor:
                        cursor.execute("DELETE FROM fsentry WHERE id=?", (child.id,))

        while len(times) < 5:
            times.append(times[-1])

        # Update this entry
        times.append(time.monotonic_ns())
        entry.update(db, None, False, stat_result)
        times.append(time.monotonic_ns())
        # entry.invalidate(db)
        # times.append(time.monotonic_ns())
        # print(*("{:12,d}".format(times[i + 1] - times[i]) for i in range(len(times) - 1)))
