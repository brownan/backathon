from django.db import connections

from .util import atomic_immediate
from . import models

def scan(alias, progress=None, skip_existing=False):
    """Scans all FSEntry objects for changes

    This is usually called from Repository.scan() and is tightly integrated
    with the Repository class. It lives in its own module for organizational
    reasons.

    The scan works in multiple passes. The first pass calls FSEntry.scan() on
    each existing FSEntry object in the database. During the scan, new FSEntries
    are added to the database for new directory entries found. Subsequent
    passes select new FSEntries from the database. This continues until no
    more new entries are found in the database. In effect, this is a breadth
    first search of the filesystem tree. From experimentation, this ends up
    being very quick since the database IO is relatively low; entries can be
    fetched in batch.

    :param alias: The database alias to use
    :param progress: A callback function that provides status updates on the
        scan
    :param skip_existing: Only scan new entries. This is used after adding a
        new root to just scan newly added files and directories.

    The progress callback function should have this signature:
    def progress(count, total):
        ...

    Where count is the number of entries processed so far, and total is the
    number of existing entries to scan. Total becomes None when we start to
    scan new entries.

    """

    # Note about the below use of qs.iterator()
    ###########################################
    # Usual evaluation of a queryset will pull every single entry into
    # memory, but we must avoid that since the table could be very large.
    # SQLite supports streaming rows from a query in batches, and Django
    # exposes this functionality with qs.iterator(), even though Django is
    # documented as not supporting it for SQLite [1][2]. This may be a bug in
    # Django or the Django docs, but it works to our advantage.

    # The caveat, and the real reason Django probably doesn't support this,
    # is that SQLite doesn't have isolation between queries on the same
    # database connection [3]. According to the SQLite documentation,
    # a SELECT query that runs interleaved with an INSERT, UPDATE,
    # or DELETE on the same table results in undefined behavior.
    # Specifically, it's undefined whether the inserted/modified/deleted rows
    # will appear (perhaps for a second time) in the SELECT results. As long
    # as the program can handle that possibility, there's no other problems
    # with doing this (there's no risk of database corruption or anything).

    # HOWEVER! Due to a Python bug [4] in versions <=3.5.2, Python may crash
    # due to misuse of the SQLite API. This is caused by the Python SQLite
    # driver resetting all SQLite statements when committing. Stepping over a
    # statement after a reset will start it from the beginning [5], but Python
    # keeps a cache of SQLite statements and thinks it's still reset. When
    # Python tries to re-use that statement by binding new parameters to it,
    # SQLite will return an error. SQLite doesn't allow binding parameters to a
    # statement that's stepped through results without resetting it first [6].

    # So this code is only compatible with Python 3.5.3 and above unless
    # someone finds another workaround.

    # This took me a good 2-3 days to figure out. Phew!

    # [1] https://docs.djangoproject.com/en/2.0/ref/models/querysets/#without-server-side-cursors
    # [2] https://github.com/django/django/blob/2.0/django/db/backends/sqlite3/features.py#L9
    # [3] https://sqlite.org/isolation.html
    # [4] https://bugs.python.org/issue10513
    # [5] https://sqlite.org/c3ref/reset.html
    # [6] https://sqlite.org/c3ref/bind_blob.html (see paragraph about SQLITE_MISUSE)

    # Note about the below use of atomic blocks
    ###########################################
    # The atomic blocks are a performance optimization. This way
    # entry.scan() calls are grouped in the same transaction. Without
    # this, not only does performance suffer from lots of small
    # transactions and extra IO, but the SQLite Write-ahead Log (WAL) grows
    # very large (gigabytes for less than 20,000 files scanned, when the
    # final DB size is less than 6 megabytes).
    # That last point actually puzzled me: why should the explicit
    # transaction make such a drastic difference in the WAL size?
    #
    # I believe this is due to 2 factors:
    #
    # 1. The WAL cannot be checkpointed while a SELECT statement is still
    #  open. I'm guessing SQLite must keep a read lock on the database even
    #  though the writes are still committing. This must prevent SQLite from
    #  auto-checkpointing during the inner loop while qs.iterator() is still
    #  open. I wasn't able to confirm this from the SQLite docs, but without
    #  the atomic block starting an explicit transaction, I observed it
    #  auto-checkpoint between outer loop iterations when qs.iterator()
    #  finishes.
    #
    # 2. SQLite won't re-use pages in the WAL across transactions; starting a
    #  new transaction will append new entries to the WAL. So lots of small
    #  write transactions in a situation where it can't checkpoint causes the
    #  WAL to grow. In contrast, re-writing the DB page within a
    #  transaction will re-write the same WAL page so the WAL stays small. I
    #  couldn't find exact details on this behavior in the SQLite docs,
    #  but it's consistent with what I observed. When we do the same operations
    #  in one big transaction, the WAL never grows beyond a few hundred KB.

    scanned = 0

    if not skip_existing:
        # First pass, scan all existing entries
        qs = models.FSEntry.objects.using(alias).all()
        total = qs.count()
        with atomic_immediate(using=alias):
            for entry in qs.iterator():
                entry.scan()

                if progress is not None:
                    scanned += 1
                    progress(scanned, total)

    # Now keep scanning for new objects until there are no more new objects
    # We evaluate this same queryset multiple times below. This only works
    # because neither .exists() nor .iterator() cache their results.
    qs = models.FSEntry.objects.using(alias).filter(new=True)
    while qs.exists():
        with atomic_immediate(using=alias):
            for entry in qs.iterator():
                entry.scan()

                if progress is not None:
                    scanned += 1
                    progress(scanned, None)

                # Guard against bugs in scan() causing an infinite loop. If this
                # item wasn't either deleted or marked new=False, then it would be
                # selected next pass
                assert entry.new is False or entry.id is None


    # This seems like as good a time as any to do this.
    with connections[alias].cursor() as cursor:
        cursor.execute("ANALYZE fsentry")

