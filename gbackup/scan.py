from tqdm import tqdm

from django.db.transaction import atomic

from . import models

@atomic()
def scan(progress=False, skip_existing=False):
    """Scans all FSEntry objects for changes

    The scan works in multiple passes. The first pass calls scan() on each
    existing FSEntry object in the database. During the scan, new FSEntries
    are added to the database for new directory entries found. Subsequent
    passes select new FSEntries from the database. This continues until no
    more new entries are found in the database. In effect, this is a breadth
    first search of the filesystem tree. From experimentation, this ends up
    being very quick since the database IO is relatively low; entries can be
    fetched in batch.

    """

    # Note about the below use of qs.iterator()
    # Usual evaluation of a queryset will pull every single entry into
    # memory, but we must avoid that since the table could be very large.
    # SQLite supports streaming rows from a query in batches, and Django
    # exposes this functionality with qs.iterator(), even though Django is
    # documented as not supporting it for SQLite [1][2]. This may be a bug in
    # Django or the Django docs, but it works to our advantage.

    # The caveat, and the reason Django probably doesn't support this,
    # is that SQLite doesn't have isolation between queries on the same
    # database connection [3]. According to the SQLite documentation,
    # a SELECT query that runs interleaved with an INSERT, UPDATE,
    # or DELETE on the same table results in undefined behavior.
    # Specifically, it's undefined whether the inserted/modified/deleted rows
    # will appear (perhaps for a second time) in the SELECT results. As long
    # as the program can handle that possibility, there's no risk of database
    # corruption or anything.

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

    if not skip_existing:
        # First pass, scan all existing entries
        qs = models.FSEntry.objects.all()

        if progress:
            entries = tqdm(
                qs.iterator(),
                desc="Existing Entries",
                total=qs.count(),
                unit="",
            )
        else:
            entries = qs.iterator()

        for entry in entries:
            entry.scan()

    # Now keep scanning for new objects until there are no more new objects
    qs = models.FSEntry.objects.filter(new=True)
    if progress:
        pbar = tqdm(
            desc="New Entries",
            unit=""
        )
    else:
        pbar = None

    while qs.all().exists():

        for entry in qs.iterator():
            entry.scan()

            # Guard against bugs in scan() causing an infinite loop. If this
            # item wasn't either deleted or marked new=False, then it would be
            # selected next pass
            assert entry.new is False or entry.id is None

            if pbar is not None:
                pbar.update(1)
    if pbar is not None:
        pbar.close()