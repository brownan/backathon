from tqdm import tqdm

from . import models

def scan():
    """Scans all FSEntry objects for changes


    """

    qs = models.FSEntry.objects.all()
    pass_ = 0
    while qs.exists():

        pass_ += 1
        count = qs.count()

        # Note: we're iterating over results of a query, while simultaneously
        # inserting new rows to the database that match that query. According to
        # the sqlite3 documentation on isolation, it's undefined whether or
        # not a row will be returned in a query that was started before
        # the row was inserted.
        # For this particular scenario, it shouldn't matter because we want
        # those rows returned eventually, either this or the next iteration
        # of the while loop.
        # https://sqlite.org/isolation.html

        for entry in tqdm(
            qs.iterator(),
            desc="Pass {}".format(pass_),
            total=count,
            unit='entries',
        ):
            entry.scan()

        qs = models.FSEntry.objects.filter(new=True)
