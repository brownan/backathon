from django.db.transaction import atomic
from django.db import connection
from django.utils import timezone
from tqdm import tqdm

from . import models
from .datastore import default_datastore

def backup(progress_enable=False):
    if models.FSEntry.objects.filter(new=True).exists():
        # This happens when a new root is added but hasn't been scanned yet.
        raise RuntimeError("You need to run a scan first")

    to_backup = models.FSEntry.objects.filter(obj__isnull=True)

    # The ready_to_backup set is the set of all nodes whose children have all
    # already been backed up. In other words, these are the entries that we
    # can back up right now.
    ready_to_backup = to_backup.exclude(
        id__in=to_backup.exclude(parent__isnull=True).values("parent_id")
    )

    if progress_enable:
        progress = tqdm(total=to_backup.count(), unit="")
    else:
        progress = None

    datastore = default_datastore

    while to_backup.exists():

        ct = 0
        for entry in ready_to_backup.iterator():
            ct += 1
            assert isinstance(entry, models.FSEntry)

            # This sanity check is just making sure that our query works
            # correctly by only selecting entries that haven't been backed up
            # yet. Because we're modifying entries and iterating over a
            # result set at the same time, SQLite may return row twice,
            # but since the modified rows don't match our query,
            # they shouldn't re-appear in this same query. However,
            # the SQLite documentation on isolation isn't clear on this. If I
            # see this assert statement getting hit in practice, then the
            # thing to do is to ignore the entry and move on.
            assert entry.obj_id is None

            iterator = entry.backup()
            try:
                obj_buf, obj_children = next(iterator)
                while True:
                    obj_buf, obj_children = iterator.send(
                        datastore.push_object(obj_buf, obj_children)
                    )
            except StopIteration:
                pass

            # Sanity check: If a bug in the entry.backup() method doesn't set
            # one of these, the entry will be selected next iteration,
            # causing an infinite loop
            assert entry.obj_id is not None or entry.id is None

            if progress is not None:
                progress.update(1)

        # Sanity check: if we entered the outer loop but the inner loop's
        # query didn't select anything, then we're not making progress and
        # may be caught in an infinite loop. In particular, this could happen
        # if we somehow got a cycle in the FSEntry objects in the database.
        # There would be entries needing backing up, but none of them have
        # all their dependent children backed up.
        assert ct > 0

    now = timezone.now()

    for root in models.FSEntry.objects.filter(
        parent__isnull=True
    ):
        assert root.obj_id is not None
        with atomic():
            ss = models.Snapshot.objects.create(
                path=root.path,
                root_id=root.obj_id,
                date=now,
            )
            datastore.put_snapshot(ss)

    cursor = connection.cursor()
    cursor.execute("ANALYZE")
    cursor.close()