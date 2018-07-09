from django.db.transaction import atomic
from django.db import connections
from django.utils import timezone

from . import models

def backup(repo, progress=None):
    """Perform a backup

    This is usually called from Repository.backup()

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

            iterator = entry.backup()
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
