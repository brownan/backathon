import hashlib

from tqdm import tqdm

from django.db.transaction import atomic
from django.core.files.storage import default_storage as storage

from . import models

def backup():

    # TODO: mark all parents of invalidated entries as invalidated themselves


    to_backup = models.FSEntry.objects.filter(objid__isnull=True)

    # The ready_to_backup set is the set of all nodes whose children have all
    # already been backed up. In other words, these are the entries that we
    # can back up right now.
    ready_to_backup = to_backup.exclude(
        id__in=to_backup.exclude(parent__isnull=True).values("parent_id")
    )

    progress = tqdm(total=to_backup.count(), unit="files")
    progress2 = tqdm(desc="pass", unit="")

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
            assert entry.objid_id is None

            iterator = entry.backup()
            try:
                obj_type, obj_buf = next(iterator)
                while True:
                    obj_type, obj_buf = iterator.send(
                        _backup_payload(obj_type, obj_buf)
                    )
            except StopIteration:
                pass

            # Sanity check: If a bug in the entry.backup() method doesn't do
            # one of these, the entry will be selected next iteration,
            # causing an infinite loop
            assert entry.objid_id is not None or entry.id is None

            progress.update(1)

        # Sanity check: if we entered the outer loop but the inner loop's
        # query didn't select anything, then we're not making progress and
        # may be caught in an infinite loop. In particular, this could happen
        # if we somehow got a cycle in the FSEntry objects in the database.
        # There would be entries needing backing up, but none of them have
        # all their dependent children backed up.
        assert ct > 0
        progress2.update(1)

@atomic()
def _backup_payload(objtype, buf):
    """Backs up the given payload, returning an Object instance

    This method is decorated with atomic() because the order of operations is
    to create the Object instance, and then upload it. If the upload fails,
    the Object should be rolled back. Once an object is successfully saved to
    the storage backend, the database should be committed.
    """
    view = buf.getbuffer()

    # 1. Compute the object ID. Use the buffer memoryview into the BytesIO to
    # avoid an extra copy
    objid = hashlib.sha256(view).hexdigest()

    # 2. Check if the object already exists in the data
    # store. If not, construct a new object
    obj_instance, isnew = models.Object.objects.get_or_create(
        objid=objid,
        defaults={
            'payload':view if objtype in ('tree', 'inode') else None,
        },
    )

    # 3. If not, upload it
    if isnew:
        name = "objects/{}/{}".format(
            objid[:2],
            objid,
        )
        storage.save(name, buf)

    return obj_instance

