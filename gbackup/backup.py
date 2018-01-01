import hashlib
import hmac

import umsgpack

from tqdm import tqdm

from django.db.transaction import atomic
from django.core.files.storage import default_storage as storage

from . import models

def backup():

    # TODO: mark all parents of invalidated entries as invalidated themselves


    to_backup = models.FSEntry.objects.filter(objid__isnull=True)

    # The ready_to_backup set is the set of all nodes that don't have any
    # children that haven't been backed up yet.
    ready_to_backup = to_backup.exclude(
        id__in=to_backup.exclude(parent__isnull=True).values("parent_id")
    )

    progress = tqdm(total=to_backup.count(), unit="files")
    progress2 = tqdm(desc="pass", unit="")

    while to_backup.exists():

        for entry in ready_to_backup.iterator():
            assert isinstance(entry, models.FSEntry)
            assert entry.objid_id == None
            iterator = entry.backup()
            try:
                obj_type, obj_buf = next(iterator)
                while True:
                    obj_type, obj_buf = iterator.send(
                        _backup_payload(obj_type, obj_buf)
                    )
            except StopIteration:
                pass
            progress.update(1)
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

