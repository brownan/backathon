import time

from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

import tqdm

from .. import models
from . import CommandBase

class Command(CommandBase):
    help="Scan the filesystem for changes and update the cache database"

    def handle(self, options):
        repo = self.get_repo()

        pbar = None

        def progress(num, total):
            nonlocal pbar
            if pbar is None:
                pbar = tqdm.tqdm(total=total, unit=" files")
            pbar.n = num
            pbar.update(0)

        t1 = time.time()
        try:
            repo.scan(progress=progress)
            t2 = time.time()
        finally:
            if pbar is not None:
                pbar.close()

        print("Scanned {} entries in {:.2f} seconds".format(
            models.FSEntry.objects.using(repo.db).count(),
            t2-t1,
            ))

        to_backup = models.FSEntry.objects.using(repo.db).filter(obj__isnull=True)
        print("Need to back up {} files and directories "
                          "totaling {}".format(
            to_backup.count(),
            filesizeformat(
                to_backup.aggregate(size=Sum("st_size"))['size']
            )
        ))

        clean = models.FSEntry.objects.using(repo.db).filter(obj__isnull=False)
        print("{} files ({}) clean".format(
            clean.count(),
            filesizeformat(
                clean.aggregate(size=Sum("st_size"))['size']
            )
        ))
