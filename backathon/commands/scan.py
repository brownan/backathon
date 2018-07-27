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

        roots = repo.get_roots()
        print("Scanning {} root{}:".format(
            len(roots),
            "s" if len(roots) != 1 else "",
        ))
        for root in repo.get_roots():
            print("* " + root.printablepath)
        print()

        def progress(num, total):
            nonlocal pbar
            if pbar is None:
                pbar = tqdm.tqdm(total=total, unit=" files")
            pbar.n = num
            pbar.total = total
            pbar.update(0)

        try:
            try:
                repo.scan(progress=progress)
            finally:
                if pbar is not None:
                    pbar.close()
        except KeyboardInterrupt:
            print("Scan canceled")
            return

        print("Scanned {} entries".format(
            models.FSEntry.objects.using(repo.db).count(),
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
