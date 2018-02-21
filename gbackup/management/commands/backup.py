import time

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from gbackup import backup
from gbackup import models

class Command(BaseCommand):
    help="Backs up changed files. Run a scan first to detect changes."

    def handle(self, *args, **kwargs):
        to_backup = models.FSEntry.objects.filter(obj__isnull=True)
        self.stderr.write("To back up: {} files totaling {}".format(
            to_backup.count(),
            filesizeformat(
                to_backup.aggregate(size=Sum("st_size"))['size']
            )
        ))

        t1 = time.time()
        backup.backup(progress_enable=True)
        t2 = time.time()

        self.stderr.write("Backup took {:.2f} seconds".format(
            t2-t1
        ))
