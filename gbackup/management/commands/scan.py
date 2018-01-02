import time

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from gbackup import scan
from gbackup import models

class Command(BaseCommand):
    help="Scan the filesystem for changes and update the cache database"

    def handle(self, *args, **kwargs):
        t1 = time.time()
        scan.scan()
        t2 = time.time()

        self.stderr.write("Scanned {} entries in {:.2f} seconds".format(
            models.FSEntry.objects.count(),
            t2-t1,
            ))
        self.stderr.write("Need to back up {} files and directories "
                          "totaling {}".format(
            models.FSEntry.objects.filter(obj__isnull=True).count(),
            filesizeformat(
                models.FSEntry.objects.aggregate(size=Sum("st_size"))['size']
            )
        ))
