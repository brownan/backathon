from django.core.management.base import BaseCommand

from gbackup import models

class Command(BaseCommand):
    help="Lists currently defined backup roots"


    def handle(self, *args, **kwargs):

        for entry in models.FSEntry.objects.filter(parent__isnull=True):
            self.stdout.write(entry.printablepath)
