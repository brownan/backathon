import os.path

from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError

from gbackup import models, scan


class Command(BaseCommand):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, *args, **kwargs):

        root_path = os.path.abspath(kwargs.pop("root"))
        if not os.path.isdir(root_path):
            raise CommandError("Not a directory: {}".format(root_path))

        num_files = models.FSEntry.objects.count()

        try:
            models.FSEntry.objects.create(
                path=root_path,
            )
        except IntegrityError:
            raise CommandError("Path is already being backed up")
        self.stdout.write("Path added: {}".format(root_path))
        self.stdout.write("Performing scan")

        scan.scan(progress=True, skip_existing=True)

        new_num_files = models.FSEntry.objects.count()
        self.stderr.write("{} new entries added to backup set".format(
            new_num_files - num_files
        ))
