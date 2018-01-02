import os.path

from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError

from gbackup import models

class Command(BaseCommand):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, *args, **kwargs):

        root_path = os.path.abspath(kwargs.pop("root"))
        try:
            entry = models.FSEntry.objects.get(
                path=root_path,
            )
        except models.FSEntry.DoesNotExist:
            raise CommandError("Path not being backed up: {}".format(root_path))

        if entry.parent_id is not None:
            raise CommandError("Path not a backup root: {}".format(root_path))

        entry.delete()
        self.stdout.write("Path removed from backup set: {}".format(root_path))
