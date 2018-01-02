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
        if not os.path.isdir(root_path):
            raise CommandError("Not a directory: {}".format(root_path))

        try:
            models.FSEntry.objects.create(
                path=root_path,
            )
        except IntegrityError:
            raise CommandError("Path is already being backed up")
        self.stdout.write("Path added: {}".format(root_path))
