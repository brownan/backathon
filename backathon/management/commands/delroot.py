import os.path

from django.core.management.base import CommandError

from ...util import atomic_immediate
from ... import models
from . import BackathonCommand

class Command(BackathonCommand):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, *args, **kwargs):

        repo = kwargs['repo']

        root_path = os.path.abspath(kwargs.pop("root"))
        try:
            entry = models.FSEntry.objects.using(repo.db).get(
                path=root_path,
            )
        except models.FSEntry.DoesNotExist:
            raise CommandError("Path not being backed up: {}".format(root_path))

        if entry.parent_id is not None:
            raise CommandError("Path not a backup root: {}".format(root_path))

        with atomic_immediate():
            before_count = models.FSEntry.objects.using(repo.db).count()

            entry.delete()

            after_count = models.FSEntry.objects.using(repo.db).count()
        self.stdout.write("Root removed: {}".format(root_path))
        self.stdout.write("{} files/directories removed from backup "
                          "set".format(before_count - after_count))
