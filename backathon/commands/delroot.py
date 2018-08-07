import os.path
import sys

from django.db import connections

from ..util import atomic_immediate
from .. import models
from . import CommandBase, CommandError

class Command(CommandBase):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, options):

        repo = self.get_repo()

        root_path = os.path.abspath(options.root)

        print("Deleting root... ", end="")
        sys.stdout.flush()
        with atomic_immediate():
            before_count = models.FSEntry.objects.using(repo.db).count()

            try:
                repo.del_root(root_path)
            except models.FSEntry.DoesNotExist:
                raise CommandError("Root does not exist: {}".format(root_path))

            after_count = models.FSEntry.objects.using(repo.db).count()

        print("Done")
        if before_count-after_count > 1:
            print("Running database vacuum... ", end="")
            sys.stdout.flush()
            with connections[repo.db].cursor() as cursor:
                cursor.execute("VACUUM")
            print("Done")

        print()
        print("Root removed: {}".format(root_path))
        print("{} files and directories removed from backup "
              "set".format(before_count - after_count))
