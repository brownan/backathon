import os.path
import sys

from django.db import connections

from ..util import atomic_immediate
from .. import models
from . import CommandBase, CommandError


class Command(CommandBase):
    help = "Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str, nargs="+")

    def handle(self, options):
        repo = self.get_repo()

        with atomic_immediate(using=repo.db):
            before_count = models.FSEntry.objects.using(repo.db).count()
            for root_path in options.root:
                root_path = os.path.abspath(root_path)

                print("Deleting root {}... ".format(root_path), end="")
                sys.stdout.flush()

                try:
                    repo.del_root(root_path)
                except models.FSEntry.DoesNotExist:
                    raise CommandError("Root does not exist: {}".format(root_path))

                print("Done")

            after_count = models.FSEntry.objects.using(repo.db).count()

        if before_count - after_count > 1:
            print("Running database vacuum... ", end="")
            sys.stdout.flush()
            with connections[repo.db].cursor() as cursor:
                cursor.execute("VACUUM")
            print("Done")

        print()
        print(
            "Root{} removed:\n{}".format(
                "s" if len(options.root) != 1 else "",
                "\n".join("* " + s for s in options.root),
            )
        )
        print(
            "{} files and directories removed from backup "
            "set".format(before_count - after_count)
        )
