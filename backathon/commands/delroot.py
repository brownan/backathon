import os.path


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

        with atomic_immediate():
            before_count = models.FSEntry.objects.using(repo.db).count()

            try:
                repo.del_root(root_path)
            except models.FSEntry.DoesNotExist:
                raise CommandError("Root does not exist: {}".format(root_path))

            after_count = models.FSEntry.objects.using(repo.db).count()

        print("Root removed: {}".format(root_path))
        print("{} files and directories removed from backup "
              "set".format(before_count - after_count))
