
import tqdm
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from backathon import models
from backathon.commands import CommandBase


class Command(CommandBase):
    help = "Backs up changed files. Run a scan first to detect changes."

    def add_arguments(self, parser):
        parser.add_argument("--single", action="store_true", help="Single-thread mode")

    def handle(self, options):
        repo = self.get_repo()

        to_backup = models.FSEntry.objects.using(repo.db).filter(obj__isnull=True)

        total = to_backup.count()
        print(
            "To back up: {} files totaling {}".format(
                total, filesizeformat(to_backup.aggregate(size=Sum("st_size"))["size"])
            )
        )

        pbar = tqdm.tqdm(total=total, unit=" files")

        def progress(num, total):
            pbar.n = num
            pbar.total = total
            pbar.update(0)

        repo.backup(progress=progress, single=options.single)
