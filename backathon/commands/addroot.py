import os.path

import tqdm
from django.db import IntegrityError
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from .. import models
from . import CommandBase, CommandError


class Command(CommandBase):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)
        parser.add_argument("--skip-scan", action='store_true')

    def handle(self, options):

        repo = self.get_repo()

        root_path = os.path.abspath(options.root)
        if not os.path.isdir(root_path):
            raise CommandError("Not a directory: {}".format(root_path))

        num_files_before = models.FSEntry.objects.using(repo.db).count()
        size_before = models.FSEntry.objects.using(repo.db)\
            .aggregate(size=Sum("st_size"))['size']
        if not size_before:
            # On first add, there will only be one item and it won't have a
            # size yet
            size_before = 0

        try:
            repo.add_root(root_path)
        except IntegrityError:
            raise CommandError("Path is already being backed up")
        print("Backup root added: {}".format(root_path))

        if options.skip_scan:
            print("Skipping scan. Make sure you run a scan before a backup")
            return

        print("Performing scan")

        pbar = None

        def progress(num, total):
            nonlocal pbar
            if pbar is None:
                pbar = tqdm.tqdm(unit=" files")
            pbar.n = num
            pbar.update(0)

        try:
            repo.scan(progress=progress, skip_existing=True)
        finally:
            if pbar is not None:
                pbar.close()

        num_files_after = models.FSEntry.objects.using(repo.db).count()
        size_after = models.FSEntry.objects.using(repo.db)\
            .aggregate(size=Sum("st_size"))['size']

        print("{} new entries (totaling {}) added to backup "
                          "set".format(
            num_files_after - num_files_before,
            filesizeformat(size_after-size_before)
        ))
