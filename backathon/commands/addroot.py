import os.path

import tqdm
from django.db import IntegrityError
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from backathon.util import atomic_immediate
from .. import models
from . import CommandBase, CommandError, RepoCommand


class Command(RepoCommand):
    help="Adds the given filesystem path as a backup root"

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument("root", type=str, nargs="+",
                            help="Filesystem path to the new backup root")
        parser.add_argument("--skip-scan", action='store_true',
                            help="Skip scanning the new root path")

    def handle(self):

        with atomic_immediate(using=repo.db):
            for root_path in options.root:
                root_path = os.path.abspath(root_path)

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

        print("Backup root{} added:\n{}".format(
            "s" if len(options.root) != 1 else "",
            "\n".join("* "+s for s in options.root)
        ))

        if options.skip_scan:
            print("Skipping scan. Make sure you run a scan before a backup")
            return

        print()
        print("Scanning for new files. This may take a while.")
        print("The initial scan has to build a local cache of file metadata;")
        print("re-scans will be faster")

        pbar = None

        def progress(num, total):
            nonlocal pbar
            if pbar is None:
                pbar = tqdm.tqdm(unit=" files")
            pbar.n = num
            pbar.update(0)

        try:
            try:
                repo.scan(progress=progress, skip_existing=True)
            finally:
                if pbar is not None:
                    pbar.close()
        except KeyboardInterrupt:
            print("Scan canceled. Make sure to run a scan operation before a "
                  "backup")
            print("or not all your files will be backed up")
            return

        num_files_after = models.FSEntry.objects.using(repo.db).count()
        size_after = models.FSEntry.objects.using(repo.db)\
            .aggregate(size=Sum("st_size"))['size']

        print("{} new entries (totaling {}) added to backup "
                          "set".format(
            num_files_after - num_files_before,
            filesizeformat(size_after-size_before)
        ))
