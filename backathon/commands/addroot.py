import os.path

import tqdm
from django.core.management.base import CommandError
from django.db import IntegrityError
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from ... import models
from . import BackathonCommand


class Command(BackathonCommand):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, *args, **kwargs):

        repo = kwargs['repo']

        root_path = os.path.abspath(kwargs.pop("root"))
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
            models.FSEntry.objects.using(repo.db).create(
                path=root_path,
            )
        except IntegrityError:
            raise CommandError("Path is already being backed up")
        self.stdout.write("Path added: {}".format(root_path))
        self.stdout.write("Performing scan")

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

        self.stderr.write("{} new entries (totaling {}) added to backup "
                          "set".format(
            num_files_after - num_files_before,
            filesizeformat(size_after-size_before)
        ))
