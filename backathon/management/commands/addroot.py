import os.path

from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError
from django.db.models import Sum
from django.template.defaultfilters import filesizeformat

from ... import models, scan


class Command(BaseCommand):
    help="Adds the given filesystem path as a backup root"

    def add_arguments(self, parser):
        parser.add_argument("root", type=str)

    def handle(self, *args, **kwargs):

        root_path = os.path.abspath(kwargs.pop("root"))
        if not os.path.isdir(root_path):
            raise CommandError("Not a directory: {}".format(root_path))

        num_files_before = models.FSEntry.objects.count()
        size_before = models.FSEntry.objects.aggregate(size=Sum("st_size"))['size']
        if not size_before:
            # On first add, there will only be one item and it won't have a
            # size yet
            size_before = 0

        try:
            models.FSEntry.objects.create(
                path=root_path,
            )
        except IntegrityError:
            raise CommandError("Path is already being backed up")
        self.stdout.write("Path added: {}".format(root_path))
        self.stdout.write("Performing scan")

        scan.scan(progress=True, skip_existing=True)

        num_files_after = models.FSEntry.objects.count()
        size_after = models.FSEntry.objects.aggregate(size=Sum("st_size"))['size']

        self.stderr.write("{} new entries (totaling {}) added to backup "
                          "set".format(
            num_files_after - num_files_before,
            filesizeformat(size_after-size_before)
        ))
