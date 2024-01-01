import sys
import logging

from django.db import connections
from django.template.defaultfilters import filesizeformat

import tqdm

from ..util import atomic_immediate
from . import CommandBase
from .. import garbage


class TQDMSpinner(tqdm.tqdm):
    def __new__(cls, *args, **kwargs):
        # The tqdm class wasn't designed for subclassing in mind. To make
        # nested progress bars work, all classes have to share the
        # cls._instances set, which is created on the first call to
        # tqdm.tqdm.__new__()
        try:
            cls._instances = tqdm.tqdm._instances
        except AttributeError:
            pass

        instance = super().__new__(cls, *args, **kwargs)

        if "_instances" not in tqdm.tqdm.__dict__:
            tqdm.tqdm._instances = cls._instances
        return instance

    @staticmethod
    def format_meter(
        n,
        total,
        elapsed,
        ncols=None,
        prefix="",
        ascii=False,
        unit="it",
        unit_scale=False,
        rate=None,
        bar_format=None,
        postfix=None,
        unit_divisor=1000,
    ):
        spin_chars = r"\|/-"
        spin_pos = spin_chars[n % len(spin_chars)]
        return "{}: {}".format(prefix, spin_pos)

    def __repr__(self, elapsed=None):
        if self.disable:
            return "{}: Done".format(self.desc)
        return super().__repr__(elapsed=elapsed)


class Command(CommandBase):
    help = "Delete garbage objects from the repository"

    def handle(self, options):
        logging.getLogger("backathon.garbage").setLevel(logging.INFO)

        repo = self.get_repo()

        filter_progress = TQDMSpinner(desc="Finding garbage", position=None)
        collect_progress = tqdm.tqdm(
            desc="Collecting garbage", unit=" objects", position=None
        )

        class ProgressIndicator(garbage.ProgressIndicator):
            def build_filter_progress(self):
                filter_progress.update(1)

            def delete_progress(self, s):
                collect_progress.update(1)

            def close(self):
                filter_progress.close()
                collect_progress.close()

        gc = garbage.GarbageCollector(repo, progress=ProgressIndicator())

        with atomic_immediate(repo.db):
            gc.build_filter()

            filter_progress.close()

            num_deleted, size_recovered = gc.delete_garbage()
            collect_progress.close()

        if num_deleted > 0:
            with connections[repo.db].cursor() as cursor:
                print("Running database vacuum...", end="")
                sys.stdout.flush()
                cursor.execute("VACUUM")
                print(" Done")

        print()
        print("Deleted {} objects of garbage".format(num_deleted))
        print("Recovered {} of storage space".format(filesizeformat(size_recovered)))
