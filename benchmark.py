"""Benchmarking tool for backathon"""

import itertools
import logging
import pathlib
import random
import tempfile
from contextlib import ExitStack

import click
from rich.logging import RichHandler
from rich.progress import Progress

from backathon.cmdline.common import (
    default_columns,
)
from backathon.encryption.nacl import NaclEncrypter
from backathon.repository import Backathon
from backathon.storage.local import LocalStorage, LocalStorageConfig

logger = logging.getLogger("backathon.benchmark")


@click.command()
def main():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    logger.setLevel(logging.INFO)

    with ExitStack() as context:
        backup_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
        context.callback(lambda: logger.info("Removing backup dir..."))
        logger.info("Setting up test files for backing up at %s", backup_dir)
        populate_backup_files(backup_dir)

        db_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
        db_path = db_dir / "backathon.db"
        logger.info("Database is at %s", db_path)

        repo_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
        context.callback(lambda: logger.info("Removing repo dir..."))
        logger.info("Repo dir is at %s", repo_dir)

        logger.info("Generating encryption keys")
        encrypter = NaclEncrypter.new("password")

        logger.info("Initializing database")
        storage = LocalStorage(LocalStorageConfig(base_path=repo_dir))
        repo = Backathon.initialize(
            db_path,
            storage,
            encrypter,
        )
        repo.add_root(backup_dir)

        logger.info("Performing scan")
        with Progress(*default_columns) as progress:
            t = progress.add_task(description="Scanning")
            repo.scan(
                progress=lambda count, total, _: progress.update(
                    t, total=total, completed=count
                )
            )

        logger.info("Performing backup")
        with Progress(*default_columns) as progress:
            t = progress.add_task("Backing up")
            repo.backup(
                lambda info: progress.update(
                    t, total=info.count_total, completed=info.count_progress
                )
            )

        click.pause("Finished. Press any key to clean up files and exit.")


def populate_backup_files(base: pathlib.Path):
    rnd = random.Random(1)

    progress = Progress(*default_columns)
    task1 = progress.add_task("Generating large directory tree")
    task2 = progress.add_task("Generating small files", total=100_000, start=False)
    task3 = progress.add_task("Generating large file", start=False)

    with progress:
        # Create a tree of directories each with a 10MB file at the end
        dir_paths = list(itertools.product(["A", "B", "C", "D"], repeat=4))
        for d in progress.track(dir_paths, task_id=task1):
            path = base.joinpath(*d)
            path.mkdir(parents=True)
            path.joinpath("file").write_bytes(rnd.randbytes(10 * 2**20))

        # Create a directory with lots of small files
        path = base / "manyfiles"
        path.mkdir()
        progress.stop_task(task1)
        progress.start_task(task2)
        for i in progress.track(range(100_000), task_id=task2):
            path.joinpath(str(i)).write_bytes(rnd.randbytes(1024))

        # Create a really huge file
        progress.stop_task(task2)
        progress.start_task(task3)
        with base.joinpath("hugefile").open("wb") as fobj:
            for _ in progress.track(range(1_000), task_id=task3):
                fobj.write(rnd.randbytes(2**20))


if __name__ == "__main__":
    main()
