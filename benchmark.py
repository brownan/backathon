"""Benchmarking tool for backathon"""

import csv
import itertools
import logging
import pathlib
import random
import subprocess
import tempfile
from contextlib import ExitStack

import click
from rich.logging import RichHandler
from rich.progress import Progress
from typing_extensions import NamedTuple

from backathon.cmdline.common import (
    default_columns,
)
from backathon.encryption.nacl import NaclEncrypter
from backathon.repository import Backathon
from backathon.storage.local import LocalStorage, LocalStorageConfig

logger = logging.getLogger("backathon.benchmark")


class BackupResults(NamedTuple):
    scan_time: float
    scan_speed: float
    backup_time: float
    backup_speed: float


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
        test_definitions = populate_backup_files(backup_dir)
        benchmarks: dict[str, BackupResults] = {}

        for testname, testdir in test_definitions.items():
            logger.info("Running test %s", testname)
            results = perform_single_benchmark(context, testdir)
            benchmarks[testname] = results

        git_hash = subprocess.check_output(
            "git log --pretty=format:%h -1".split(), encoding="utf-8"
        )

        benchmark_file = pathlib.Path("benchmark.csv")
        exists = benchmark_file.exists()
        writer = csv.writer(benchmark_file.open("a"))

        if not exists:
            headers = []
            for testname in test_definitions:
                for f in BackupResults._fields:
                    headers.append(f"{testname} {f}")

            writer.writerow(["git rev", *headers])

        cols = []
        for testname in test_definitions:
            results = benchmarks[testname]
            cols.extend(
                [
                    format(results.scan_time, ".1f"),
                    format(results.scan_speed, ".0f"),
                    format(results.backup_time, ".1f"),
                    format(results.backup_speed, ".0f"),
                ]
            )
        writer.writerow([git_hash, *cols])
        logger.info("Results written to %s", benchmark_file)


def perform_single_benchmark(
    context: ExitStack, backup_dir: pathlib.Path
) -> BackupResults:
    db_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
    db_path = db_dir / "backathon.db"

    repo_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
    context.callback(lambda: logger.info("Removing repo dir %s...", repo_dir))

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
        scan_task_id = progress.add_task(description="Scan")
        backup_task_id = progress.add_task(description="Backup", start=False)

        repo.scan(
            progress=lambda count, total, _: progress.update(
                scan_task_id, total=total, completed=count
            )
        )
        progress.stop_task(scan_task_id)
        progress.start_task(backup_task_id)

        logger.info("Performing backup")
        repo.backup(
            lambda info: progress.update(
                backup_task_id, total=info.count_total, completed=info.count_progress
            )
        )
        progress.stop_task(backup_task_id)

    tasks = {task.id: task for task in progress.tasks}
    scan_task = tasks[scan_task_id]
    backup_task = tasks[backup_task_id]
    assert scan_task.elapsed is not None
    assert backup_task.elapsed is not None
    return BackupResults(
        scan_task.elapsed,
        scan_task.completed / scan_task.elapsed,
        backup_task.elapsed,
        backup_task.completed / backup_task.elapsed,
    )


def populate_backup_files(base: pathlib.Path):
    rnd = random.Random(1)

    progress = Progress(*default_columns)
    task1 = progress.add_task("Generating large directory tree")
    task2 = progress.add_task("Generating small files", total=0, start=False)
    task3 = progress.add_task("Generating large file", total=0, start=False)

    with progress:
        # Create a tree of directories each with a 10MB file at the end
        dir_paths = list(itertools.product(["A", "B", "C", "D"], repeat=4))
        for d in progress.track(dir_paths, task_id=task1):
            path = base.joinpath("tree", *d)
            path.mkdir(parents=True)
            path.joinpath("file").write_bytes(rnd.randbytes(10 * 2**20))

        # Create a directory with lots of small files
        path = base / "manyfiles"
        path.mkdir()
        progress.stop_task(task1)
        progress.start_task(task2)
        for i in progress.track(range(10_000), task_id=task2):
            path.joinpath(str(i)).write_bytes(rnd.randbytes(1024))

        # Create a really huge file
        progress.stop_task(task2)
        progress.start_task(task3)
        hugefile_dir = base / "hugefile"
        hugefile_dir.mkdir()
        with hugefile_dir.joinpath("hugefile").open("wb") as fobj:
            for _ in progress.track(range(1_000), task_id=task3):
                fobj.write(rnd.randbytes(2**20))

    return {
        "Dir Tree": base / "tree",
        "Many Files": base / "manyfiles",
        "Huge file": hugefile_dir,
    }


if __name__ == "__main__":
    main()
