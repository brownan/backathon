"""Benchmarking tool for backathon"""

import csv
import itertools
import logging
import pathlib
import random
import re
import subprocess
import tempfile
from contextlib import ExitStack
from typing import Callable

import click
import yappi
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
@click.option("-k", "--test-pattern")
def main(test_pattern: str):
    logging.basicConfig(
        level=logging.WARNING,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )
    logger.setLevel(logging.INFO)

    yappi.set_clock_type("wall")

    with ExitStack() as context:
        backup_dir = pathlib.Path(context.enter_context(tempfile.TemporaryDirectory()))
        context.callback(lambda: logger.info("Removing backup dir..."))
        logger.info("Setting up test files for backing up at %s", backup_dir)
        test_definitions: dict[str, pathlib.Path] = {}
        benchmarks: dict[str, BackupResults] = {}

        patterns: None | list[re.Pattern] = None
        if test_pattern:
            patterns = [
                re.compile(".*".join(re.escape(c) for c in s.split("*")))
                for s in test_pattern.split(",")
            ]

        with Progress() as progress:
            for testname, testgen in TEST_DEFS.items():
                if patterns and not any(p.search(testname) for p in patterns):
                    continue
                logger.info("Generating files for test %s", testname)
                task_id = progress.add_task(f"Generating Test Data for {testname}")
                testdir = pathlib.Path(
                    context.enter_context(tempfile.TemporaryDirectory())
                )
                testgen(
                    testdir,
                    lambda count, total: progress.update(
                        task_id, completed=count, total=total
                    ),
                )
                test_definitions[testname] = testdir

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
        yappi.get_func_stats().save("benchmark.pstat", type="pstat")
        logger.info("Function profile information written to benchmark.pstat")


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
    progress = Progress(*default_columns)
    scan_task_id = progress.add_task(description="Scan")
    backup_task_id = progress.add_task(description="Backup", start=False)
    with progress:
        repo.scan(
            progress=lambda count, total, _: progress.update(
                scan_task_id, total=total, completed=count
            )
        )
        progress.stop_task(scan_task_id)
        progress.start_task(backup_task_id)

        logger.info("Performing backup")
        with yappi.run():
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


TestFileGenerator = Callable[[pathlib.Path, Callable[[int, int], None]], None]


def test_dir_tree(testdir: pathlib.Path, update: Callable[[int, int], None]):
    # Create a tree of directories each with a 10MB file at the end
    rnd = random.Random(1)
    dir_paths = list(itertools.product(["A", "B", "C", "D"], repeat=5))
    i = 0
    for d in dir_paths:
        path = testdir.joinpath(*d)
        path.mkdir(parents=True)
        path.joinpath("file").write_bytes(rnd.randbytes(10 * 2**20))
        i += 1
        update(i, len(dir_paths))


def test_small_files(testdir: pathlib.Path, update: Callable[[int, int], None]):
    # Create a directory with lots of small files
    rnd = random.Random(1)
    count = 10_000
    for i in range(count):
        testdir.joinpath(str(i)).write_bytes(rnd.randbytes(1024))
        update(i + 1, count)


def test_huge_file(testdir: pathlib.Path, update: Callable[[int, int], None]):
    # Create a really huge file
    rnd = random.Random(1)
    with testdir.joinpath("hugefile").open("wb") as fobj:
        count = 1_000
        for i in range(count):
            fobj.write(rnd.randbytes(2**20))
            update(int(count + 1 // count * 100), 100)


TEST_DEFS: dict[str, TestFileGenerator] = {
    "dir_tree": test_dir_tree,
    "small_files": test_small_files,
    "huge_file": test_huge_file,
}


if __name__ == "__main__":
    main()
