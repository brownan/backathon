import logging
import pathlib
import sqlite3
from collections import deque
from datetime import timedelta
from typing import Sequence

import click
import rich
from rich.console import Console, ConsoleOptions, Group, RenderableType, RenderResult
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskProgressColumn,
    TextColumn,
)
from rich.text import Text

import backathon.db
import backathon.repository

logger = logging.getLogger("backathon.cmdline")


@click.group()
@click.argument(
    "configfile", type=click.Path(dir_okay=False, readable=True, path_type=pathlib.Path)
)
@click.option("--verbose", "-v", is_flag=True)
@click.pass_context
def main(
    ctx: click.Context,
    configfile: pathlib.Path,
    verbose: bool,
):
    loglevel = logging.INFO if not verbose else logging.DEBUG
    logging.basicConfig(format="%(message)s", level=loglevel, handlers=[RichHandler()])

    ctx.ensure_object(dict)

    db = backathon.db.Database(configfile)
    ctx.obj["db"] = db

    repo = backathon.repository.Backathon(db)
    ctx.obj["repo"] = repo


@main.command()
@click.argument("path", type=click.Path(path_type=pathlib.Path))
@click.pass_context
def add_root(ctx: click.Context, path: pathlib.Path):
    click.echo("Adding root {}".format(path))
    try:
        ctx.obj["b"].add_root(path)
    except sqlite3.IntegrityError as e:
        raise click.BadParameter(f"Root {path} already exists", param_hint="path")


@main.command()
@click.pass_context
def list_roots(ctx: click.Context):
    repo: backathon.repository.Backathon = ctx.obj["repo"]
    roots = repo.get_roots()
    if roots:
        for root in roots:
            click.echo(root)
    else:
        click.echo("no roots", err=True)


@main.command()
@click.pass_context
def edit_roots(ctx: click.Context):
    repo: backathon.repository.Backathon = ctx.obj["repo"]
    roots = repo.get_roots()
    text = "\n".join(str(r.decoded_path) for r in roots)
    new_text = click.edit(text)
    if new_text is not None:
        old_roots_set = set(str(r.decoded_path) for r in roots)
        new_roots = [line.strip() for line in new_text.split("\n")]
        new_roots = [line for line in new_roots if line]
        roots_deleted = 0
        roots_added = 0
        for to_del in set(old_roots_set).difference(new_roots):
            repo.del_root(pathlib.Path(to_del))
            roots_deleted += 1
        for to_add in set(new_roots).difference(old_roots_set):
            repo.add_root(pathlib.Path(to_add))
            roots_added += 1
        if roots_added:
            click.echo(
                "{} root{} added".format(roots_added, "s" if roots_added != 1 else "")
            )
        if roots_deleted:
            click.echo(
                "{} root{} removed".format(
                    roots_deleted, "s" if roots_deleted != 1 else ""
                )
            )
        if roots_deleted:
            # Deleting a root can cascade to a lot of metadata in the fsentry table. Recover
            # a bit of space if there are any empty pages
            click.echo("Cleaning up database...")
            with repo.db.cursor() as cursor:
                cursor.execute("PRAGMA incremental_vacuum")
                cursor.fetchall()

    else:
        click.echo("Roots unmodified")


@main.command()
@click.pass_context
def edit_excludes(ctx: click.Context):
    db: backathon.db.Database = ctx.obj["db"]
    current = db.config_get_json("excludes", [])
    text = """# Add excludes, one per line. Globs are supported.\n\n"""
    text += "\n".join(current)
    new_text = click.edit(text)
    if new_text is not None:
        new_lines = [line.strip() for line in new_text.split("\n")]
        new_lines = [line for line in new_lines if line and not line.startswith("#")]
        # TODO: if any excludes were removed, mark all directory entries as new so that
        # they are forced rescanned next scan
        # TODO: if excludes were added, we can do a single pass over the fsentry table
        # to remove any entries that are now excluded
        db.config_set_json("excludes", new_lines)
        click.echo("Exclude list updated")
    else:
        click.echo("Exclude list unchanged")


class CountCompleteColumn(ProgressColumn):
    def render(self, task: "Task") -> RenderableType:
        completed = int(task.completed)
        if task.total is None:
            return Text(f"{completed:,d}/?", style="progress.download")
        else:
            return Text(f"{completed:,d}/{int(task.total):,d}", style="progress.download")


class TimerColumn(ProgressColumn):
    def render(self, task: "Task") -> RenderableType:
        elapsed = task.elapsed
        if elapsed is None:
            elapsed_text = "-:--:--"
        else:
            delta = timedelta(seconds=max(0, int(elapsed)))
            elapsed_text = str(delta)

        if task.total is None or task.time_remaining is None or task.finished:
            remaining_text = ""
        else:
            minutes, seconds = divmod(int(task.time_remaining), 60)
            hours, minutes = divmod(minutes, 60)
            remaining_text = (
                f"[/]<[progress.remaining]{hours:d}:{minutes:02d}:{seconds:02d}"
            )
        return Text.from_markup(rf"[progress.elapsed]{elapsed_text}{remaining_text}")


class SpeedColumn(ProgressColumn):
    def render(self, task: "Task") -> RenderableType:
        if task.speed:
            return Text(f"{task.speed:,.0f} files/s", style="progress.download")
        else:
            return ""


class FileListRenderable:
    def __init__(self, file_list: Sequence[str]):
        self.file_list = file_list

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        max_width = options.max_width
        for t in list(self.file_list):
            text = Text(t)
            text.truncate(max_width, overflow="ellipsis")
            yield text


@main.command()
@click.option("--rescan-dirs", is_flag=True)
@click.option("--no-rich", is_flag=True)
@click.pass_context
def scan(ctx: click.Context, rescan_dirs: bool, no_rich: bool = False):
    repo: backathon.repository.Backathon = ctx.obj["repo"]
    if no_rich:
        repo.scan(rescan_dirs=rescan_dirs)
    else:
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            TaskProgressColumn(),
            BarColumn(),
            CountCompleteColumn(),
            TimerColumn(),
            SpeedColumn(),
            speed_estimate_period=20,
        )
        task1 = progress.add_task("Scanning", total=0)

        last_scanned_files = deque(maxlen=4)
        group = Group(progress, FileListRenderable(last_scanned_files))

        def update(num: int, total: None | int, fname: str):
            progress.update(task1, completed=num, total=total)
            last_scanned_files.append(fname)

        try:
            with Live(group, refresh_per_second=5, transient=True):
                repo.scan(progress=update, rescan_dirs=rescan_dirs)
        finally:
            rich.print(progress)


if __name__ == "__main__":
    main()
