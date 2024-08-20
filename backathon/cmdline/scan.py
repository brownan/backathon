from collections import deque
from datetime import timedelta
from typing import Sequence

import click
import rich
from rich.console import Console, ConsoleOptions, Group, RenderableType, RenderResult
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskProgressColumn,
    TextColumn,
)
from rich.text import Text

from backathon.cmdline.common import BackathonContext


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


@click.command()
@click.option("--rescan-dirs", is_flag=True)
@click.option("--no-rich", is_flag=True)
@click.pass_context
def scan(ctx: click.Context, rescan_dirs: bool, no_rich: bool = False):
    b = BackathonContext.from_click_context(ctx)
    repo = b.repo
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
