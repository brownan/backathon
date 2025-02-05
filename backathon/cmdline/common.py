import dataclasses
import pathlib
from datetime import timedelta
from typing import Sequence

import click
import rich.filesize
from rich.console import Console
from rich.console import ConsoleOptions
from rich.console import RenderableType
from rich.console import RenderResult
from rich.progress import BarColumn
from rich.progress import ProgressColumn
from rich.progress import Task
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.text import Text

import backathon.db
import backathon.repository


@dataclasses.dataclass
class BackathonContext:
    repo: backathon.repository.Backathon
    db: backathon.db.Database

    @classmethod
    def from_click_context(cls, ctx: click.Context):
        db = backathon.db.Database(ctx.obj["db_path"])
        repo = backathon.repository.Backathon(db)
        return cls(repo=repo, db=db)

    @classmethod
    def from_db_path(cls, path: pathlib.Path):
        db = backathon.db.Database(path)
        repo = backathon.repository.Backathon(db)
        return cls(repo=repo, db=db)


class CountCompleteColumn(ProgressColumn):
    def render(self, task: "Task") -> RenderableType:
        completed = int(task.completed)
        if task.fields.get("size_display"):

            def strfunc(v):
                return rich.filesize.decimal(int(v))

        else:

            def strfunc(v):
                return format(int(v), ",d")

        if task.total is None:
            return Text(f"{strfunc(completed)}/?", style="progress.download")
        else:
            return Text(
                f"{strfunc(completed)}/{strfunc(task.total)}", style="progress.download"
            )


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
        units = task.fields.get("units", "files")
        if task.speed:
            if task.fields.get("size_display"):
                speed_str = rich.filesize.decimal(int(task.speed)).rstrip() + "/s"
            else:
                speed_str = f"{task.speed:,.0f} {units}/s"
            return Text(speed_str, style="progress.download")
        else:
            return ""


# Set of default columns we use throughout
default_columns = (
    TextColumn("[progress.description]{task.description}"),
    TaskProgressColumn(),
    BarColumn(),
    CountCompleteColumn(),
    TimerColumn(),
    SpeedColumn(),
)


class FileListRenderable:
    def __init__(self, file_list: Sequence[str]):
        self.file_list = file_list

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        max_width = options.max_width
        for t in list(self.file_list):
            text = Text(t)
            text.truncate(max_width, overflow="ellipsis")
            yield text
