from collections import deque

import rich
import typer
from rich.console import Group
from rich.live import Live
from rich.progress import (
    Progress,
)

from backathon.cmdline.common import (
    BackathonContext,
    FileListRenderable,
    default_columns,
)
from backathon.cmdline.types import PathOption

app = typer.Typer()


@app.command()
def scan(db_path: PathOption, rescan_dirs: bool = False, no_rich: bool = False):
    b = BackathonContext.from_db_path(db_path)
    repo = b.repo
    if no_rich:
        repo.scan(rescan_dirs=rescan_dirs)
    else:
        progress = Progress(
            *default_columns,
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
