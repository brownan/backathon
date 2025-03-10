import asyncio
from collections import deque

import rich
import typer
from rich.console import Group
from rich.live import Live
from rich.progress import Progress

import backathon.scan
from backathon.cmdline.common import BackathonContext
from backathon.cmdline.common import FileListRenderable
from backathon.cmdline.common import default_columns
from backathon.cmdline.types import PathOption

app = typer.Typer()


@app.command()
def scan(db_path: PathOption, rescan_dirs: bool = False, no_rich: bool = False):
    b = BackathonContext.from_db_path(db_path)
    repo = b.repo
    if no_rich:

        async def start():
            await repo.scan_async(rescan_dirs=rescan_dirs)

        asyncio.run(start())
    else:
        progress = Progress(
            *default_columns,
            speed_estimate_period=20,
        )
        task1 = progress.add_task("Scanning", total=0)

        last_scanned_files = deque(maxlen=4)
        group = Group(progress, FileListRenderable(last_scanned_files))

        async def update(scan_progress: backathon.scan.ScanProgress | None):
            if scan_progress is not None:
                num = scan_progress.scanned
                total = scan_progress.total
                fname = scan_progress.last_path
                progress.update(task1, completed=num, total=total)
                last_scanned_files.append(fname)

        async def run_scan():
            async with repo.scan_job.channel.listen(update):
                await repo.scan_async(rescan_dirs=rescan_dirs)

        try:
            with Live(group, refresh_per_second=5, transient=True):
                asyncio.run(run_scan())
        finally:
            rich.print(progress)
