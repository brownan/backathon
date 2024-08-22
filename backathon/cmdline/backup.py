import click
import rich.live
import rich.progress
from rich.progress import BarColumn, TaskProgressColumn, TextColumn

from backathon.backup import BackupProgressReport
from backathon.cmdline.common import (
    BackathonContext,
    CountCompleteColumn,
    SpeedColumn,
    TimerColumn,
)


@click.command()
@click.pass_context
def backup(ctx: click.Context):
    b = BackathonContext.from_click_context(ctx)
    repo = b.repo

    progress = rich.progress.Progress(
        TextColumn("[progress.description]{task.description}"),
        TaskProgressColumn(),
        BarColumn(),
        CountCompleteColumn(),
        TimerColumn(),
        SpeedColumn(),
        speed_estimate_period=20,
    )
    entry_progress = progress.add_task("Items")
    size_progress = progress.add_task("Size", size_display=True)

    def update_progress(report: BackupProgressReport):
        progress.update(
            entry_progress,
            completed=report.count_progress,
            total=report.count_total,
        )
        progress.update(
            size_progress,
            completed=report.size_progress,
            total=report.size_total,
        )

    with rich.live.Live(progress, refresh_per_second=4):
        repo.backup(progress=update_progress)
