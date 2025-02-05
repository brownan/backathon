import rich.live
import rich.progress
import typer

from backathon.backup import BackupProgressReport
from backathon.cmdline.common import BackathonContext
from backathon.cmdline.common import default_columns
from backathon.cmdline.types import PathOption

app = typer.Typer()


@app.command()
def backup(db_path: PathOption):
    b = BackathonContext.from_db_path(db_path)
    repo = b.repo

    progress = rich.progress.Progress(
        *default_columns,
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
