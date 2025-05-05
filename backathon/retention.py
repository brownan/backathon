import datetime
import functools
import json
from typing import TYPE_CHECKING

import pydantic

if TYPE_CHECKING:
    from backathon import Backathon
from backathon.models import Snapshot


class Bucket(pydantic.BaseModel):
    timeframe: datetime.timedelta
    interval: datetime.timedelta


class RetentionSettings(pydantic.BaseModel):
    enabled: bool
    buckets: list[Bucket]


serialize_datetime = functools.partial(
    pydantic.TypeAdapter(datetime.datetime).dump_python,
    mode="json",
)


def run_retention(repo: "Backathon") -> list[Snapshot]:
    settings = repo.db.config.retention_settings

    with repo.db.cursor() as cursor:
        cursor.execute("SELECT path FROM snapshots GROUP BY path")
        all_paths: list[str] = [row[0] for row in cursor]

    saved_snapshots: set[int] = set()

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    for bucket in settings.buckets:
        for path in all_paths:
            bucket_min_time = now - bucket.timeframe

            snapshots = repo.db.query(
                Snapshot,
                """
                SELECT * FROM snapshots
                WHERE timestamp > ?
                  AND path = ?
                ORDER BY timestamp DESC
                """,
                (serialize_datetime(bucket_min_time), path),
            )

            last_kept: Snapshot | None = None
            for snapshot in snapshots:
                if (
                    last_kept is None
                    or snapshot.timestamp - last_kept.timestamp >= bucket.interval
                ):
                    saved_snapshots.add(snapshot.id)
                    last_kept = snapshot

    to_save = list(
        repo.db.query(
            Snapshot,
            """
        SELECT * FROM snapshots
        WHERE id NOT IN (SELECT value FROM json_each(?))
        ORDER BY timestamp DESC, path
        """,
            (json.dumps(list(saved_snapshots)),),
        )
    )
    return to_save
