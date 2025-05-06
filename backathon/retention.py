import datetime
import functools
import json
from typing import TYPE_CHECKING

import pydantic

if TYPE_CHECKING:
    from backathon import Backathon
from backathon.models import Snapshot


class Bucket(pydantic.BaseModel):
    """A retention bucket

    Snapshots that fall into at least one bucket will be retained (not deleted)

    Each bucket defines a timeframe, interval, and count.

    The timeframe defines the period of time that is considered for inclusion
    to this bucket. The timeframe extends from now into the past by the defined
    length of time.

    The interval is the minimum time delta between snapshots within the bucket.
    Only one snapshot per interval may be included in the bucket.

    Count is the maximum number of snapshots that may be included in the bucket,
    or if the unlimited flag is set, unlimited.

    Snapshots are allocated into buckets greedily starting from most recent
    to oldest.
    """

    timeframe: datetime.timedelta
    interval: datetime.timedelta
    count: int
    unlimited: bool = False


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
            count = 0
            for snapshot in snapshots:
                if (
                    last_kept is None
                    or snapshot.timestamp - last_kept.timestamp >= bucket.interval
                ):
                    saved_snapshots.add(snapshot.id)
                    last_kept = snapshot
                    # Structuring the condition here ensures we'll always
                    # keep the most recent snapshot in each bucket.
                    count += 1
                    if not bucket.unlimited and count > bucket.count:
                        break

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
