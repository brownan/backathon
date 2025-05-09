from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Collection

import pydantic

from backathon.retention import RetentionSettings
from backathon.schedule import ScheduleSettings
from backathon.types import PathType

if TYPE_CHECKING:
    pass

logger = logging.getLogger("backathon.settings")


class Settings(pydantic.BaseModel):
    """Backathon settings

    This class is used a bit differently than a normal pydantic model when
    saving and loading to/from the database.

    Instead of serializing the entire instance using .model_dump_json(),
    we dump each individual field to json with a type adapter for the field's
    type, and then store each field in a separate row in the sqlite settings
    table.

    This lets us edit individual fields in the database instead of reading/
    writing one giant json blob.
    """

    encrypter: Annotated[
        pydantic.ImportString,
        pydantic.Field(title="Encryption Backend", description="The encryption backend"),
    ]

    encrypter_config: Annotated[
        pydantic.JsonValue,
        pydantic.Field(
            title="Encrypter Config", description="Encrypter configuration, in JSON"
        ),
    ]

    storage: Annotated[
        pydantic.ImportString,
        pydantic.Field(
            title="Storage Backend",
            description="Storage backend to use for backed-up data",
        ),
    ]
    storage_config: Annotated[
        pydantic.JsonValue,
        pydantic.Field(
            title="Storage Config", description="Storage configuration, in JSON"
        ),
    ]

    enable_compression: Annotated[
        bool,
        pydantic.Field(
            title="Enable Compression",
            description="Whether to enable compression of backed-up data",
        ),
    ] = True

    excludes: set[PathType] = pydantic.Field(
        default_factory=set,
        title="Excludes",
        description="Local directories to exclude from backup",
    )

    inline_threshold: Annotated[
        int,
        pydantic.Field(
            title="Inline Threshold",
            description="Maximum size for files to automatically inline into INODE"
            " objects instead of uploading as separate BLOBS",
            ge=0,
        ),
    ] = (
        2**20
    )

    chunk_threshold: Annotated[
        int,
        pydantic.Field(
            title="Chunk Threshold",
            description="Maximum size for files to be uploaded into a single BLOB"
            " object. Files above this size will be split into multiple BLOBS",
            ge=0,
        ),
    ] = (
        30 * 2**20
    )

    chunk_size: Annotated[
        int,
        pydantic.Field(
            title="Chunk Size",
            description="Size of file chunks for files larger than the chunk_threshold "
            "parameter",
            ge=2**16,
        ),
    ] = (
        2**16
    )

    max_backup_workers: Annotated[
        int | None,
        pydantic.Field(
            title="Max Backup Workers",
            description="Maximum number of backup workers, for parallelized backups. "
            "If not set, defaults to 8 more than the number of CPU cores",
            gt=1,
        ),
    ] = None

    schedule_settings: Annotated[
        ScheduleSettings,
        pydantic.Field(
            title="Schedule Settings", default_factory=lambda: ScheduleSettings()
        ),
    ]

    retention_settings: Annotated[
        RetentionSettings,
        pydantic.Field(
            title="Retention Settings",
            default_factory=lambda: RetentionSettings(enabled=False, buckets=[]),
        ),
    ]

    def __setattr__(self, key, value):
        logger.debug("Settings setattr on %s", key)
        if key in super().model_fields:
            logger.debug("...Adding to changed attr set")
            changed_attrs = getattr(self, "_changed_attrs", None)
            if changed_attrs is None:
                changed_attrs = self._changed_attrs = set()
            changed_attrs.add(key)
        return super().__setattr__(key, value)

    @property
    def changed_attrs(self) -> Collection[str]:
        return getattr(self, "_changed_attrs", set())

    def clear_changed_attrs(self):
        s = getattr(self, "_changed_attrs", None)
        if s:
            s.clear()


class MarkerData(pydantic.BaseModel):
    name: str
    version: str
    encrypter: pydantic.ImportString
    encrypter_params: pydantic.JsonValue
