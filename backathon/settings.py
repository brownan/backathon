from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Self

import pydantic

from backathon.retention import RetentionSettings
from backathon.schedule import ScheduleSettings
from backathon.signals import ConfigChange
from backathon.types import PathType

if TYPE_CHECKING:
    from backathon import Backathon
    from backathon import Database

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

    migration: Annotated[
        int | None,
        pydantic.Field(
            description="Internal field to track database migrations. Don't change "
            "unless you know what you're doing"
        ),
    ] = None

    @classmethod
    def load(cls, db: Database, initial_settings: Self | None = None) -> Self:
        with db.cursor() as cursor:
            cursor.execute("SELECT key, value FROM config")
            db_config = {
                key: pydantic.TypeAdapter(cls.model_fields[key].annotation).validate_json(
                    value
                )
                for key, value in cursor.fetchall()
                if key in cls.model_fields
            }

            config_dict = initial_settings.model_dump() if initial_settings else {}

            config_dict.update(db_config)

            return cls.model_validate(config_dict)

    def save(self, repo: "Backathon", all: bool = False):
        db = repo.db
        changed_attrs = getattr(self, "_changed_attrs", set())
        if not all:
            logger.debug("Saving change attributes: %s", changed_attrs)
        with db.atomic():
            for key, value in self:
                if all or key in changed_attrs:
                    adapter = pydantic.TypeAdapter(self.model_fields[key].annotation)
                    ser_value = adapter.dump_json(value).decode("utf-8")

                    # noinspection PyProtectedMember
                    db._config_set(key, ser_value)
        for key in changed_attrs:
            repo.signals.send(ConfigChange(key=key))
        changed_attrs.clear()

    def __setattr__(self, key, value):
        logger.debug("Settings setattr on %s", key)
        if key in super().model_fields:
            logger.debug("...Adding to changed attr set")
            changed_attrs = getattr(self, "_changed_attrs", None)
            if changed_attrs is None:
                changed_attrs = self._changed_attrs = set()
            changed_attrs.add(key)
        return super().__setattr__(key, value)


class MarkerData(pydantic.BaseModel):
    name: str
    version: str
    encrypter: pydantic.ImportString
    encrypter_params: pydantic.JsonValue
