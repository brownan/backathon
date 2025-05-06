from __future__ import annotations

import json
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Self
from typing import TypeVar

import pydantic

from backathon.retention import RetentionSettings
from backathon.schedule import ScheduleSettings
from backathon.types import PathType

if TYPE_CHECKING:
    from backathon import Database

T = TypeVar("T")
"""Wraps a pydantic-compatible type in json for serialization

This is used below to serialize list, set, and json fields to a json string
for storage in an sqlite field. SQLite can store types like int and boolean
directly, but more complex types need to be serialized explicitly.
"""
JsonWrap = Annotated[
    T,
    pydantic.WrapSerializer(
        lambda x, h: json.dumps(h(x)), return_type=str, when_used="json"
    ),
    pydantic.BeforeValidator(lambda x: json.loads(x)),
]


class Settings(pydantic.BaseModel):
    """Backathon settings

    This class is used a bit differently than a normal pydantic model.
    Instead of serializing the entire instance using .model_dump_json(),
    we dump each individual field to json with .model_dump(mode='json')
    and then store each field in a separate row in the sqlite settings table.

    This lets fields like int and boolean to be stored natively in sqlite,
    while more complex types like list, set, and json objects will use the
    JsonWrap type above to serialize to string for storage in sqlite as text.
    """

    encrypter: Annotated[
        pydantic.ImportString,
        pydantic.Field(title="Encryption Backend", description="The encryption backend"),
    ]

    encrypter_config: Annotated[
        JsonWrap[pydantic.JsonValue],
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
        JsonWrap[pydantic.JsonValue],
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

    excludes: JsonWrap[set[PathType]] = pydantic.Field(
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
        JsonWrap[ScheduleSettings],
        pydantic.Field(
            title="Schedule Settings", default_factory=lambda: ScheduleSettings()
        ),
    ]

    retention_settings: Annotated[
        JsonWrap[RetentionSettings],
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
            db_config = {key: value for key, value in cursor.fetchall()}

            config_dict = (
                initial_settings.model_dump(mode="json") if initial_settings else {}
            )

            config_dict.update(db_config)

            return cls.model_validate(config_dict)

    def save(self, db: Database):
        with db.atomic():
            for key, value in self.model_dump(mode="json").items():
                # noinspection PyProtectedMember
                db._config_set(key, value)


class MarkerData(pydantic.BaseModel):
    name: str
    version: str
    encrypter: pydantic.ImportString
    encrypter_params: pydantic.JsonValue
