from __future__ import annotations

import json
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Self

import pydantic

if TYPE_CHECKING:
    from backathon import Database

ConfigType = Annotated[
    pydantic.JsonValue,
    pydantic.AfterValidator(lambda x: json.loads(x) if isinstance(x, str) else x),
    pydantic.PlainSerializer(lambda x: json.dumps(x), when_used="json"),
]

JsonList = Annotated[
    list,
    pydantic.BeforeValidator(lambda x: json.loads(x) if isinstance(x, str) else x),
    pydantic.PlainSerializer(lambda x: json.dumps(x), when_used="json"),
]


class Settings(pydantic.BaseModel):
    encrypter: Annotated[
        pydantic.ImportString,
        pydantic.Field(title="Encryption Backend", description="The encryption backend"),
    ]

    encrypter_config: Annotated[
        ConfigType,
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
        ConfigType,
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

    excludes: JsonList = pydantic.Field(
        default_factory=list,
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
            rows = cursor.fetchall()
            config_dict = initial_settings.model_dump() if initial_settings else {}
            config_dict.update(rows)
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
    encrypter_params: ConfigType
