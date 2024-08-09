from __future__ import annotations

import datetime
import enum
import logging
import os
import os.path
import pathlib
import sys
from collections.abc import Collection
from functools import cached_property
from typing import IO, TYPE_CHECKING, Annotated, NamedTuple, NewType, cast

import msgpack
from pydantic import (
    BaseModel,
    EncodedBytes,
    EncoderProtocol,
    WrapSerializer,
)
from typing_extensions import Self

import backathon.db

if TYPE_CHECKING:
    pass

scanlogger = logging.getLogger("backathon.scan")

ObjIDType = NewType("ObjIDType", bytes)


class ObjectType(str, enum.Enum):
    INODE = "inode"
    BLOB = "blob"
    TREE = "tree"
    SYMLINK = "symlink"


class BytesHexEncoder(EncoderProtocol):
    @classmethod
    def decode(cls, data: bytes) -> bytes:
        str_data = data.decode("ascii")
        return bytes.fromhex(str_data)

    @classmethod
    def encode(cls, value: bytes) -> bytes:
        return value.hex().lower().encode("ascii")


class Object(BaseModel):
    """Represents a row in the object table

    This table keeps track of what objects exist in the remote repository

    The existence of an object in this table implies that an object has been
    committed to the repository.

    The children relation is used in the calculation of garbage objects. If
    an object depends on another in any way, it is added as a "child". Then,
    when a root object is deleted, a set of unreachable garbage objects can
    be calculated.
    """

    # This is the binary representation of the hash of the payload.
    # To get the int representation, you can use int.from_bytes(objid, 'little')
    # To get the hex representation, use objid.hex()
    # To create a bytes representation from a hex representation,
    # use bytes.fromhex(hex_representation)
    objid: ObjIDType

    # These fields are cached about the object. They may or may not have
    # values depending on the object type. Additionally, they may not be
    # filled in after a restore, as the objects have not yet been downloaded
    # and decoded.
    type: None | ObjectType
    uploaded_size: int | None
    file_size: int | None
    last_modified_time: datetime.datetime | None
    sha1: Annotated[
        bytes | None,
        WrapSerializer(func=lambda b, _: b.hex(), when_used="json-unless-none"),
    ]

    @property
    def objid_hex(self):
        return self.objid.hex()

    @property
    def objid_int(self):
        return int.from_bytes(self.objid, "little")

    def __str__(self):
        return self.objid_hex[:7]


class ObjectRelation(BaseModel):
    """Represents a row in the object_relations table

    Keeps track of the dependency graph between objects

    This model's primary purpose is to track object relations (when one object
    references another object) so that we can do garbage collection
    calculations purely on the client. This model is not used during a
    restore. During a restore, the object payloads are decoded and the object
    tree traversed from the validated contents of each object.

    This model is also used to support browsing file manifests in the UI.

    The relations table may not be filled in at all after a recovery,
    in which case UI browsing will be impossible or will have to fetch the
    objects on demand. But full restores of an entire snapshot are still
    possible.
    """

    parent: ObjIDType
    child: ObjIDType
    name: str | None

    def __repr__(self):
        return "<ObjectRelation {} - {}>".format(
            self.parent.hex()[:7],
            self.child.hex()[:7],
        )


class FSEntry(BaseModel):
    """Represents a row in the fsentry table

    Keeps track of an entry in the local filesystem, either a directory,
    or a file.

    This tracks the last known state of each filesystem entry, so that it can
    be compared to the actual state of the filesystem to see if it has changed.

    It also keeps track of the last known object ID that was uploaded for
    this object. If obj is null, then this entry is considered "dirty"
    and needs to be uploaded.
    """

    id: int
    objid: ObjIDType | None = None

    # Paths are stored as bytes here and in the database as a reminder that the
    # decoded strings may not be printable due to decoding errors -- we can't
    # generally know the encoding of filesystem names, and filesystems in linux
    # don't have to be valid byte strings in any encoding.
    #
    # The path attribute can be used in most os path apis. To print for console
    # or user output, use the printable_path property. The decoded_path property
    # may be used to manipulate the path as a pathlib.Path object, but still be careful
    # about printing those objects.
    path: bytes

    @staticmethod
    def encode_path(path: pathlib.Path) -> bytes:
        return os.fsencode(path.absolute())

    @cached_property
    def name(self) -> bytes:
        return os.path.basename(self.path)

    @cached_property
    def decoded_path(self) -> pathlib.Path:
        return pathlib.Path(os.fsdecode(self.path))

    @cached_property
    def printable_path(self) -> str:
        """Used in printable representations"""
        # Use the replacement error handler to turn any surrogate codepoints
        # into something that won't crash attempts to encode them
        return self.path.decode(sys.getfilesystemencoding(), errors="replace")

    parent: int | None = None

    new: bool = True

    # These fields are used to determine if an entry has changed
    st_mode: int | None = None
    st_mtime_ns: int | None = None
    st_size: int | None = None

    def _update_stat_info(self, stat_result: os.stat_result):
        self.st_mode = stat_result.st_mode
        self.st_mtime_ns = stat_result.st_mtime_ns
        self.st_size = stat_result.st_size

    def compare_stat_info(self, stat_result: os.stat_result):
        return (
            self.st_mode == stat_result.st_mode
            and self.st_mtime_ns == stat_result.st_mtime_ns
            and self.st_size == stat_result.st_size
        )

    def __repr__(self):
        return "<FSEntry {}>".format(self.printable_path)

    def __str__(self):
        return self.printable_path

    def matches_glob(self, patterns: Collection[str]) -> bool:
        return any(self.decoded_path.match(pattern) for pattern in patterns)

    def update(
        self,
        db: backathon.db.Database,
        objid: ObjIDType | None,
        new: bool,
        stat_result: os.stat_result,
    ):
        with db.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fsentry SET objid=?, new=?, st_mode=?, st_mtime_ns=?, st_size=?
                WHERE id=?
            """,
                (
                    objid,
                    new,
                    stat_result.st_mode,
                    stat_result.st_mtime_ns,
                    stat_result.st_size,
                    self.id,
                ),
            )
        self.objid = objid
        self.new = new
        self._update_stat_info(stat_result)


class Snapshot(BaseModel):
    """A snapshot of a filesystem at a particular time"""

    path: str
    root: Annotated[ObjIDType, EncodedBytes(encoder=BytesHexEncoder)]
    timestamp: datetime.datetime


class ObjectStats(BaseModel):
    """Information in the object payload header related to entries on the filesystem

    e.g. inode, tree, and symlink all have these fields in common, but blobs don't

    """

    size: int
    inode: int
    uid: int
    gid: int
    mode: int
    # in nanoseconds since the epoch
    mtime: int
    atime: int

    @classmethod
    def from_stat_result(cls, stat_result: os.stat_result):
        return cls(
            size=stat_result.st_size,
            inode=stat_result.st_ino,
            uid=stat_result.st_uid,
            gid=stat_result.st_gid,
            mode=stat_result.st_mode,
            mtime=stat_result.st_mtime_ns,
            atime=stat_result.st_atime_ns,
        )

    def __rich_repr__(self):
        yield "size", self.size
        yield "inode", self.inode
        yield "uid", self.uid
        yield "gid", self.gid
        yield "mode", oct(self.mode)
        yield "mtime", datetime.datetime.fromtimestamp(
            self.mtime / 1000000000, tz=datetime.timezone.utc
        ).astimezone().strftime("%c %Z")
        yield "atime", datetime.datetime.fromtimestamp(
            self.atime / 1000000000, tz=datetime.timezone.utc
        ).astimezone().strftime("%c %Z")


class BlobRef(NamedTuple):
    pos: int
    objid: ObjIDType

    def __rich_repr__(self):
        yield "pos", self.pos
        yield "objid", self.objid.hex().lower()


class EntryRef(NamedTuple):
    name: bytes
    objid: ObjIDType

    def __rich_repr__(self):
        yield "name", self.name.decode(sys.getfilesystemencoding(), errors="replace")
        yield "objid", self.objid.hex().lower()


class ObjectHeader(BaseModel):
    """Information that gets serialized into the top of every object payload"""

    type: ObjectType
    length: int
    stats: ObjectStats | None = None
    blobs: list[BlobRef] | None = None
    entries: list[EntryRef] | None = None

    def __rich_repr__(self):
        yield "type", self.type.name
        yield "length", self.length
        yield "stats", self.stats
        if self.blobs:
            yield "blobs", self.blobs
        if self.entries:
            yield "entries", self.entries

    def model_dump_msgpack(self) -> bytes:
        return cast(bytes, msgpack.packb(self.model_dump(exclude_defaults=True)))

    @classmethod
    def model_load_msgpack(cls, data: bytes) -> Self:
        return cls.model_validate(msgpack.unpackb(data))

    @classmethod
    def read_header(cls, buf: IO[bytes]) -> Self:
        """Reads the header from the stream and returns the ObjectHeader instance

        Leaves the stream open for reading at the position directly after the header

        """
        unpacker = msgpack.Unpacker(buf)
        header_data = unpacker.unpack()
        return cls.model_validate(header_data)

    @property
    def file_size(self) -> int | None:
        """Convenience property to get the file size of file (inode) objects"""
        return self.stats.size if self.stats is not None else None

    @property
    def last_modified_time(self) -> datetime.datetime | None:
        """Convenience property to get the mtime of a file (inode) object"""
        return (
            datetime.datetime.fromtimestamp(
                self.stats.mtime / 1_000_000_000, tz=datetime.timezone.utc
            )
            if self.stats is not None
            else None
        )
