import datetime
import logging
import os
import os.path
import pathlib
import sys
from collections.abc import Collection
from functools import cached_property

from pydantic import BaseModel

from backathon.db import Database

scanlogger = logging.getLogger("backathon.scan")


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
    objid: bytes

    # These fields are cached about the object. They may or may not have
    # values depending on the object type. Additionally, they may not be
    # filled in after a restore, as the objects have not yet been downloaded
    # and decoded.
    type: str | None
    uploaded_size: int | None
    file_size: int | None
    last_modified_time: datetime.datetime

    def __str__(self):
        return self.objid.hex()[:7]


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

    parent: bytes
    child: bytes
    name: str | None

    def __repr__(self):
        return "<ObjectRelation {}→{}>".format(
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
    obj: bytes | None = None

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

    def get_children(self, db: Database) -> list["FSEntry"]:
        return list(
            db.get_objects(FSEntry, "SELECT * FROM fsentry WHERE parent=?", (self.id,))
        )

    def delete_children(self, db: Database):
        with db.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM fsentry WHERE parent = ?
            """,
                (self.id,),
            )

    def update(
        self, db: Database, obj: bytes | None, new: bool, stat_result: os.stat_result
    ):
        with db.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fsentry SET obj=?, new=?, st_mode=?, st_mtime_ns=?, st_size=?
                WHERE id=?
            """,
                (
                    obj,
                    new,
                    stat_result.st_mode,
                    stat_result.st_mtime_ns,
                    stat_result.st_size,
                    self.id,
                ),
            )
        self.obj = obj
        self.new = new
        self._update_stat_info(stat_result)


class Snapshot(BaseModel):
    """A snapshot of a filesystem at a particular time"""

    path: bytes
    root: bytes
    date: datetime.datetime

    @property
    def printablepath(self):
        """Used in printable representations"""
        # Use the replacement error handler to turn any surrogate codepoints
        # into something that won't crash attempts to encode them
        bytepath = os.fsencode(self.path)
        return bytepath.decode("utf-8", errors="replace")
