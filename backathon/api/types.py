import pathlib
from typing import Collection
from typing import Container

import pydantic

from backathon.models import FSEntry
from backathon.models import ObjIDType
from backathon.types import PathType
from backathon.types import PrintableBytes
from backathon.types import PrintablePath


class FSEntryType(pydantic.BaseModel):
    # Wrapper for the FSEntry model used in the api, because the underlying
    # FSEntry model has byte fields that may not be serializable

    model_config = pydantic.ConfigDict(title="FSEntry")

    id: int
    objid: ObjIDType | None
    name: PrintableBytes
    path: PrintablePath
    parent: int | None
    new: bool
    st_mode: int | None
    st_mtime: int | None
    st_size: int | None

    @classmethod
    def from_fsentry(cls, entry: FSEntry):
        return cls.model_construct(
            id=entry.id,
            objid=entry.objid,
            name=entry.name,
            path=entry.decoded_path,
            parent=entry.parent,
            new=entry.new,
            st_mode=entry.st_mode,
            st_mtime=int(entry.st_mtime_ns // 1e9) if entry.st_mtime_ns else None,
            st_size=entry.st_size,
        )


class PathInfo(pydantic.BaseModel):
    path: PrintablePath
    key: PathType

    # Roots are checked
    root: bool

    # Excluded items are shown with an X
    excluded: bool

    # If this node is the parent of some root, then it is shown partially
    # checked
    parentOfRoot: bool

    @classmethod
    def from_path(
        cls,
        path: pathlib.Path,
        roots: Collection[pathlib.Path],
        excludes: Container[pathlib.Path],
    ):
        parent_of_root = any(path == p for root in roots for p in root.parents)

        return cls.model_construct(
            path=path,
            key=path,
            root=path in roots,
            excluded=path in excludes,
            parentOfRoot=parent_of_root,
        )


class Browse(pydantic.BaseModel):
    info: PathInfo
    children: list[PathInfo]
