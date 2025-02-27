from __future__ import annotations

import asyncio
import logging
import os
import pathlib
from typing import IO
from typing import TYPE_CHECKING
from typing import AsyncIterable

from typing_extensions import Buffer

from backathon.models import ObjectHeader
from backathon.models import ObjectStats
from backathon.models import ObjectType
from backathon.models import ObjIDType

if TYPE_CHECKING:
    from backathon.repository import ObjGetter

logger = logging.getLogger("backathon.restore")


async def restore_obj(
    objid: ObjIDType,
    path: pathlib.Path,
    get_object: ObjGetter,
):
    """Restore the given object to the given path

    If the object is a directory, recursively restores all directory entries within

    """
    raw_payload = await get_object(objid)
    header = raw_payload.header
    body = raw_payload.body

    if header.type == ObjectType.FILE:
        return await _restore_file(path, header, body, get_object)
    elif header.type == ObjectType.TREE:
        return await _restore_dir(path, header, get_object)
    elif header.type == ObjectType.SYMLINK:
        return await _restore_symlink(path, header, body)
    else:
        raise ValueError(f"Cannot restore objects of type {header.type}")


async def stream_file(
    objid: ObjIDType, get_object: ObjGetter
) -> tuple[ObjectHeader, AsyncIterable[bytes]]:
    """Yields a stream of bytes for the given file"""
    raw_payload = await get_object(objid)
    header = raw_payload.header
    body = raw_payload.body
    if header.type != ObjectType.FILE:
        raise ValueError("Object given is not a file")

    logger.info("Starting to stream file objid %s", objid.hex())

    if header.blobs is None:

        async def get_body_single():
            yield bytes(body)

        return header, get_body_single()

    else:
        header.blobs.sort(key=lambda blobref: blobref.pos)

        async def get_body_multiple(header: ObjectHeader):
            assert header.blobs is not None
            pos = 0
            for blobref in header.blobs:
                # Handle holes in the file
                if pos < blobref.pos:
                    yield b"\0" * (blobref.pos - pos)
                    pos = blobref.pos
                blob_payload = await get_object(blobref.objid)
                if blob_payload.header != ObjectType.BLOB:
                    logger.error("Expected blob object: %s", objid.hex())
                    return
                blob_body = bytes(blob_payload.body)
                yield blob_body
                pos += len(blob_body)

        return header, get_body_multiple(header)


async def _restore_file(
    path: pathlib.Path,
    header: ObjectHeader,
    body: Buffer,
    get_object: ObjGetter,
):
    if path.exists():
        logger.error("%s: Already exists, refusing to overwrite", pathstr(path))
        return

    logger.info("Restoring %s", pathstr(path))

    try:
        if header.blobs is None:
            with path.open("wb") as fout:
                fout.write(body)
        else:
            with path.open("wb") as fobj:
                coros = [
                    _write_blob(path, fobj, blobref.pos, blobref.objid, get_object)
                    for blobref in header.blobs
                ]
                await asyncio.gather(*coros)
    except OSError as e:
        logger.error("%s: Error writing to file: %s", pathstr(path), e)
        return

    if header.stats is None:
        logger.warning("%s: Can't restore stats. Missing stats header", pathstr(path))
    else:
        _set_file_properties(path, header.stats)


async def _write_blob(
    path: pathlib.Path, fobj: IO[bytes], pos: int, objid: ObjIDType, get_object: ObjGetter
):
    raw_payload = await get_object(objid)
    header = raw_payload.header

    if header.type != ObjectType.BLOB:
        logger.error("Expected blob object: %s", objid.hex())
        return

    try:
        fobj.seek(pos)
        fobj.write(raw_payload.body)
    except OSError as e:
        logger.error("%s: Error writing to file: %s", pathstr(path), e)


async def _restore_dir(
    path: pathlib.Path,
    header: ObjectHeader,
    get_object: ObjGetter,
):
    if path.exists() and not path.is_dir():
        logger.error(
            "%s: File already exists. Refusing to restore directory to this path.",
            pathstr(path),
        )
        return

    mode: int
    if header.stats is None:
        logger.warning(
            "%s: Cannot restore stats. Header missing stat information", pathstr(path)
        )
        mode = 0o777
    else:
        mode = header.stats.mode

    if not path.exists():
        try:
            path.mkdir(mode=mode)
        except OSError as e:
            logger.error("%s: Could not make directory: %s", pathstr(path), e)
            return

    if header.stats is not None:
        _set_file_properties(path, header.stats)

    if header.entries is None:
        logger.error("%s: Malformed header, entries list is missing", pathstr(path))
        return
    await asyncio.gather(
        *(
            restore_obj(entryref.objid, path / os.fsdecode(entryref.name), get_object)
            for entryref in header.entries
        )
    )


async def _restore_symlink(path: pathlib.Path, header: ObjectHeader, body: Buffer):
    target = bytes(body)
    if path.exists():
        logger.warning("%s: Path already exists. Not overriding", pathstr(path))
        return

    try:
        path.symlink_to(target)
    except OSError as e:
        logger.error("%s: Could not create symlink: %s", pathstr(path), e)
        return

    if header.stats is None:
        logger.warning("%s: Can't restore stats. Missing stats header", pathstr(path))
        return

    # Try to restore properties to the symlink itself, but only if the os supports not
    # following symlinks for these operations.
    if os.chown in os.supports_follow_symlinks:
        try:
            os.chown(path, header.stats.uid, header.stats.gid, follow_symlinks=False)
        except OSError as e:
            logger.warning("%s: Could not chown: %s", pathstr(path), e)

    if os.chmod in os.supports_follow_symlinks:
        try:
            os.chmod(path, header.stats.mode, follow_symlinks=False)
        except OSError as e:
            logger.warning("%s: Could not chmod: %s", pathstr(path), e)

    if os.utime in os.supports_follow_symlinks:
        try:
            os.utime(
                path, ns=(header.stats.atime, header.stats.mtime), follow_symlinks=False
            )
        except OSError as e:
            logger.warning("%s: Could not set mtime: %s", pathstr(path), e)


def _set_file_properties(path: pathlib.Path, stats: ObjectStats):
    """Sets the file properties of the given path

    Sets: owner, group, mode, atime, mtime
    """
    try:
        os.chown(path, stats.uid, stats.gid)
    except OSError as e:
        logger.warning("%s: Could not chown: %s", pathstr(path), e)
    try:
        os.chmod(path, stats.mode)
    except OSError as e:
        logger.warning("%s: Could not chmod: %s", pathstr(path), e)
    try:
        os.utime(path, ns=(stats.atime, stats.mtime))
    except OSError as e:
        logger.warning("%s: Could not set mtime: %s", pathstr(path), e)


def pathstr(p: str | pathlib.Path) -> str:
    """Returns the path string suitable for printing or logging"""
    return os.fsencode(os.fspath(p)).decode("utf-8", errors="replace")
