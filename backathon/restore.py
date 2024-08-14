import asyncio
import logging
import os
import pathlib
import shutil
from typing import IO, Awaitable, Callable

from backathon.models import ObjectHeader, ObjectStats, ObjectType, ObjIDType

logger = logging.getLogger("backathon.restore")

GetObject = Callable[[ObjIDType], Awaitable[tuple[ObjectHeader, IO[bytes]]]]


async def restore_obj(
    objid: ObjIDType,
    path: pathlib.Path,
    get_object: GetObject,
):
    """Restore the given object to the given path

    If the object is a directory, recursively restores all directory entries within

    The given get_object() should return the decrypted, decompressed byte stream
    from the remote repository. get_object() is also responsible for verifying the
    integrity, including checking the object ID.
    """
    header, body = await get_object(objid)

    try:
        if header.type == ObjectType.FILE:
            return await _restore_file(path, header, body, get_object)
        elif header.type == ObjectType.TREE:
            return await _restore_dir(path, header, get_object)
        elif header.type == ObjectType.SYMLINK:
            return await _restore_symlink(path, header, body)
        else:
            raise ValueError(f"Cannot restore objects of type {header.type}")
    finally:
        body.close()


async def _restore_file(
    path: pathlib.Path,
    header: ObjectHeader,
    body: IO[bytes],
    get_object: GetObject,
):
    if path.exists():
        logger.error("%s: Already exists, refusing to overwrite", pathstr(path))
        return

    logger.info("Restoring %s", pathstr(path))

    try:
        if header.blobs is None:
            with path.open("wb") as fout:
                shutil.copyfileobj(body, fout)
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
    path: pathlib.Path, fobj: IO[bytes], pos: int, objid: ObjIDType, get_object: GetObject
):
    header, body = await get_object(objid)

    try:
        if header.type != ObjectType.BLOB:
            logger.error("Expected blob object: %s", objid.hex())
            return

        try:
            fobj.seek(pos)
            shutil.copyfileobj(body, fobj)
        except OSError as e:
            logger.error("%s: Error writing to file", pathstr(path))
    finally:
        body.close()


async def _restore_dir(
    path: pathlib.Path,
    header: ObjectHeader,
    get_object: GetObject,
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


async def _restore_symlink(path: pathlib.Path, header: ObjectHeader, body: IO[bytes]):
    target: bytes = body.read()
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
