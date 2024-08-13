import asyncio
import logging
import os
import pathlib
import shutil
from typing import IO, Awaitable, Callable

from backathon.exceptions import CorruptedRepository
from backathon.models import ObjectHeader, ObjectStats, ObjectType, ObjIDType

logger = logging.getLogger("backathon.restore")


async def restore_obj(
    objid: ObjIDType,
    path: pathlib.Path,
    get_object: Callable[[ObjIDType], Awaitable[IO[bytes]]],
):
    """Restore the given object to the given path

    If the object is a directory, recursively restores all directory entries within

    The given get_object() should return the decrypted, decompressed byte stream
    from the remote repository. get_object() is also responsible for verifying the
    integrity, including checking the object ID.
    """
    payload = await get_object(objid)
    header = ObjectHeader.from_stream(payload)

    if header.type == ObjectType.FILE:
        return await _restore_file(path, header, payload, get_object)
    elif header.type == ObjectType.TREE:
        return await _restore_dir(path, header, get_object)
    elif header.type == ObjectType.SYMLINK:
        return await _restore_symlink(path, header, payload)
    else:
        raise ValueError(f"Cannot restore objects of type {header.type}")


async def _restore_file(
    path: pathlib.Path,
    header: ObjectHeader,
    body: IO[bytes],
    get_object: Callable[[ObjIDType], Awaitable[IO[bytes]]],
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
                    _write_blob(
                        path, fobj, blobref.pos, blobref.objid, get_object(blobref.objid)
                    )
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
    path: pathlib.Path,
    fobj: IO[bytes],
    pos: int,
    objid: ObjIDType,
    stream_future: Awaitable[IO[bytes]],
):
    payload = await stream_future
    header = ObjectHeader.from_stream(payload)

    if header.type != ObjectType.BLOB:
        logger.error("Expected blob object: %s", objid.hex())
        return

    try:
        fobj.seek(pos)
        shutil.copyfileobj(payload, fobj)
    except OSError as e:
        logger.error("%s: Error writing to file", pathstr(path))


async def _restore_dir(
    path: pathlib.Path,
    header: ObjectHeader,
    get_object: Callable[[ObjIDType], Awaitable[IO[bytes]]],
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


def restore_item(repo, objid, path, key=None):
    """Restore the given object to the given path

    The last component of path is the item we're restoring. If it
    doesn't exist, it will be created. In either case, its properties are
    restored according to the obj's properties. If this is a tree object,
    all entries within it are also restored recursively.

    This is usually called from Repository.restore() and is tightly integrated
    with the Repository class. It lives in its own module for organizational
    reasons.

    :type repo: backathon.repository.Repository
    :param objid: The object ID of the object to restore
    :type path: str|pathlib.Path
    :param key: The key to decrypt files if decryption was enabled
    :type key: None | nacl.public.PrivateKey

    Many kinds of errors can occur during a restore, as repository and local
    cache data is read in, parsed, and cross referenced with other local and
    remote data. Corruptions and inconsistencies in local data may be fixed
    by rebuilding the local cache, assuming the remote repository is still
    good. There could also be corruptions and inconsistencies in remote
    repository data, and errors writing to the local filesystem.
    All errors are logged to the backathon.restore logger, and the restore
    will continue restoring as much as it can. Callers should watch log
    entries at the WARNING level and higher for messages about files that
    could not be restored entirely.

    Any exceptions raised from this function indicate a bug. The philosophy of
    this function is to restore as much as possible and log anything that
    couldn't be restored.

    """
    # Important: if you print or log an error involving the path, pass it
    # through pathstr() first to sanitize any undecodable unicode surrogates
    path = pathlib.Path(path)

    try:
        payload = repo.get_object(objid, key)
    except CorruptedRepository as e:
        logger.error(
            "Can't restore {}: {}".format(
                pathstr(path),
                e,
            )
        )
        return
    payload_items = unpack_payload(payload)

    try:
        obj_type = next(payload_items)
        obj_info = next(payload_items)
        obj_contents = next(payload_items)
    except umsgpack.UnpackException:
        # If the object was downloaded, decrypted, decompressed, and its hash
        # validated, but then we get an error here with the msgpack payload,
        # that's got to be either a bug or a malicious upload from an actor that
        # has the encryption key
        logger.error(
            "Can't restore {}: Object {} has an invalid payload. "
            "This may be a bug.".format(pathstr(path), objid)
        )
        return

    if obj_type == "inode":
        if path.exists() and not path.is_file():
            logger.error(
                "Can't restore path {}: it already exists but isn't "
                "a file".format(pathstr(path))
            )
            return
        logger.info("Restoring file {}".format(pathstr(path)))

        obj_payload_type, obj_payload_contents = obj_contents

        try:
            with path.open("wb") as fileout:
                if obj_payload_type == "chunklist":
                    for pos, chunk_id in obj_payload_contents:
                        try:
                            blob_payload = unpack_payload(repo.get_object(chunk_id, key))
                        except CorruptedRepository as e:
                            logger.error(
                                "Could not restore chunk of {} at byte {}: "
                                "{}".format(pathstr(path), pos, e)
                            )
                            continue

                        try:
                            blob_type = next(blob_payload)
                            blob_contents = next(blob_payload)
                        except umsgpack.UnpackException:
                            logger.error(
                                "Could not restore chunk of {} at byte {}: "
                                "invalid or corrupted data".format(pathstr(path), pos)
                            )
                            continue

                        if blob_type != "blob":
                            logger.error(
                                "Could not restore chunk of {} at byte {}: object of "
                                "type blob expected".format(pathstr(path), pos)
                            )
                            continue

                        fileout.seek(pos)
                        fileout.write(blob_contents)
                elif obj_payload_type == "immediate":
                    assert isinstance(obj_payload_contents, bytes)
                    fileout.write(obj_payload_contents)

                else:
                    raise AssertionError("Invalid inode payload type")

        except OSError as e:
            logger.error("Error writing {}: {}".format(pathstr(path), e))
            return

        _set_file_properties(path, obj_info)

    elif obj_type == "tree":
        if path.exists() and not path.is_dir():
            logger.error(
                "Can't restore path {}: it already exists but isn't "
                "a directory".format(pathstr(path))
            )
            return

        if not path.exists():
            try:
                path.mkdir(mode=obj_info["mode"])
            except OSError as e:
                logger.error("Could not make directory {}: {}".format(pathstr(path), e))
                return

        _set_file_properties(path, obj_info)

        for name, child_objid in obj_contents:
            name = os.fsdecode(name)

            restore_item(repo, child_objid, path / name, key)

    elif obj_type == "symlink":
        try:
            os.symlink(obj_contents, path)
        except OSError as e:
            logger.error("Could not create symlink at {}: {}".format(path, e))
        else:
            # Custom set-file-properties code that only attempts to do so if
            # the platform supports the follow_symlinks param
            if os.chown in os.supports_follow_symlinks:
                try:
                    os.chown(
                        str(path), obj_info["uid"], obj_info["gid"], follow_symlinks=False
                    )
                except OSError as e:
                    logger.warning("Could not chown {}: {}".format(pathstr(path), e))
            if os.chmod in os.supports_follow_symlinks:
                try:
                    os.chmod(str(path), obj_info["mode"], follow_symlinks=False)
                except OSError as e:
                    logger.warning("Could not chmod {}: {}".format(pathstr(path), e))
            if os.utime in os.supports_follow_symlinks:
                try:
                    os.utime(
                        str(path),
                        ns=(obj_info["atime"], obj_info["mtime"]),
                        follow_symlinks=False,
                    )
                except OSError as e:
                    logger.warning(
                        "Could not set mtime on {}: {}".format(pathstr(path), e)
                    )

    else:
        raise NotImplementedError(
            "Restore not implemented for {} " "object type".format(obj_type)
        )


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


def unpack_payload(payload):
    """Returns an iterator over a payload, iterating over the msgpacked
    objects within

    :param payload: A byte-like object

    """
    buf = util.BytesReader(payload)
    try:
        while True:
            try:
                yield umsgpack.unpack(buf)
            except umsgpack.InsufficientDataException:
                return
    finally:
        buf.close()
