import pathlib
import logging
import os

import umsgpack

from .exceptions import CorruptedRepository
from . import util

logger = logging.getLogger("backathon.restore")


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


def _set_file_properties(path, obj_info):
    """Sets the file properties of the given path

    :type path: pathlib.Path
    :type obj_info: dict

    Sets: owner, group, mode, atime, mtime
    """
    try:
        os.chown(str(path), obj_info["uid"], obj_info["gid"])
    except OSError as e:
        logger.warning("Could not chown {}: {}".format(pathstr(path), e))
    try:
        os.chmod(str(path), obj_info["mode"])
    except OSError as e:
        logger.warning("Could not chmod {}: {}".format(pathstr(path), e))
    try:
        os.utime(str(path), ns=(obj_info["atime"], obj_info["mtime"]))
    except OSError as e:
        logger.warning("Could not set mtime on {}: {}".format(pathstr(path), e))


def pathstr(p):
    """Returns the path string suitable for printing or logging"""
    return os.fsencode(str(p)).decode("UTF-8", errors="replace")


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
