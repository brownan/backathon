import pathlib
import logging
import os

from umsgpack import UnpackException

from . import models
from .exceptions import CorruptedRepository

logger = logging.getLogger("backathon.restore")

def _set_file_properties(path, obj_info):
    """Sets the file properties of the given path

    :type path: pathlib.Path
    :type obj_info: dict

    Sets: owner, group, mode, atime, mtime
    """
    try:
        os.chown(str(path), obj_info['uid'], obj_info['gid'])
    except OSError as e:
        logger.warning("Could not chown {}: {}".format(
            pathstr(path), e
        ))
    try:
        os.chmod(str(path), obj_info['mode'])
    except OSError as e:
        logger.warning("Could not chmod {}: {}".format(
            pathstr(path), e
        ))
    try:
        os.utime(str(path), ns=(obj_info['atime'], obj_info['mtime']))
    except OSError as e:
        logger.warning("Could not set mtime on {}: {}".format(
            pathstr(path), e
        ))

def pathstr(p):
    """Returns the path string suitable for printing or logging"""
    return os.fsencode(str(p)).decode("UTF-8", errors="replace")

def restore_item(repo, obj, path, key=None):
    """Restore the given object to the given path

    The last component of path is the item we're restoring. If it
    doesn't exist, it will be created. In either case, its properties are
    restored according to the obj's properties. If this is a tree object,
    all entries within it are also restored recursively.

    This is usually called from Repository.restore() and is tightly integrated
    with the Repository class. It lives in its own module for organizational
    reasons.

    :type repo: backathon.repository.Repository
    :type obj: models.Object
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
    assert repo.db == obj._state.db

    # Important: if you print or log an error involving the path, pass it
    # through pathstr() first to sanitize any undecodable unicode surrogates
    path = pathlib.Path(path)

    payload_items = models.Object.unpack_payload(obj.payload)

    try:
        obj_type = next(payload_items)
        obj_info = next(payload_items)
        obj_contents = next(payload_items)
    except UnpackException:
        logger.error("Can't restore {}: Object {} has invalid cached "
                     "data. Rebuilding the local cache may fix this "
                     "problem.".format(
            pathstr(path), obj.objid
        ))
        return

    if obj_type == "inode":
        if path.exists() and not path.is_file():
            logger.error("Can't restore path {}: it already exists but isn't "
                         "a file".format(pathstr(path)))
            return
        logger.info("Restoring file {}".format(pathstr(path)))

        obj_payload_type, obj_payload_contents = obj_contents

        try:
            with path.open("wb") as fileout:
                if obj_payload_type == "chunklist":
                    for pos, chunk_id in obj_payload_contents:

                        try:
                            blob_payload = models.Object.unpack_payload(
                                repo.get_object(chunk_id, key)
                            )
                        except CorruptedRepository as e:
                            logger.error("Could not restore chunk of {} at byte {}: "
                                         "{}".format(
                                pathstr(path), pos, e
                            ))
                            continue

                        try:
                            blob_type = next(blob_payload)
                            blob_contents = next(blob_payload)
                        except UnpackException:
                            logger.error(
                                "Could not restore chunk of {} at byte {}: "
                                "invalid or corrupted data".format(
                                    pathstr(path), pos
                                )
                            )
                            continue

                        if blob_type != "blob":
                            logger.error(
                                "Could not restore chunk of {} at byte {}: object of "
                                "type blob expected".format(
                                    pathstr(path), pos
                                )
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
            logger.error("Error writing {}: {}".format(
                pathstr(path), e
            ))
            return

        _set_file_properties(path, obj_info)

    elif obj_type == "tree":
        if path.exists() and not path.is_dir():
            logger.error("Can't restore path {}: it already exists but isn't "
                         "a directory".format(pathstr(path)))
            return

        if not path.exists():
            try:
                path.mkdir(mode=obj_info['mode'])
            except OSError as e:
                logger.error("Could not make directory {}: {}".format(
                    pathstr(path), e
                ))
                return

        _set_file_properties(path, obj_info)

        for name, objid in obj_contents:
            name = os.fsdecode(name)
            try:
                childobj = models.Object.objects.using(repo.db).get(objid=objid)
            except models.Object.DoesNotExist:
                logger.error("Could not restore {}: referenced object does "
                             "not exist in the local cache. Rebuilding the "
                             "local cache may help fix this problem".format(
                    pathstr(path / name)
                ))
                return

            restore_item(repo, childobj, path / name, key)

    else:
        raise NotImplementedError("Restore not implemented for {} "
                                  "object type".format(obj_type))

