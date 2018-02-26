import pathlib
import logging
import os

from umsgpack import UnpackException

from gbackup import models
from gbackup.datastore import default_datastore
from gbackup.exceptions import CorruptedRepository

logger = logging.getLogger("gbackup.restore")

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
            path, e
        ))
    try:
        os.chmod(str(path), obj_info['mode'])
    except OSError as e:
        logger.warning("Could not chmod {}: {}".format(
            path, e
        ))
    try:
        os.utime(str(path), ns=(obj_info['atime'], obj_info['mtime']))
    except OSError as e:
        logger.warning("Could not set mtime on {}: {}".format(
            path, e
        ))

def restore_item(obj, path):
    """Restore the given object to the given path

    The last component of path is the item we're restoring. If it
    doesn't exist, it will be created. In either case, its properties are
    restored according to the obj's properties. If this is a tree object,
    all entries within it are also restored recursively.

    :type obj: models.Object
    :type path: str|pathlib.Path

    Many kinds of errors can occur during a restore, as repository and local
    cache data is read in, parsed, and cross referenced with other local and
    remote data. Corruptions and inconsistencies in local data may be fixed
    by rebuilding the local cache, assuming the remote repository is still
    good. There could also be corruptions and inconsistencies in remote
    repository data, and errors writing to the local filesystem.
    All errors are logged to the gbackup.restore logger, and the restore
    will continue restoring as much as it can. Callers should watch log
    entries at the WARNING level and higher for messages about files that
    could not be restored entirely.

    Any exceptions raised from this function indicate a bug. The philosophy of
    this function is to restore as much as possible and log anything that
    couldn't be restored.

    """
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
            path, obj.objid
        ))
        return

    if obj_type == "inode":
        if path.exists() and not path.is_file():
            logger.error("Can't restore path {}: it already exists but isn't "
                         "a file".format(path))
            return
        logger.info("Restoring file {}".format(path))

        try:
            with path.open("wb") as fileout:
                for pos, chunk_id in obj_contents:

                    try:
                        blob_payload = models.Object.unpack_payload(
                            default_datastore.get_object(chunk_id).read()
                        )
                    except CorruptedRepository as e:
                        logger.error("Could not restore chunk of {} at byte {}: "
                                     "{}".format(
                            path, pos, e
                        ))
                        continue

                    try:
                        blob_type = next(blob_payload)
                        blob_contents = next(blob_payload)
                    except UnpackException:
                        logger.error(
                            "Could not restore chunk of {} at byte {}: "
                            "invalid or corrupted data".format(
                                path, pos
                            )
                        )
                        continue

                    if blob_type != "blob":
                        logger.error(
                            "Could not restore chunk of {} at byte {}: object of "
                            "type blob expected".format(
                                path, pos
                            )
                        )
                        continue

                    fileout.seek(pos)
                    fileout.write(blob_contents)

        except OSError as e:
            logger.error("Error writing {}: {}".format(
                path, e
            ))
            return

        _set_file_properties(path, obj_info)

    elif obj_type == "tree":
        if path.exists() and not path.is_dir():
            logger.error("Can't restore path {}: it already exists but isn't "
                         "a directory".format(path))
            return

        if not path.exists():
            try:
                path.mkdir(mode=obj_info['mode'])
            except OSError as e:
                logger.error("Could not make directory {}: {}".format(
                    path, e
                ))
                return

        _set_file_properties(path, obj_info)

        for name, objid in obj_contents:
            name = os.fsdecode(name)
            try:
                childobj = models.Object.objects.get(objid=objid)
            except models.Object.DoesNotExist:
                logger.error("Could not restore {}: referenced object does "
                             "not exist in the local cache. Rebuilding the "
                             "local cache may help fix this problem".format(
                    path / name
                ))
                return

            restore_item(childobj, path / name)

    else:
        raise NotImplementedError("Restore not implemented for {} "
                                  "object type".format(obj_type))

