import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Callable

from backathon.db import Database
from backathon.encryption.base import EncrypterBase
from backathon.exceptions import CorruptedRepository
from backathon.models import Object, ObjIDType, Snapshot
from backathon.repoobject import RawPayload, make_object_path
from backathon.repository import make_snapshot_putter
from backathon.storage.base import StorageBase

logger = logging.getLogger("backathon.recover")


@dataclass
class RebuildProgress:
    # seen_* - items that exist in the remote repository
    # new_* - items in the remote repo that we didn't know about
    # mismatched_* - items that we have a local copy of, but its data doesn't match the remote repo
    # missing_* - items we found a reference to, but the object doesn't exist in the repo
    # corrupt_* - items in the remote repository that were found to be corrupted
    seen_snapshots: int = 0
    new_snapshots: int = 0

    seen_objects: int = 0
    new_objects: int = 0
    mismatched_objects: int = 0
    missing_objects: int = 0
    corrupt_objects: int = 0


async def recover_encryption(storage: StorageBase) -> dict:
    """Downloads the encryption recovery data"""
    data = await asyncio.to_thread(storage.get_object, "backathon.json")
    marker_data = json.load(data.stream)
    if not marker_data.get("name") == "Backathon Repository":
        raise CorruptedRepository("This does not look like a Backathon repository")

    if marker_data.get("encryption") is None:
        raise CorruptedRepository("Missing encryption recovery data")

    return marker_data["encryption"]


async def rebuild_object_index(
    db: Database,
    encrypter: EncrypterBase,
    storage: StorageBase,
    progress_callback: Callable[[RebuildProgress], None],
):
    progress = RebuildProgress()
    put_snapshot = make_snapshot_putter(db, encrypter, storage)

    # First, get all the snapshot objects
    snapshots: list[Snapshot] = []
    for fname in storage.list_dir("snapshots"):
        path = "snapshots/" + fname
        data = storage.get_object(path)
        decrypted = encrypter.decrypt(data.stream)
        snapshot = Snapshot.model_validate_json(bytes(decrypted))
        snapshots.append(snapshot)
        progress.seen_snapshots += 1

    progress_callback(progress)

    # Check if each one is already in the database. If not, add it.
    existing_snapshots = list(db.query(Snapshot, "SELECT * FROM snapshots"))
    for snapshot in snapshots:
        found = any(snapshot == s for s in existing_snapshots)
        if not found:
            logger.info("Found new snapshot: %s - %s", snapshot.timestamp, snapshot.path)
            put_snapshot(snapshot)
            progress.new_snapshots += 1

    progress_callback(progress)

    # Start the process of traversing all the objects starting at each snapshot root
    with db.atomic(), db.cursor() as cursor:
        # We do a depth first search, to save on memory
        # Items we've checked go into this temporary table, since it can grow very large
        # and we don't want to take too much memory
        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS checked_objects (objid BLOB primary key)"
        )
        # noinspection SqlWithoutWhere
        cursor.execute("DELETE FROM checked_objects")

        to_check: list[ObjIDType] = [snapshot.root for snapshot in snapshots]
        while to_check:
            objid = to_check.pop()

            try:
                downloaded_obj = await asyncio.to_thread(
                    storage.get_object, make_object_path(objid)
                )
            except FileNotFoundError:
                logger.warning("Missing object: %s", objid.hex())
                progress.missing_objects += 1
                continue

            try:
                raw_payload = RawPayload.from_encrypted_payload(
                    objid, downloaded_obj.stream, encrypter
                )
            except CorruptedRepository:
                logger.warning("Object corrupted: %s", objid.hex())
                progress.corrupt_objects += 1
                continue

            # See if this object is in our local database
            db_object = next(
                db.query(Object, "SELECT * FROM objects WHERE objid=?", (objid,)), None
            )

            # This is what the object /should/ look like according to what we downloaded
            remote_obj = Object(
                objid=objid,
                type=raw_payload.header.type,
                uploaded_size=downloaded_obj.size,
                file_size=raw_payload.header.file_size,
                last_modified_time=raw_payload.header.last_modified_time,
                sha1=downloaded_obj.sha1,
            )

            if db_object is None:
                # No object in the local database was found
                # Add it
                progress.new_objects += 1
                remote_obj.add_to_database(
                    db,
                    raw_payload.get_children(),
                )
            elif db_object != remote_obj:
                # An object was in the local database, but it doesn't match what we read from
                # the remote repository
                logger.warning(
                    "Local data mismatch. Updating local metadata for %s", objid.hex()
                )
                progress.mismatched_objects += 1
                cursor.execute("DELETE FROM objects WHERE objid=?", (objid,))
                remote_obj.add_to_database(
                    db,
                    raw_payload.get_children(),
                )

            # TODO: Get the object's references and add them to the to_search stack
            # TODO: Add object references to the object_relations table
            # Note: we don't have to traverse BLOB types, just make note of them in the database,
            # although we should still fetch those files' metadata

        # TODO: scan object_relations table for any missing references and delete them
        # (otherwise the transaction won't commit due to missing fkey references)
