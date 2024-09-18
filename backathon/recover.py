import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Callable

from backathon.db import Database
from backathon.encryption.base import EncrypterBase
from backathon.exceptions import CorruptedRepository
from backathon.models import Object, ObjectRelation, ObjIDType, Snapshot
from backathon.repoobject import RawPayload, make_object_path
from backathon.storage.base import StorageBase

logger = logging.getLogger("backathon.recover")


@dataclass
class RebuildProgress:
    # seen_* - items that exist in the remote repository
    # new_* - items in the remote repo that we didn't know about
    # mismatched_* - items that we have a local copy of, but its data doesn't match the remote repo
    # missing_* - items we found a reference to, but the object doesn't exist in the repo
    # corrupt_* - items in the remote repository that were found to be corrupted
    # extra_* - items in our local database that shouldn't be there
    seen_snapshots: int = 0
    new_snapshots: int = 0

    seen_objects: int = 0
    new_objects: int = 0
    mismatched_objects: int = 0
    missing_objects: int = 0
    corrupt_objects: int = 0

    missing_relations: int = 0
    extra_relations: int = 0


async def init_local_from_remote(
    storage: StorageBase, password_callback: Callable[[], str]
) -> Database:
    raise NotImplementedError


async def recover_encryption(storage: StorageBase) -> dict:
    """Downloads the encryption recovery data"""
    data = await asyncio.to_thread(storage.get_object, "backathon.json")
    marker_data = json.load(data.stream)
    if not marker_data.get("name") == "Backathon Repository":
        raise CorruptedRepository("This does not look like a Backathon repository")

    if marker_data.get("encryption") is None:
        raise CorruptedRepository("Missing encryption recovery data")

    return marker_data["encryption"]


async def repair_object_index(
    db: Database,
    storage: StorageBase,
    encrypter: EncrypterBase,
    progress_callback: Callable[[RebuildProgress], None] | None = None,
):
    progress = RebuildProgress()

    # Get all the snapshot objects in the remote repo
    remote_snapshots: list[Snapshot] = []
    for fname in storage.list_dir("snapshots"):
        path = "snapshots/" + fname
        with storage.get_object(path) as data:
            decrypted = encrypter.decrypt(data.stream)
        snapshot = Snapshot.model_validate_json(bytes(decrypted))
        remote_snapshots.append(snapshot)
        progress.seen_snapshots += 1

    if progress_callback:
        progress_callback(progress)

    # Note: we take advantage of sqlite's deferred foreign keys within this atomic block.
    with db.atomic(), CheckedObjects(db) as checked_objects, db.cursor() as cursor:
        # Check if each one is already in the database. If not, add it.
        local_snapshots = list(db.query(Snapshot, "SELECT * FROM snapshots"))
        for snapshot in remote_snapshots:
            found = any(snapshot == s for s in local_snapshots)
            if not found:
                logger.info(
                    "Found new snapshot: %s - %s", snapshot.timestamp, snapshot.path
                )
                progress.new_snapshots += 1
                cursor.execute(
                    "INSERT INTO snapshots (path, root, timestamp) VALUES (?,?,?)",
                    (snapshot.path, snapshot.root, snapshot.timestamp),
                )

        # Start the process of traversing all the objects starting at each snapshot root
        # We do a depth first search, to save on memory
        # Items we've checked go into this temporary table, since it can grow very large
        # and we don't want to take too much memory. Sqlite will spill temporary tables to
        # disk if they exceed its page cache.
        to_check: list[ObjIDType] = [snapshot.root for snapshot in remote_snapshots]
        while to_check:
            objid = to_check.pop()

            if progress_callback:
                progress_callback(progress)

            try:
                downloaded_obj = await asyncio.to_thread(
                    storage.get_object, make_object_path(objid)
                )
            except FileNotFoundError:
                logger.warning("Missing object: %s", objid.hex())
                progress.missing_objects += 1
                cursor.execute("DELETE FROM objects WHERE objid=?", (objid,))
                continue

            try:
                raw_payload = RawPayload.from_encrypted_payload(
                    objid, downloaded_obj.stream, encrypter
                )
            except CorruptedRepository:
                logger.warning("Object corrupted: %s", objid.hex())
                progress.corrupt_objects += 1
                cursor.execute("DELETE FROM objects WHERE objid=?", (objid,))
                continue

            # See if this object is in our local database
            local_object = next(
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

            if local_object is None:
                # No object in the local database was found
                # Add it
                progress.new_objects += 1
                remote_obj.add_to_database(
                    db,
                    raw_payload.get_children(),
                )
            elif local_object != remote_obj:
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

            # Next phase: check this object's relations
            local_relations = {
                (r.child, r.name)
                for r in db.query(
                    ObjectRelation,
                    "SELECT * FROM object_relations WHERE parent=?",
                    (objid,),
                )
            }

            # Parse the object's header to see what other objects it references
            remote_relations = set()
            for _, child_objid, name in raw_payload.get_children():
                # Check that each expected relation is in our local db and that the
                # name is correct
                key = (child_objid, name)
                remote_relations.add(key)
                if key not in local_relations:
                    progress.missing_relations += 1
                    cursor.execute(
                        "INSERT INTO object_relations (parent, child, name) VALUES (?,?,?)",
                        (objid, child_objid, name),
                    )

                # Add this child to the queue of objects to check
                # We only need to check each object once, even if it's referenced multiple
                # times. If we add a reference to an object and later we discover it
                # doesn't exist, we'll discover that in the next phase
                if child_objid in checked_objects:
                    continue
                checked_objects.add(child_objid)
                to_check.append(child_objid)

            # Check if any relations in our local database aren't supposed to be
            # there
            for child_objid, name in local_relations - remote_relations:
                progress.extra_relations += 1
                cursor.execute(
                    """DELETE FROM object_relations WHERE parent=? AND child=? AND name=?""",
                    (objid, child_objid, name),
                )

        # Scan object_relations table for any missing references and delete them
        # (otherwise the transaction won't commit due to missing fkey references)
        # We've already discovered and noted the missing objects since they would have
        # gone through the to_check queue. So all we need to do here is remove the relations
        # that would error because of missing fkeys.
        cursor.execute(
            """
            DELETE FROM object_relations WHERE child NOT IN
            (SELECT objid FROM objects)
            """
        )

        # Same with snapshots missing their root
        # TODO: log the snapshots that were removed due to missing roots
        cursor.execute(
            """
            DELETE FROM snapshots WHERE root NOT IN
            (SELECT objid FROM objects)
            """
        )

        if progress_callback:
            progress_callback(progress)


class CheckedObjects:
    def __init__(self, db: Database):
        self.db = db

    def __enter__(self):
        if not self.db.conn.in_transaction:
            raise RuntimeError("Not in a transaction")

        with self.db.cursor() as cursor:
            cursor.execute(
                "CREATE TEMP TABLE IF NOT EXISTS checked_objects (objid BLOB primary key)"
            )
            # noinspection SqlWithoutWhere
            cursor.execute("DELETE FROM checked_objects")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.db.cursor() as cursor:
            cursor.execute("DROP TABLE checked_objects")

    def __contains__(self, objid: ObjIDType) -> bool:
        with self.db.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM checked_objects WHERE objid=? LIMIT 1", (objid,)
            )
            return bool(cursor.fetchone())

    def add(self, objid: ObjIDType):
        with self.db.cursor() as cursor:
            cursor.execute("INSERT INTO checked_objects (objid) VALUES (?)", (objid,))
