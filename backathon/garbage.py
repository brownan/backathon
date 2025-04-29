"""
Garbage collection routines

The approach implemented is to construct a simple bloom filter tuned such that we
find and collect about 95% of all garbage objects.

This approach was chosen for two main reasons:
* Only requires 2 linear-time passes over the objects table, where the first pass is
  read-only, and the second pass removes rows
* memory efficient: uses about 760k per million objects in the table


"""
import json
import logging
import math
import random
import threading
from typing import Iterator
from typing import NamedTuple
from typing import Self

from rich import filesize

from backathon import models
from backathon import repoobject
from backathon.db import Database
from backathon.db import batch_fetch_from_cursor
from backathon.storage.base import StorageBase

logger = logging.getLogger("backathon.garbage")


class BloomFilter(NamedTuple):
    """The bloom filter performs a set difference between
    sets of objects in the database's Object table. It is primarily used to
    find unreachable objects, but can also be used to find objects referenced
    by one snapshot but not another.

    The set operation performed is Y - X where:
    * X is the set of all nodes reachable from the snapshot list passed to
      build_filter(), or all snapshots if no explicit snapshot list is given.

    * Y is the set of all nodes reachable from the snapshot list passed to
      iter_unreachable(), or the set of all known objects if a snapshot
      list is not given.

    build_filter() requires a linear time complexity pass over the objects and
    marks all objects reachable from the given snapshot roots in the bloom
    filter.

    iter_unreachable() performs another linear time complexity scan of
    objects in the table, either all objects or objects reachable from the
    given snapshot roots, and yields items not found in the bloom filter.

    As per bloom filter semantics, items returned are guaranteed not to be
    in set X, but not all items in Y - X may be found. The accuracy is
    set by the size of the filter and number of hash functions used. At the
    time of writing this is tuned to 5%, or a 95% accuracy. This is a decent
    compromise between accuracy and memory usage.
    """

    bloom: bytearray
    hashes: list[int]
    m: int

    @classmethod
    def build_filter(
        cls,
        db: Database,
        snapshot_ids: list[int] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> Self:
        """Builds the bloom filter

        cancel_event is a threading.Event object which, when set,
        will cause this function to return early. It interrupts the iteration
        over reachable objects, but not execution within sqlite itself.
        """

        cancel_event = cancel_event or threading.Event()

        with db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM objects")
            num_objects: int = cursor.fetchone()[0] or 0

        # m - number of bits in the filter. Depends on num_objects
        # k - number of hash functions needed. Should be 4 for p=0.05
        p = 0.05
        m = int(
            math.ceil(
                (num_objects * math.log(p)) / math.log(1 / math.pow(2, math.log(2)))
            )
        )
        k = int(round(math.log(2) * m / num_objects))

        arr_size = int(math.ceil(m / 8))
        bloom = bytearray(arr_size)

        # The "hash" functions will just be a random number that will be
        # xor'd with the object IDs. Using a different random int each time
        # also guards against false positives from collisions happening from
        # the same two objects each run.
        r = random.SystemRandom()
        hashes = [r.getrandbits(256) for _ in range(k)]

        with db.cursor() as cursor:
            if snapshot_ids is None:
                cursor.execute("SELECT id FROM snapshots")
                snapshot_ids = [row[0] for row in cursor]

            # This query iterates over all the reachable objects by walking the
            # hierarchy formed using the Snapshot table as the roots and
            # traversing the links in the object_relations table
            query = """
                WITH RECURSIVE reachable(id) AS (
                    SELECT root FROM snapshots WHERE id IN (SELECT value FROM json_each(?))
                    UNION ALL
                    SELECT child FROM object_relations
                    INNER JOIN reachable ON reachable.id=parent
                ) SELECT id FROM reachable
                """
            cursor.execute(query, (json.dumps(snapshot_ids),))
            for row in batch_fetch_from_cursor(cursor):
                objid_int = int.from_bytes(row[0], "little")

                for h in hashes:
                    h ^= objid_int
                    h %= m
                    bytepos, bitpos = divmod(h, 8)
                    bloom[bytepos] |= 1 << bitpos

                if cancel_event.is_set():
                    break

        return cls(bloom=bloom, hashes=hashes, m=m)

    def iter_unreachable(
        self, db: Database, snapshot_ids: list[int] | None = None
    ) -> Iterator[models.Object]:
        """Iterates over unreachable objects"""
        hashes = self.hashes

        def hash_match(h, objid, bloom=self.bloom, m=self.m):
            h ^= objid
            h %= m
            bytepos, bitpos = divmod(h, 8)
            return bloom[bytepos] & (1 << bitpos)

        if snapshot_ids is None:
            query = "SELECT * FROM objects"
            args = ()
        else:
            query = """
                WITH RECURSIVE reachable(id) AS (
                    SELECT root FROM snapshots WHERE id IN (SELECT value FROM json_each(?))
                    UNION ALL
                    SELECT child FROM object_relations
                    INNER JOIN reachable ON reachable.id=parent
                ) SELECT * FROM objects WHERE objid IN reachable
            """
            args = (json.dumps(snapshot_ids),)

        # Now we can iterate over all objects. If an object does not appear
        # in the bloom filter, we can guarantee it's not reachable.
        for obj in db.query(models.Object, query, args):
            objid = int.from_bytes(obj.objid, "little")

            if not all(hash_match(h, objid) for h in hashes):
                yield obj


def collect_garbage(db: Database, storage: StorageBase) -> tuple[int, int]:
    """Collects and deletes garbage from the given database

    Returns the number of objects deleted and the number of bytes deleted
    """
    n = 0
    s = 0
    logger.debug("Garbage collection beginning. Acquiring write lock on database")
    with db.atomic(immediate=True), db.cursor() as cursor:
        # Build the bloom filter
        logger.info("Garbage scan, first pass...")
        bloom_filter = BloomFilter.build_filter(db)

        # Log garbage objects to the garbage table for deletion in the next step
        # We don't want to delete the garbage from the remote repo within this
        # transaction since if there's an error and the transaction can't be committed,
        # the local database will lose track of which objects in the remote repo were
        # deleted and which still exist.
        logger.info("Garbage scan, counting garbage objects")
        for obj in bloom_filter.iter_unreachable(db):
            cursor.execute("DELETE FROM objects WHERE objid=?", (obj.objid,))
            cursor.execute("INSERT INTO garbage (objid) VALUES (?)", (obj.objid,))
            n += 1
            if obj.uploaded_size:
                s += obj.uploaded_size
            logger.log(5, "Found garbage: %r", obj)

        logger.debug("Committing transaction")

    with db.atomic(immediate=True), db.cursor() as cursor:
        logger.info("Found %s objects to delete, totaling %s", n, filesize.decimal(s))
        cursor.execute("SELECT objid FROM garbage")
        for row in batch_fetch_from_cursor(cursor):
            objid = row[0]
            path = repoobject.make_object_path(objid)
            logger.log(5, "Deleting %s", objid.hex())
            storage.delete_object(path)

        # Clear the garbage table. A DELETE FROM statement without a WHERE clause in sqlite
        # efficiently clears the table without visiting each row.
        # noinspection SqlWithoutWhere
        cursor.execute("DELETE FROM garbage")

        return n, s
