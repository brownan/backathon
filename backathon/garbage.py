import math
import random
import logging

from django.db import connections

from . import models

logger = logging.getLogger("backathon.garbage")


class GarbageCollector:
    """Finds garbage objects in the Object table

    The approach implemented is to construct a simple bloom filter
    such that we collect about 95% of all garbage objects.

    This approach was chosen because it should be quick (2 passes over
    the database, where the first pass is read-only) and memory
    efficient (uses about 760k for a million objects in the table)

    One alternative is to perform a query for objects with no
    references, which is quick due to indices on the
    object_relations table, but requires many queries in a loop
    to collect all garbage. It's theoretically possible to do this with
    a single recursive query, but that requires holding the entire
    garbage set in memory, which could get big.

    Another approach is a traditional garbage collection strategy such as
    mark-and-sweep. Problem with that is it would involve writing each
    row on the first pass, which is a lot more IO and would probably be
    slower.
    """

    def __init__(self, repo, progress=None):
        """

        :type repo: backathon.repository.Repository
        :type progress: ProgressIndicator
        """
        self.repo = repo
        self.db = repo.db
        self.bloom = None
        self.m = None
        self.hashes = None
        self.progress = progress or ProgressIndicator()

    def build_filter(self):
        """Builds the bloom filter"""
        num_objects = models.Object.objects.using(self.db).all().count()

        # m - number of bits in the filter. Depends on num_objects
        # k - number of hash functions needed. Should be 4 for p=0.05
        p = 0.05
        m = int(
            math.ceil(
                (num_objects * math.log(p)) / math.log(1 / math.pow(2, math.log(2)))
            )
        )
        k = 4  # = int(round(math.log(2) * m / num_objects))

        arr_size = int(math.ceil(m / 8))
        bloom = bytearray(arr_size)

        # The "hash" functions will just be a random number that will be
        # xor'd with the object IDs. Using a different random int each time
        # also guards against false positives from collisions happening from
        # the same two objects each run.
        r = random.SystemRandom()
        hashes = [r.getrandbits(256) for _ in range(k)]

        # This query iterates over all the reachable objects by walking the
        # hierarchy formed using the Snapshot table as the roots and
        # traversing the links in the ManyToMany relation.
        query = """
            WITH RECURSIVE reachable(id) AS (
                SELECT root_id FROM snapshots
                UNION ALL
                SELECT child_id FROM object_relations
                INNER JOIN reachable ON reachable.id=parent_id
            ) SELECT id FROM reachable
            """
        with connections[self.db].cursor() as c:
            c.execute(query)
            for row in c:
                objid_int = int.from_bytes(row[0], "little")

                for h in hashes:
                    h ^= objid_int
                    h %= m
                    bytepos, bitpos = divmod(h, 8)
                    bloom[bytepos] |= 1 << bitpos

                self.progress.build_filter_progress()

        self.bloom = bloom
        self.m = m
        self.hashes = hashes

    def _iter_garbage(self):
        """Iterates over garbage objects

        Callers should take care to atomically delete objects in the remote
        storage backend along with rows in the Object table. It's more
        important to delete the rows, however, because if a row exists
        without a backing object, that can corrupt future backups that may
        try to reference that object. Leaving an un-referenced object on the
        backing store doesn't hurt anything except by taking up space.

        """
        hashes = self.hashes

        def hash_match(h, objid, bloom=self.bloom, m=self.m):
            h ^= objid
            h %= m
            bytepos, bitpos = divmod(h, 8)
            return bloom[bytepos] & (1 << bitpos)

        # Now we can iterate over all objects. If an object does not appear
        # in the bloom filter, we can guarantee it's not reachable.
        for obj in models.Object.objects.using(self.db).all().iterator():
            objid = int.from_bytes(obj.objid, "little")

            if not all(hash_match(h, objid) for h in hashes):
                yield obj

    def delete_garbage(self):
        """Deletes garbage according to the filter built in a previous call
        to build_filter()

        Callers should take care to hold the SQLite reserved lock between
        calls to build_filter() and delete_garbage() if there's a chance of
        any other connections manipulating the Object table simultaneously.
        Otherwise, new objects or references could be created between the calls.

        In order to preserve consistency between the local db and the
        repository, this method swallows all exceptions and logs them to the
        backathon.garbage logger. This way the caller's DB transaction is not
        rolled back from any exceptions.

        Returns the number of objects deleted and the number of bytes deleted
        """
        n = 0
        s = 0
        try:
            for obj in self._iter_garbage():  # type: models.Object
                self.repo.delete_object(obj)
                n += 1
                s += obj.uploaded_size
                self.progress.delete_progress(obj.uploaded_size)
        except KeyboardInterrupt:
            self.progress.close()
            logger.info("Ctrl-C caught, canceling garbage collection")
        except Exception:
            self.progress.close()
            logger.critical("Error in garbage collection", exc_info=True)
        finally:
            self.progress.close()

        return n, s


class ProgressIndicator:
    def build_filter_progress(self):
        """Called for each step of building the filter"""
        pass

    def delete_progress(self, s):
        """Called for each object deleted"""
        pass

    def close(self):
        pass
