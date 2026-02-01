import datetime
import unittest.mock
from typing import Iterable
from typing import Iterator
from typing import cast

import backathon.garbage
from backathon import models
from backathon import repoobject
from backathon.encryption.base import Payload
from backathon.models import ObjIDType
from tests.base import BackathonTest

UTC = datetime.timezone.utc

ExpectedObjects = dict[str, "ExpectedObjects"]


class TestGarbage(BackathonTest):
    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo()
        self.db = self.back.db

    def find_garbage(self) -> Iterator[models.Object]:
        bloom = backathon.garbage.BloomFilter.build_filter(self.db)
        return bloom.iter_unreachable(self.db)

    def _insert_objects(self, *objs: tuple[str, Iterable[str]]):
        with self.db.atomic():
            for objid, obj_rels in objs:
                self._create_obj(objid)
                # Making use of deferred foreign keys, we can create the relations before
                # we create all the objects, as long as everything's in place when the
                # transaction commits
                for obj_rel in obj_rels:
                    self._create_rel(objid, obj_rel)

    def _create_obj(self, objid: str):
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO objects (objid, type) VALUES (?,'tree')",
                (objid.encode(),),
            )

    def _create_rel(self, parent: str, child: str):
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO object_relations (parent, child) VALUES (?,?)",
                (parent.encode(), child.encode()),
            )

    def _create_snapshot(
        self,
        root_id: str,
        date: datetime.datetime,
    ):
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO snapshots (path, root, timestamp) VALUES ('', ?,?)",
                (root_id.encode(), date),
            )

    def assert_objects(
        self,
        objs: ExpectedObjects,
        roots: list[str] | None = None,
        no_extras: bool = True,
    ):
        with self.db.cursor() as cursor:
            if roots is None:
                cursor.execute(
                    """SELECT objid FROM objects
                    WHERE objid NOT IN (
                        SELECT child FROM object_relations
                    )"""
                )
                roots = [row[0].decode() for row in cursor]

            for objid, children in objs.items():
                self.assertIn(objid, roots)
                roots.remove(objid)

                cursor.execute(
                    "SELECT child FROM object_relations WHERE parent=?", (objid.encode(),)
                )
                child_objids = [row[0].decode() for row in cursor]
                self.assert_objects(children, child_objids, no_extras=no_extras)
            if no_extras:
                self.assertEqual([], roots, "Unexpected object found")

    def test_find_garbage(self):
        self._insert_objects(
            # Tree 1
            ("A", ["B", "C"]),
            ("B", ["D", "E"]),
            ("C", []),
            ("D", ["F"]),
            ("E", []),
            ("F", []),
            # Tree 2, shares some of the same objects
            ("G", ["B", "H"]),
            ("H", ["I", "J"]),
            ("I", ["F"]),
            ("J", []),
        )
        self._create_snapshot(root_id="A", date=datetime.datetime(2018, 1, 1, tzinfo=UTC))
        self._create_snapshot(root_id="G", date=datetime.datetime(2018, 1, 1, tzinfo=UTC))

        self.assert_object_count(self.back, 10)
        self.assert_objects(
            {
                "A": {
                    "B": {
                        "D": {"F": {}},
                        "E": {},
                    },
                    "C": {},
                },
                "G": {
                    "B": {
                        "D": {"F": {}},
                        "E": {},
                    },
                    "H": {
                        "I": {"F": {}},
                        "J": {},
                    },
                },
            }
        )

        # No garbage expected yet
        self.assertSetEqual(
            set(self.find_garbage()),
            set(),
        )

        # Remove snapshot A
        with self.db.cursor() as cursor:
            cursor.execute("DELETE FROM snapshots WHERE root=?", (b"A",))

        garbage = list(self.find_garbage())
        # Garbage collection is stochastic, but should never collect
        # non-garbage
        self.assertTrue(
            {g.objid for g in garbage}.issubset({b"A", b"C"}),
        )

        with self.db.atomic(), self.db.cursor() as cursor:
            for g in garbage:
                cursor.execute("DELETE FROM objects WHERE objid=?", (g.objid,))

        self.assert_objects(
            {
                "G": {
                    "B": {
                        "D": {"F": {}},
                        "E": {},
                    },
                    "H": {
                        "I": {"F": {}},
                        "J": {},
                    },
                }
            },
            no_extras=False,
        )

    def _build_test_tree(self):
        n = 1000
        for root in ["A", "B"]:
            objid = f"root_{root}"
            self._create_obj(objid)
            for i in range(n):
                sub_objid = f"obj_{root}_{i}"
                self._create_obj(sub_objid)
                self._create_rel(objid, sub_objid)
                objid = sub_objid

        self._create_snapshot(
            root_id="root_A", date=datetime.datetime(2018, 1, 1, tzinfo=UTC)
        )
        self._create_snapshot(
            root_id="root_B", date=datetime.datetime(2018, 1, 1, tzinfo=UTC)
        )
        self.assert_object_count(self.back, n * 2 + 2)

        # Create objects on the filesystem so collect_garbage() has something to delete
        storage = self.back.get_storage()
        for obj in self.db.query(models.Object, "SELECT * FROM objects"):
            path = repoobject.make_object_path(obj.objid)
            storage.put_object(path, Payload(b"", 0, b""))

        return n

    def test_find_garbage_2(self):
        n = self._build_test_tree()
        garbage = list(self.find_garbage())
        self.assertListEqual([], garbage)

        with self.db.cursor() as cursor:
            cursor.execute("DELETE FROM snapshots WHERE root=?", (b"root_B",))
        garbage = list(self.find_garbage())
        self.assertLessEqual(
            len(garbage),
            n + 1,
        )

        # Assert at least some garbage was collected. The current
        # implementation is probabilistic, and may not collect all the
        # garbage. So just make sure it's getting something.
        self.assertGreater(
            len(garbage),
            1,
        )
        for obj in garbage:
            objid = obj.objid.decode()
            self.assertTrue(objid.startswith("obj_B") or objid == "root_B")

    def test_collect_garbage(self):
        """Tests the collect_garbage() routine's functionality to remove garbage objects from
        the database and filesystem

        """
        n = self._build_test_tree()

        with self.db.cursor() as cursor:
            cursor.execute("DELETE FROM snapshots WHERE root=?", (b"root_B",))

        collected_n, _ = backathon.garbage.collect_garbage(
            self.db, self.back.get_storage()
        )
        self.assertLessEqual(collected_n, n + 1)
        self.assertGreater(collected_n, 1)
        root_A_objs = {b"root_A"}.union(f"obj_A_{i}".encode() for i in range(n))
        root_B_objs = {b"root_B"}.union(f"obj_B_{i}".encode() for i in range(n))
        with self.db.cursor() as cursor:
            cursor.execute("SELECT objid FROM objects")
            all_objs = set(row[0] for row in cursor)

        missing = root_A_objs.difference(all_objs)
        self.assertFalse(
            missing,
            "Objects missing from database:\n" + "\n".join(repr(m) for m in missing),
        )

        # Make sure all root A objects are on the filesystem
        for objid in root_A_objs:
            path = self.repopath(repoobject.make_object_path(cast(ObjIDType, objid)))
            self.assertTrue(path.is_file())

        # Make sure the deleted root B objects were deleted. Some root B objects
        # are expected to remain
        for objid in root_B_objs.difference(all_objs):
            path = self.repopath(repoobject.make_object_path(cast(ObjIDType, objid)))
            self.assertFalse(path.exists(), f"Expected deleted object: {objid.hex()}")

        # The root B objects still in the database must also still be on the filesystem
        for objid in root_B_objs.intersection(all_objs):
            path = self.repopath(repoobject.make_object_path(cast(ObjIDType, objid)))
            self.assertTrue(path.exists())

    def test_collection_interrupted(self):
        """Tests what happens if collect_garbage() is interrupted mid-way through
        deleting filesystem objects

        """

        n = self._build_test_tree()
        with self.db.cursor() as cursor:
            cursor.execute("DELETE FROM snapshots WHERE root=?", (b"root_B",))

        # Mock out and proxy the storage.delete_object() method. When collect_garbage()
        # starts to delete objects, we'll interrupt it mid-way through.

        storage = self.back.get_storage()
        orig_delete_object = storage.delete_object

        class DelInterrupt(Exception):
            pass

        deleted_count = 0

        def new_delete(path):
            nonlocal deleted_count
            deleted_count += 1
            if deleted_count >= n / 2:
                raise DelInterrupt
            orig_delete_object(path)

        with unittest.mock.patch.object(storage, "delete_object", new_delete):
            with self.assertRaises(DelInterrupt):
                backathon.garbage.collect_garbage(self.db, storage)

        root_A_objs = {b"root_A"}.union(f"obj_A_{i}".encode() for i in range(n))
        root_B_objs = {b"root_B"}.union(f"obj_B_{i}".encode() for i in range(n))
        original_objs = root_A_objs | root_B_objs
        all_objs = set(
            obj.objid for obj in self.db.query(models.Object, "SELECT * FROM objects")
        )
        with self.db.cursor() as cursor:
            cursor.execute("SELECT objid FROM garbage")
            garbage_objs = {row[0] for row in cursor}

        self.assertGreaterEqual(
            len(garbage_objs),
            1,
        )

        # Objects removed from the objects table should exactly equal the pending garbage table
        self.assertSetEqual(
            garbage_objs,
            original_objs - all_objs,
        )

        # Running the garbage collection again should remove all pending garbage
        # Even though another garbage collection pass may not find the exact same set of
        # garbage, the previous pending garbage in the table should still get deleted
        # by this next pass
        backathon.garbage.collect_garbage(self.db, storage)

        with self.db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM garbage")
            self.assertEqual(0, cursor.fetchone()[0])

        for objid in garbage_objs:
            path = self.repopath(repoobject.make_object_path(cast(ObjIDType, objid)))
            self.assertFalse(path.exists())
