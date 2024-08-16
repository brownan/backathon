import datetime
from typing import Iterable, Iterator

import backathon.garbage
from backathon import models
from tests.base import BackathonTest

UTC = datetime.timezone.utc

ExpectedObjects = dict[str, "ExpectedObjects"]


class TestGarbage(BackathonTest):
    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo()
        self.db = self.back.db

    def find_garbage(self) -> Iterator[models.Object]:
        bloom = backathon.garbage._build_filter(self.db)
        return backathon.garbage._iter_garbage(self.db, bloom)

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

    def test_collect_garbage(self):
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

    def test_collect_garbage_2(self):
        cursor = self.db.cursor()
        N = 1000
        for root in ["A", "B"]:
            objid = f"root_{root}"
            self._create_obj(objid)
            for i in range(N):
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

        self.assert_object_count(self.back, N * 2 + 2)
        garbage = list(self.find_garbage())
        self.assertListEqual([], garbage)

        with self.db.cursor() as cursor:
            cursor.execute("DELETE FROM snapshots WHERE root=?", (b"root_B",))
        garbage = list(self.find_garbage())
        self.assertLessEqual(
            len(garbage),
            N + 1,
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
