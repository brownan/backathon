import datetime
from typing import Iterable

from backathon import garbage, models
from tests.base import BackathonTest

UTC = datetime.timezone.utc

ExpectedObjects = dict[str, "ExpectedObjects"]


class TestGarbage(BackathonTest):
    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo()
        self.db = self.back.db

    def find_garbage(self) -> list[models.Object]:
        bloom = garbage._build_filter(self.db)
        return list(garbage._iter_garbage(self.db, bloom))

    def _insert_objects(self, *objs: tuple[str, Iterable[str]]):
        with self.db.atomic(), self.db.cursor() as cursor:
            for objid, obj_rels in objs:
                cursor.execute(
                    "INSERT INTO objects (objid, type) VALUES (?, 'tree')",
                    (objid.encode(),),
                )
                for obj_rel in obj_rels:
                    cursor.execute(
                        "INSERT INTO object_relations (parent, child) VALUES (?,?)",
                        (objid.encode(), obj_rel.encode()),
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
        N = 100
        for root in ["A", "B"]:
            obj = self.object.create(objid="root_{}".format(root).encode("ASCII"))
            for i in range(N):
                obj2 = self.object.create(
                    objid="obj_{}_{}".format(root, i).encode("ASCII")
                )
                self.obj_relation.create(parent=obj, child=obj2)
                obj = obj2

        self.snapshot.create(
            root_id=b"root_A", date=datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC)
        )
        self.snapshot.create(
            root_id=b"root_B", date=datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC)
        )

        self.assertEqual(
            N * 2 + 2,
            self.object.count(),
        )
        garbage = list(self.find_garbage())
        self.assertListEqual([], garbage)

        self.snapshot.get(root_id=b"root_B").delete()
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
            objid = obj.objid.decode("ASCII")
            self.assertTrue(objid.startswith("obj_B") or objid == "root_B")
