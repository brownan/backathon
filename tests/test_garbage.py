import datetime

import pytz
from django.db.transaction import atomic

import backathon.garbage
from tests.base import TestBase


class TestGarbage(TestBase):
    def setUp(self):
        super().setUp()
        self.gc = backathon.garbage.GarbageCollector(self.repo)

    def find_garbage(self):
        self.gc.build_filter()
        yield from self.gc._iter_garbage()

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
        self.snapshot.create(
            root_id=b"A", date=datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC)
        )
        self.snapshot.create(
            root_id=b"G", date=datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC)
        )

        self.assertEqual(10, self.object.count())
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
        self.snapshot.filter(root_id=b"A").delete()

        garbage = list(self.find_garbage())
        # Garbage collection is stochastic, but should never collect
        # non-garbage
        self.assertTrue(
            {g.objid for g in garbage}.issubset({b"A", b"C"}),
        )

        with atomic():
            for g in garbage:
                g.delete()

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
