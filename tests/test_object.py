import datetime

import pytz

from django.db import IntegrityError
from django.db.transaction import atomic
from django.test import TransactionTestCase

from backathon import models
from .base import TestBase

class TestObject(TestBase, TransactionTestCase):
    """Tests various functionality of the Object class"""

    def _insert_objects(self, *objects):
        """Insert a set of objects into the Object table

        Each object is a tuple of (objid, [children])

        Callers must be careful to avoid reference loops in the object
        hierarchy, as that is not a valid object tree.
        """
        # Since SQLite has deferrable foreign key constraints, we can insert
        # references to rows that don't exist yet as long as they exist when
        # the transaction is committed.
        with atomic(using=self.db):
            for objid, children in objects:
                if isinstance(objid, str):
                    objid = objid.encode("ASCII")
                obj = self.object.create(
                    objid=objid,
                )
                self.obj_relation.bulk_create([
                    models.ObjectRelation(
                        parent=obj,
                        child_id=c.encode("ASCII") if isinstance(c,str) else c
                    ) for c in children
                ])

    def assert_objects(self, objs, roots=None, no_extras=True):
        """Asserts that the given hierarchy exists in the database and that
        no other objects exist in the database

        """
        if roots is None:
            roots = self.object.filter(
                parents__isnull=True,
            )

        rootmap = {r.objid: r for r in roots}
        for name, children in objs.items():
            obj = rootmap.pop(name.encode("ASCII") if isinstance(name, str) else name)
            self.assert_objects(
                children,
                obj.children.all(),
            )

        if no_extras:
            self.assertDictEqual(
                rootmap,
                {},
                "Unexpected object found"
            )

    def test_foreign_key_constraints(self):
        """Tests that the foreign key constraints are enforced by sqlite

        This mostly just tests the _insert_objects() method, which should be
        able to insert its objects in any order due to sqlite's deferrable
        foreign keys, but we want to make sure they're still enforced when a
        transaction is comitted. This also requires this test case to be a
        TransactionalTestCase.
        """
        self.assertRaises(
            IntegrityError,
            self._insert_objects,
            ("A", ["B", "C"]),
            ("B", []),
        )

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
        self.snapshot.create(root_id=b"A",
                             date=datetime.datetime(2018, 1,1, tzinfo=pytz.UTC))
        self.snapshot.create(root_id=b"G",
                             date=datetime.datetime(2018, 1,1, tzinfo=pytz.UTC))

        self.assertEqual(
            10,
            self.object.count()
        )
        self.assert_objects({
            'A': {
                "B": {
                    "D": {"F": {}},
                    "E": {},
                },
                "C": {}
            },
            "G": {
                "B": {
                    "D": {"F": {}},
                    "E": {},
                },
                "H": {
                    "I": {"F": {}},
                    "J": {},
                }
            }
        })

        # No garbage expected yet
        self.assertSetEqual(
            set(models.Object.collect_garbage(using=self.db)),
            set(),
        )

        # Remove snapshot A
        self.snapshot.filter(root_id=b"A").delete()

        garbage = list(models.Object.collect_garbage(using=self.db))
        # Garbage collection is stochastic, but should never collect
        # non-garbage
        self.assertTrue(
            {g.objid for g in garbage}.issubset(
                {b'A', b'C'}
            ),
        )

        with atomic():
            for g in garbage:
                g.delete()

        self.assert_objects({
            "G": {
                "B": {
                    "D": {"F": {}},
                    "E": {},
                },
                "H": {
                    "I": {"F": {}},
                    "J": {},
                }
            }
        }, no_extras=False)

    def test_collect_garbage_2(self):
        N = 100
        for root in ["A", "B"]:
            obj = self.object.create(
                objid="root_{}".format(root).encode("ASCII")
            )
            for i in range(N):
                obj2 = self.object.create(
                    objid="obj_{}_{}".format(root,i).encode("ASCII")
                )
                self.obj_relation.create(
                    parent=obj,
                    child=obj2
                )
                obj = obj2


        self.snapshot.create(root_id=b"root_A",
                             date=datetime.datetime(2018, 1,1, tzinfo=pytz.UTC))
        self.snapshot.create(root_id=b"root_B",
                             date=datetime.datetime(2018, 1,1, tzinfo=pytz.UTC))

        self.assertEqual(
            N*2 + 2,
            self.object.count(),
        )
        garbage = list(models.Object.collect_garbage(self.db))
        self.assertListEqual(
            [],
            garbage
        )

        self.snapshot.get(root_id=b"root_B").delete()
        garbage = list(models.Object.collect_garbage(self.db))
        self.assertLessEqual(
            len(garbage),
            N+1,
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
            self.assertTrue(
                objid.startswith("obj_B") or objid == "root_B"
            )
