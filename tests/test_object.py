from django.db import IntegrityError
from django.test import TransactionTestCase

from tests.base import TestBase


class TestObject(TestBase, TransactionTestCase):
    """Tests various functionality of the Object class"""

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
