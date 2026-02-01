"""Tests related to the Database class"""

import sqlite3

from tests.base import BackathonTest


class TestDatabase(BackathonTest):
    """Tests related to the Database class"""

    def test_fkey_constraints(self):
        """Tests that foreign key constraints are enforced, but deferred"""
        repo = self.init_basic_repo()
        db = repo.db
        with db.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute("INSERT INTO objects (objid, type) VALUES ('A', 'a')")
            cursor.execute(
                "INSERT INTO object_relations (parent, child) VALUES ('A', 'B'), ('A', 'C')"
            )
            cursor.execute("INSERT INTO objects (objid, type) VALUES ('B', 'b')")
            with self.assertRaises(sqlite3.IntegrityError):
                cursor.execute("COMMIT")
