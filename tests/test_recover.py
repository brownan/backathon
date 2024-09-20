import asyncio
import pathlib

from backathon.db import Database
from backathon.recover import repair_object_index
from backathon.repository import Backathon
from tests.base import BackathonTest


class TestRepair(BackathonTest):
    """Tests for the recover functionality"""

    def create_test_repo(self):
        back = self.init_basic_repo(None)
        self.create_file("file", "contents")
        back.scan()
        back.backup()
        back.close()

        # Remove the local database
        pathlib.Path(self.db_path).unlink()
        self.assertFalse(pathlib.Path(self.db_path + "-wal").exists())
        self.assertFalse(pathlib.Path(self.db_path + "-shm").exists())

    def test_recover_repository(self):
        """Tests the Backathon.recover() class method"""
        self.create_test_repo()
        back = asyncio.run(
            Backathon.recover(
                self.db_path,
                self.storage,
                None,
            )
        )
        self.assertEqual(
            back.db.config_get("storage"),
            "LocalStorage",
        )
        self.assertEqual(
            back.db.config_get_json("storage-config"),
            {"base_path": str(self.repodir)},
        )
        self.assertEqual(
            back.db.config_get("encrypter"),
            "NullEncrypter",
        )
        self.assertEqual(
            back.db.config_get_json("encrypter-config"),
            {},
        )

    def test_rebuild_database(self):
        """Tests rebuilding the local database from scratch, with no encryption"""
        self.create_test_repo()

        # Initiate the recovery process
        # Start with a blank database
        db = Database(self.db_path, create=True)

        coro = repair_object_index(
            db,
            self.storage,
            self.encrypter,
        )
        asyncio.run(coro)

    def test_rebuild_database_with_encryption(self):
        """Tests rebuilding the database when it's encrypted"""
        raise NotImplementedError
