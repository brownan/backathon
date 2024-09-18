import asyncio
import pathlib

from backathon.db import Database
from backathon.recover import repair_object_index
from tests.base import BackathonTest


class TestRepair(BackathonTest):
    """Tests for the recover functionality"""

    def test_rebuild_database(self):
        """Tests rebuilding the local database from scratch, with no encryption"""
        back = self.init_basic_repo(None)
        self.create_file("file", "contents")
        back.scan()
        back.backup()
        back.close()

        # Remove the local database
        pathlib.Path(self.db_path).unlink()
        self.assertFalse(pathlib.Path(self.db_path + "-wal").exists())
        self.assertFalse(pathlib.Path(self.db_path + "-shm").exists())

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
