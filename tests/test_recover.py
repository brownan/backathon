import asyncio
import json
import pathlib

from backathon.encryption.nacl import NaclEncrypter
from backathon.recover import repair_object_index
from backathon.repository import Backathon
from tests.base import BackathonTest
from tests.test_backup import AssertObjHelperMixin
from tests.test_backup import ExpectedDir
from tests.test_backup import ExpectedFile


class TestRepair(AssertObjHelperMixin, BackathonTest):
    """Tests for the recover functionality"""

    def create_test_repo(self, enable_encryption: bool = False):
        if enable_encryption:
            self.password = "foobar"
            encrypter = NaclEncrypter.new(self.password)
        else:
            self.password = None
            encrypter = None
        back = self.init_basic_repo(encrypter)
        self.create_file("file", "contents")
        asyncio.run(back.scan_async())
        asyncio.run(back.backup_async())
        back.close()

        # Remove the local database. This should have been done by sqlite automatically
        # when the database was closed above, but for some reason it isn't. My guess is
        # Python's keeping some references to some sqlite objects around that will get
        # garbage collected later.
        pathlib.Path(self.db_path).unlink()
        pathlib.Path(self.db_path + "-wal").unlink(missing_ok=True)
        pathlib.Path(self.db_path + "-shm").unlink(missing_ok=True)

    def test_recover_repository(self):
        """Tests the Backathon.recover() class method

        This method is expected to read from the remote repository and create a new properly
        configured local database
        """
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

    def test_recover_repository_with_encryption(self):
        """Tests the Backathon.recover() method with encryption

        This is expected to read the encryption paramaters from the remote repository and
        configure a new local database with the proper encryption keys
        """
        self.create_test_repo(True)
        back = asyncio.run(
            Backathon.recover(
                self.db_path,
                self.storage,
                self.password,
            )
        )
        self.assertEqual(
            back.db.config_get("encrypter"),
            "NaclEncrypter",
        )
        self.assertEqual(
            back.db.config_get_json("encrypter-config"),
            json.loads(self.encrypter.config.model_dump_json()),
        )

    def test_rebuild_database(self):
        """Tests rebuilding the local database from scratch, with no encryption"""
        self.create_test_repo()
        with asyncio.Runner() as r:
            back = r.run(
                Backathon.recover(
                    self.db_path,
                    self.storage,
                    self.password,
                )
            )
            self.back = back

            with back.db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM objects),
                        (SELECT COUNT(*) FROM fsentry)
                """
                )
                self.assertEqual(
                    tuple(cursor.fetchone()),
                    (0, 0),
                )

            r.run(
                repair_object_index(
                    back.db,
                    back.get_storage(),
                    back.get_encrypter(),
                )
            )
        with back.db.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM objects),
                    (SELECT COUNT(*) FROM fsentry)
            """
            )
            self.assertEqual(
                tuple(cursor.fetchone()),
                (2, 0),
            )
        self.assert_backupsets(
            {
                self.backupdir: ExpectedDir(
                    {
                        "file": ExpectedFile("contents"),
                    }
                )
            }
        )

    def test_rebuild_database_with_encryption(self):
        """Tests rebuilding the database when it's encrypted"""
        raise NotImplementedError
