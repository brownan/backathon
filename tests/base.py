import os
import pathlib
import tempfile
import unittest.mock
from unittest import TestCase

import nacl.pwhash.argon2id

from backathon.encryption.base import EncrypterBase
from backathon.encryption.nacl import NaclEncrypter
from backathon.encryption.null import NullConfig
from backathon.encryption.null import NullEncrypter
from backathon.repository import Backathon
from backathon.storage.local import LocalStorage
from backathon.storage.local import LocalStorageConfig


class BackathonTest(TestCase):
    def setUp(self):
        # Set encryption params to something quicker for testing
        self.enterContext(
            unittest.mock.patch.object(
                NaclEncrypter, "DEFAULT_OPSLIMIT", nacl.pwhash.argon2id.OPSLIMIT_MIN
            )
        )
        self.enterContext(
            unittest.mock.patch.object(
                NaclEncrypter, "DEFAULT_MEMLIMIT", nacl.pwhash.argon2id.MEMLIMIT_MIN
            )
        )

        # Directory to be backed up
        self.backupdir = pathlib.Path(
            self.enterContext(
                tempfile.TemporaryDirectory(),
            )
        )
        # Directory to store the data files
        self.repodir = pathlib.Path(
            self.enterContext(
                tempfile.TemporaryDirectory(),
            )
        )

        # Reserve a name in the filesystem that tests can use to create a sqlite
        # database. The context manager makes sure it's removed at the end of the test.
        self.db_path = self.enterContext(tempfile.NamedTemporaryFile()).name

        # For some reason, sqlite doesn't remove the shm and wal files when a test
        # finishes. So we remove them manually.
        self.addCleanup(
            lambda: pathlib.Path(self.db_path + "-shm").unlink(missing_ok=True)
        )
        self.addCleanup(
            lambda: pathlib.Path(self.db_path + "-wal").unlink(missing_ok=True)
        )

    def backuppath(self, *args) -> pathlib.Path:
        return self.backupdir.joinpath(*args)

    def repopath(self, *args) -> pathlib.Path:
        return self.repodir.joinpath(*args)

    def create_file(self, path: str | os.PathLike[str], contents: str) -> pathlib.Path:
        path = self.backuppath(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def init_basic_repo(self, encrypter: EncrypterBase | None = None) -> Backathon:
        self.encrypter = encrypter or NullEncrypter(NullConfig())
        self.storage = LocalStorage(LocalStorageConfig(base_path=self.repodir))
        back = Backathon.initialize(
            self.db_path,
            self.storage,
            self.encrypter,
        )
        back.db.config.enable_compression = False
        # For these tests, always inline files unless the test specifies otherwise.
        # This ensures these tests work independently of the default inline threshold
        # changing
        # The specific value here needs to be larger than any test files
        back.db.config.inline_threshold = 2**20
        back.db.save_config()
        back.add_root(self.backupdir)
        return back

    def assert_object_count(self, back: Backathon, expected: int):
        with back.db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM objects")
            self.assertEqual(expected, cursor.fetchone()[0] or 0)

    def assert_fsentry_count(self, back: Backathon, expected: int):
        with back.db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM fsentry")
            self.assertEqual(expected, cursor.fetchone()[0] or 0)
