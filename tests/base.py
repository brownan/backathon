import os
import pathlib
import tempfile
from contextlib import ExitStack
from unittest import TestCase

from backathon.encryption.null import NullConfig, NullEncrypter
from backathon.repository import Backathon
from backathon.storage.local import LocalStorage, LocalStorageConfig


class BackathonTest(TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

        # Directory to be backed up
        self.backupdir = pathlib.Path(
            self.stack.enter_context(
                tempfile.TemporaryDirectory(),
            )
        )
        # Directory to store the data files
        self.repodir = pathlib.Path(
            self.stack.enter_context(
                tempfile.TemporaryDirectory(),
            )
        )

        # Reserve a name in the filesystem that tests can use to create a sqlite
        # database. The context manager makes sure it's removed at the end of the test.
        self.db_path = self.stack.enter_context(tempfile.NamedTemporaryFile()).name

        # For some reason, sqlite doesn't remove the shm and wal files when a test
        # finishes. So we remove them manually.
        self.stack.callback(
            lambda: pathlib.Path(self.db_path + "-shm").unlink(missing_ok=True)
        )
        self.stack.callback(
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

    def init_basic_repo(self) -> Backathon:
        back = Backathon.initialize(
            self.db_path,
            LocalStorage(LocalStorageConfig(base_path=self.repodir)),
            NullEncrypter(NullConfig()),
        )
        back.db.config_set("enable-compression", False)
        # For these tests, always inline files unless the test specifies otherwise.
        # This ensures these tests work independently of the default inline threshold
        # changing
        # The specific value here needs to be larger than any test files
        back.db.config_set("inline-threshold", 2**20)
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
