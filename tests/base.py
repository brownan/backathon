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

        # Create a repo object with a temporary database. We can't use sqlite
        # in-memory databases because the backup routine is multi-threaded
        # and all threads access the same database.
        self.db_path = self.stack.enter_context(tempfile.NamedTemporaryFile()).name

    def backuppath(self, *args) -> pathlib.Path:
        return self.backupdir.joinpath(*args)

    def datapath(self, *args) -> pathlib.Path:
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
        back.add_root(self.backupdir)
        return back
