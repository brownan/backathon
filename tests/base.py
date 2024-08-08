import pathlib
import tempfile
from contextlib import ExitStack
from unittest import TestCase


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
        self.datadir = pathlib.Path(
            self.stack.enter_context(
                tempfile.TemporaryDirectory(),
            )
        )

        # Create a repo object with a temporary database. We can't use sqlite
        # in-memory databases because the backup routine is multi-threaded
        # and all threads access the same database.
        self.db_path = self.stack.enter_context(tempfile.NamedTemporaryFile()).name

    def backuppath(self, *args):
        return self.backupdir.joinpath(*args)

    def datapath(self, *args):
        return self.datadir.joinpath(*args)
