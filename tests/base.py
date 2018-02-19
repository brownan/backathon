from contextlib import ExitStack
import tempfile
import os.path
import pathlib

from django.test import TestCase

class TestBase(TestCase):
    def setUp(self):
        self.stack = ExitStack()

        # Directory to be backed up
        self.backupdir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )

        # Directory to store the data files
        self.datadir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )
        self.stack.enter_context(
            self.settings(
                MEDIA_ROOT=self.datadir,
                DEFAULT_FILE_STORAGE="django.core.files.storage"
                                     ".FileSystemStorage"
            )
        )

    def path(self, *args):
        return os.path.join(self.backupdir, *args)

    def create_file(self, path, contents):
        assert not path.startswith("/")
        pathobj = pathlib.Path(self.path(path))
        if not pathobj.parent.exists():
            pathobj.parent.mkdir(parents=True)
        pathobj.write_text(contents)
        return pathobj

    def tearDown(self):
        self.stack.close()