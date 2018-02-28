from contextlib import ExitStack
import tempfile
import os.path
import pathlib

from django.test import TestCase

from gbackup import models

class TestBase(TestCase):
    def setUp(self):
        self.stack = ExitStack()

        # Directory to be backed up
        self.backupdir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )
        models.FSEntry.objects.create(path=self.backupdir)

        # Directory to store the data files
        self.datadir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )

        # Configure the settings in the database
        models.Setting.set("REPO_BACKEND", "local")
        models.Setting.set("REPO_PATH", self.datadir)

        models.Setting.set("ENCRYPTION", "none")
        models.Setting.set("COMPRESSION", "none")

    def path(self, *args):
        return os.path.join(self.backupdir, *args)

    def create_file(self, path, contents):
        assert not path.startswith("/")
        pathobj = pathlib.Path(self.path(path))
        if not pathobj.parent.exists():
            pathobj.parent.mkdir(parents=True)
        pathobj.write_text(contents, encoding="UTF-8")
        return pathobj

    def tearDown(self):
        self.stack.close()