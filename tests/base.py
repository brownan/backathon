from contextlib import ExitStack
import tempfile
import os.path
import pathlib

from django.test import TestCase

from backathon import models
from backathon.repository import Repository


class TestBase(TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

        # Directory to be backed up
        self.backupdir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )
        # Directory to store the data files
        self.datadir = self.stack.enter_context(
            tempfile.TemporaryDirectory(),
        )

        # Create a repo object with an in-memory database, and configure it
        self.repo = Repository(":memory:")

        self.repo.set_storage("local", {"base_dir": self.datadir})
        self.repo.set_compression(False)
        self.repo.set_encrypter("none", None)
        self.repo.backup_inline_threshold = 0

        # Shortcut for a few managers to prevent lots of typing in the unit
        # tests
        self.db = self.repo.db
        self.fsentry = models.FSEntry.objects.using(self.db)
        self.object = models.Object.objects.using(self.db)
        self.snapshot = models.Snapshot.objects.using(self.db)
        self.obj_relation = models.ObjectRelation.objects.using(self.db)

        # Create the root of the backup set
        self.fsentry.create(path=self.backupdir)

    def tearDown(self):
        # You can't "close" an in-memory database in Django, so instead we
        # just delete it from the connection handler. The garbage collector
        # will hopefully free the resources, but the important thing is we get a
        # fresh database for each test
        import django.db
        del django.db.connections[self.repo.db]
        del django.db.connections.databases[self.repo.db]

    def path(self, *args):
        return os.path.join(self.backupdir, *args)

    def create_file(self, path, contents):
        assert not path.startswith("/")
        pathobj = pathlib.Path(self.path(path))
        if not pathobj.parent.exists():
            pathobj.parent.mkdir(parents=True)
        pathobj.write_text(contents, encoding="UTF-8")
        return pathobj

