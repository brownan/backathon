from contextlib import ExitStack
import tempfile
import os.path
import pathlib

from django.db.transaction import atomic
from django.test import TestCase

from backathon import models
from backathon.repository import Repository
import backathon.encryption


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

        # Create a repo object with a temporary database. We can't use sqlite
        # in-memory databases because the backup routine is multi-threaded
        # and all threads access the same database.
        tmpdb = tempfile.NamedTemporaryFile(delete=False)
        tmpdb.close()
        self.stack.callback(os.unlink, tmpdb.name)
        self.repo = Repository(tmpdb.name)

        self.repo.set_storage("local", {"base_dir": self.datadir})
        self.repo.set_compression(False)
        self.repo.set_encrypter(backathon.encryption.NullEncryption.init_new())
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

    def _insert_objects(self, *objects):
        """Insert a set of objects into the Object table

        Each object is a tuple of (objid, [children])

        Callers must be careful to avoid reference loops in the object
        hierarchy, as that is not a valid object tree.
        """
        # Since SQLite has deferrable foreign key constraints, we can insert
        # references to rows that don't exist yet as long as they exist when
        # the transaction is committed.
        with atomic(using=self.db):
            for objid, children in objects:
                if isinstance(objid, str):
                    objid = objid.encode("ASCII")
                obj = self.object.create(
                    objid=objid,
                )
                self.obj_relation.bulk_create([
                    models.ObjectRelation(
                        parent=obj,
                        child_id=c.encode("ASCII") if isinstance(c,str) else c
                    ) for c in children
                ])

    def assert_objects(self, objs, roots=None, no_extras=True):
        """Asserts that the given hierarchy exists in the database and that
        no other objects exist in the database

        """
        if roots is None:
            roots = self.object.filter(
                parents__isnull=True,
            )

        rootmap = {r.objid: r for r in roots}
        for name, children in objs.items():
            obj = rootmap.pop(name.encode("ASCII") if isinstance(name, str) else name)
            self.assert_objects(
                children,
                obj.children.all(),
            )

        if no_extras:
            self.assertDictEqual(
                rootmap,
                {},
                "Unexpected object found"
            )
