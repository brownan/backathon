import tempfile
import pathlib
import logging
import stat
import os
import unittest

from gbackup import scan, backup, models, restore

from .base import TestBase

class AssertionHandler(logging.Handler):
    """A logging handler that will raise an AssertionError if any warnings or
    errors are emitted

    Use this in tests by attaching it to a logger to make sure no warnings are
    emitted by that logger
    """
    def emit(self, record):
        raise AssertionError(self.format(record))

class TestRestore(TestBase):
    def setUp(self):
        super().setUp()
        self.restoredir = self.stack.enter_context(
            tempfile.TemporaryDirectory()
        )
        models.FSEntry.objects.create(path=self.backupdir)

        self.handler = AssertionHandler()
        self.handler.setLevel(logging.WARNING)
        logging.getLogger("gbackup.restore").addHandler(self.handler)

    def tearDown(self):
        logging.getLogger("gbackup.restore").removeHandler(self.handler)
        super().tearDown()

    def assert_restored_file(self, path, contents):
        fullpath = pathlib.Path(self.restoredir, path)
        self.assertEqual(
            contents,
            fullpath.read_text(),
        )

    def test_simple_restore(self):
        self.create_file("file1", "contents1")
        self.create_file("dir/file2", "contents2")
        scan.scan()
        backup.backup()

        ss = models.Snapshot.objects.get()

        restore.restore_item(ss.root, self.restoredir)

        self.assert_restored_file("file1", "contents1")
        self.assert_restored_file("dir/file2", "contents2")

    def test_restore_mode(self):
        file_a = self.create_file("file1", "contents")
        file_a.chmod(0o777)

        scan.scan()
        backup.backup()
        ss = models.Snapshot.objects.get()
        restore.restore_item(ss.root, self.restoredir)

        file_b = pathlib.Path(self.restoredir, "file1")
        stat_result = file_b.stat()
        self.assertEqual(
            0o777,
            stat.S_IMODE(stat_result.st_mode),
        )

    def test_restore_uid_gid(self):
        file_a = self.create_file("file1", "contents")
        try:
            os.chown(file_a, 1, 1)
        except PermissionError:
            raise unittest.SkipTest("Process doesn't have chown permission")

        scan.scan()
        backup.backup()
        ss = models.Snapshot.objects.get()
        restore.restore_item(ss.root, self.restoredir)

        file_b = pathlib.Path(self.restoredir, "file1")
        stat_result = file_b.stat()
        self.assertEqual(
            1,
            stat_result.st_uid
        )
        self.assertEqual(
            1,
            stat_result.st_gid
        )

    def test_restore_multiple_revisions(self):
        self.create_file("file", "contents A")

        scan.scan()
        backup.backup()

        self.create_file("file", "new contents")

        scan.scan()
        backup.backup()

        snapshots = list(models.Snapshot.objects.order_by("date"))

        self.assertEqual(2, len(snapshots))
        self.assertEqual(
            6,
            models.Object.objects.count()
        )

        restoredir = pathlib.Path(self.restoredir)
        restore.restore_item(snapshots[0].root, restoredir/"ss1")
        restore.restore_item(snapshots[1].root, restoredir/"ss2")

        file1 = restoredir / "ss1" / "file"
        file2 = restoredir / "ss2" / "file"

        self.assertEqual(
            "contents A",
            file1.read_text()
        )
        self.assertEqual(
            "new contents",
            file2.read_text()
        )

    def test_restore_single_file(self):
        self.create_file("file", "contents")

        scan.scan()
        backup.backup()

        root = models.Snapshot.objects.get().root

        # Should just be one child
        inode = root.children.get()

        filename = pathlib.Path(self.restoredir, "my_file")
        restore.restore_item(inode, filename)

        self.assertEqual(
            "contents",
            filename.read_text()
        )
