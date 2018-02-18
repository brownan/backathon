import stat

from django.test import TestCase

from gbackup import models, scan
from .base import TestBase

class FSEntryTest(TestCase):

    def test_invalidate(self):
        """Tests that the FSEntry.invalidate() method works"""
        o = models.Object.objects.create(objid="a")

        root = models.FSEntry.objects.create(
            path="/1",
            obj=o,
        )
        e1 = models.FSEntry.objects.create(
            path="/1/2",
            parent=root,
            obj=o,
        )
        e2 = models.FSEntry.objects.create(
            path="/1/2/3",
            parent=e1,
            obj=o,
        )
        e3 = models.FSEntry.objects.create(
            path="/1/2/3/4",
            parent=e2,
            obj=None,
        )

        self.assertListEqual(
            list(models.FSEntry.objects.filter(obj__isnull=True)),
            [e3],
        )
        self.assertSetEqual(
            set(models.FSEntry.objects.filter(obj__isnull=False)),
            {root,e1,e2},
        )

        e3.invalidate()

        self.assertEqual(
            models.FSEntry.objects.filter(obj__isnull=True).count(),
            4
        )

class FSEntryScan(TestBase):

    def test_scan(self):
        self.create_file("dir/file1", "file contents")
        self.create_file("dir2/file2", "another file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        self.assertEqual(
            5,
            models.FSEntry.objects.count()
        )
        entries = models.FSEntry.objects.all()

        names = set(e.name for e in entries)
        for name in ['file1', 'file2', 'dir', 'dir2']:
            self.assertIn(
                name,
                names
            )

    def test_deleted_file(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        self.assertTrue(
            models.FSEntry.objects.filter(path=file.as_posix()).exists()
        )
        file.unlink()
        scan.scan()
        self.assertFalse(
            models.FSEntry.objects.filter(path=file.as_posix()).exists()
        )

    def test_deleted_dir(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        file.unlink()
        file.parent.rmdir()
        scan.scan()
        self.assertFalse(
            models.FSEntry.objects.filter(path=file.parent.as_posix()).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=file.as_posix()).exists()
        )

    def test_replace_dir_with_file(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        file.unlink()
        file.parent.rmdir()
        file.parent.write_text("another  file contents")
        # Scan the parent first
        models.FSEntry.objects.get(path=file.parent.as_posix()).scan()
        scan.scan()
        self._replace_dir_with_file_asserts(file)

    def _replace_dir_with_file_asserts(self, file):
        self.assertTrue(
            models.FSEntry.objects.filter(path=file.parent.as_posix()).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=file.as_posix()).exists()
        )
        entry = models.FSEntry.objects.get(
            path=file.parent.as_posix()
        )
        self.assertEqual(
            entry.children.count(),
            0
        )
        self.assertTrue(
            stat.S_ISREG(entry.st_mode)
        )

    def test_replace_dir_with_file_2(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        file.unlink()
        file.parent.rmdir()
        file.parent.write_text("another  file contents")
        # Scan the file first
        models.FSEntry.objects.get(path=file.as_posix()).scan()
        scan.scan()
        self._replace_dir_with_file_asserts(file)

    def test_dir_no_permission(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)

        file.parent.chmod(0o000)
        scan.scan()

        self.assertTrue(
            models.FSEntry.objects.filter(path=file.parent.as_posix()).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=file.as_posix()).exists()
        )

        # Set permission back so the tests can be cleaned up
        file.parent.chmod(0o777)

    def test_root_merge(self):
        file = self.create_file("dir1/dir2/file", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        models.FSEntry.objects.create(path=file.parent.as_posix())
        self.assertEqual(
            2,
            models.FSEntry.objects.filter(parent__isnull=True).count()
        )
        scan.scan()
        self.assertEqual(
            1,
            models.FSEntry.objects.filter(parent__isnull=True).count()
        )

