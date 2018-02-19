import stat
from unittest import mock
import os

from django.test import TestCase

from gbackup import models, scan, backup
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
            models.FSEntry.objects.filter(path=os.fspath(file)).exists()
        )
        file.unlink()
        scan.scan()
        self.assertFalse(
            models.FSEntry.objects.filter(path=os.fspath(file)).exists()
        )

    def test_deleted_dir(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        file.unlink()
        file.parent.rmdir()
        scan.scan()
        self.assertFalse(
            models.FSEntry.objects.filter(path=os.fspath(file.parent)).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=os.fspath(file)).exists()
        )

    def test_replace_dir_with_file(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        scan.scan()
        file.unlink()
        file.parent.rmdir()
        file.parent.write_text("another  file contents")
        # Scan the parent first
        models.FSEntry.objects.get(path=os.fspath(file.parent)).scan()
        scan.scan()
        self._replace_dir_with_file_asserts(file)

    def _replace_dir_with_file_asserts(self, file):
        self.assertTrue(
            models.FSEntry.objects.filter(path=os.fspath(file.parent)).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=os.fspath(file)).exists()
        )
        entry = models.FSEntry.objects.get(
            path=os.fspath(file.parent)
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
        models.FSEntry.objects.get(path=os.fspath(file)).scan()
        scan.scan()
        self._replace_dir_with_file_asserts(file)

    def test_dir_no_permission(self):
        file = self.create_file("dir/file1", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)

        file.parent.chmod(0o000)
        scan.scan()

        self.assertTrue(
            models.FSEntry.objects.filter(path=os.fspath(file.parent)).exists()
        )
        self.assertFalse(
            models.FSEntry.objects.filter(path=os.fspath(file)).exists()
        )

        # Set permission back so the tests can be cleaned up
        file.parent.chmod(0o777)

    def test_root_merge(self):
        file = self.create_file("dir1/dir2/file", "file contents")
        models.FSEntry.objects.create(path=self.backupdir)
        models.FSEntry.objects.create(path=os.fspath(file.parent))
        self.assertEqual(
            2,
            models.FSEntry.objects.filter(parent__isnull=True).count()
        )
        scan.scan()
        self.assertEqual(
            1,
            models.FSEntry.objects.filter(parent__isnull=True).count()
        )

class FSEntryBackup(TestBase):

    def setUp(self):
        super().setUp()
        models.FSEntry.objects.create(path=self.backupdir)

    def _assert_file_obj(self, obj, contents):
        """Asserts that the given object is a file object with the given
        contents"""
        payload = obj.unpack_payload()
        self.assertEqual("inode", next(payload))

        info = next(payload)
        chunks = next(payload)
        self.assertRaises(StopIteration, next,payload)

        buf = bytearray(info['size'])
        for pos, chunkid in chunks:
            chunk = models.Object.objects.get(objid=chunkid)
            chunkpayload = chunk.unpack_payload()
            self.assertEqual("blob", next(chunkpayload))
            chunkcontents = next(chunkpayload)
            self.assertRaises(StopIteration, next,chunkpayload)
            buf[pos:pos+len(chunkcontents)] = chunkcontents

        self.assertEqual(buf.decode("UTF-8"), contents)

    def _assert_dir(self, obj, contents):
        """Asserts that the given object is a dir object with the given
        contents"""
        payload = obj.unpack_payload()
        self.assertEqual("tree", next(payload))

        info = next(payload)
        children = next(payload)
        self.assertRaises(StopIteration, next,payload)

        children_objs = {
            name: models.Object.objects.get(objid=objid)
            for name, objid in children
        }
        self.assert_objects(contents, children_objs)

    def assert_objects(self, structure, objects=None):
        """Asserts that a hierarchy of objects described by `structure`
        exists in the database

        :param structure: A mapping of names to structure|string describing
            the layout of the objects
        :param objects: A mapping of name to Object instances. Used on
        recursive calls back into this method.

        Each structure or string in the structure dict will be checked
        against its corresponding Object
        """
        if objects is None:
            objects = {
                os.fsencode(s.path): s.root
                for s in models.Snapshot.objects.all()
            }
        structure = {
            os.fsencode(name): contents
            for name, contents in structure.items()
        }

        for name, contents in structure.items():
            self.assertIn(
                name,
                objects,
                "Object {} not found".format(name)
            )
            obj = objects.pop(name)
            if isinstance(contents, str):
                self._assert_file_obj(obj, contents)
            elif isinstance(contents, dict):
                self._assert_dir(obj, contents)
            else:
                raise TypeError("Unknown contents type")

        self.assertEqual(
            0,
            len(objects),
            "Extra objects not expected: {}".format(objects),
        )


    def test_backup(self):
        self.create_file("dir/file1", "file contents")
        self.create_file("dir/file2", "file contents 2")
        scan.scan()
        self.assertEqual(
            4,
            models.FSEntry.objects.count()
        )
        backup.backup()
        self.assertTrue(
            all(entry.obj is not None for entry in
                models.FSEntry.objects.all())
        )
        self.assertEqual(
            6,
            models.Object.objects.count()
        )

        self.assert_objects({
            self.backupdir: {
                'dir': {
                    'file1': 'file contents',
                    'file2': 'file contents 2',
                }
            }
        })

    def test_backup_identical_files(self):
        self.create_file("file1", "file contents")
        self.create_file("file2", "file contents")
        scan.scan()
        backup.backup()
        self.assert_objects({
            self.backupdir: {
                'file1': 'file contents',
                'file2': 'file contents',
            }
        })
        # Inode objects differ, so 4 total objects uploaded
        self.assertEqual(
            4,
            models.Object.objects.count()
        )

    def test_backup_hardlinked_files(self):
        file = self.create_file("file1", "file contents")
        os.link(
            file,
            file.parent / "file2"
        )
        scan.scan()
        backup.backup()
        self.assert_objects({
            self.backupdir: {
                'file1': 'file contents',
                'file2': 'file contents',
            }
        })
        # The inode objects should be identical, so 3 total objects uploaded
        self.assertEqual(
            3,
            models.Object.objects.count()
        )

    def test_file_disappeared(self):
        file = self.create_file("dir/file1", "file contents")
        scan.scan()
        self.assertEqual(
            3,
            models.FSEntry.objects.count()
        )
        file.unlink()
        backup.backup()
        self.assertEqual(
            2,
            models.FSEntry.objects.count()
        )
        self.assertEqual(
            2,
            models.Object.objects.count(),
        )
        self.assert_objects({
            self.backupdir: {'dir': {}}
        })

    def test_file_type_change(self):
        file = self.create_file("dir/file1", "file contents")
        scan.scan()
        self.assertEqual(
            3,
            models.FSEntry.objects.count()
        )
        file.unlink()
        file.mkdir()
        backup.backup()
        self.assertEqual(
            2,
            models.FSEntry.objects.count()
        )
        self.assertEqual(
            2,
            models.Object.objects.count(),
        )
        self.assert_objects({
            self.backupdir: {'dir': {}}
        })

    def test_file_disappeared_2(self):
        # We want to delete the file after the initial lstat() call,
        # but before the file is opened for reading later on, to test this
        # race condition. So we patch os.lstat to delete the file right after
        # the lstat call.
        file = self.create_file("dir/file1", "file contents")
        scan.scan()
        self.assertEqual(
            3,
            models.FSEntry.objects.count()
        )

        import os
        real_lstat = os.lstat
        def lstat(path):
            stat_result = real_lstat(path)
            if path == str(file):
                file.unlink()
            return stat_result
        self.stack.enter_context(
            mock.patch(
                "os.lstat",
                lstat,
            )
        )

        backup.backup()
        self.assertEqual(
            2,
            models.FSEntry.objects.count()
        )
        self.assertEqual(
            2,
            models.Object.objects.count(),
        )
        self.assert_objects({
            self.backupdir: {'dir': {}}
        })

    def test_permission_denied_file(self):
        file = self.create_file("dir/file1", "file contents")
        scan.scan()
        self.assertEqual(
            3,
            models.FSEntry.objects.count()
        )

        file.chmod(0o000)

        backup.backup()
        self.assertEqual(
            2,
            models.FSEntry.objects.count()
        )
        self.assertEqual(
            2,
            models.Object.objects.count(),
        )
        self.assert_objects({
            self.backupdir: {'dir': {}}
        })

    def test_invalid_utf8_filename(self):
        """Tests that a file with invalid utf-8 in the name can be backed up"""
        name = os.fsdecode(b"\xff\xffhello\xff\xff")
        self.create_file(name, "file contents")
        scan.scan()
        backup.backup()

        self.assert_objects({
            self.backupdir: {name: "file contents"}
        })

