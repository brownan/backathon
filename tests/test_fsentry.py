import os
import pathlib
import stat
from typing import Iterable
from unittest import mock

from backathon import models, repoobject
from backathon.models import ObjectHeader, ObjectType
from tests.base import BackathonTest


class TestBackup(BackathonTest):
    """Tests backup functionality

    This test case covers both saving objects to storage, and updating the objects table
    in the database.
    """

    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo()

    def _assert_obj_contents(self, objid: bytes, body: bytes):
        """Asserts that the given objid has the given body by reading in the
        object on the filesystem


        """
        full_obj_path = self.backuppath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(body, stream.read())

    def _assert_symlink(self, objid: bytes, target: bytes):
        """Asserts that the given object is a symlink type with the given
        target

        """
        full_obj_path = self.backuppath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.SYMLINK, header.type)
            self.assertEqual(target, stream.read())

    def _assert_dir(self, objid: bytes, entries: Iterable[bytes]):
        """Asserts that the given object is a dir object with the given
        entries

        """
        full_obj_path = self.backuppath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.TREE, header.type)
            assert header.entries is not None
            obj_entries = {e.name for e in header.entries}
            self.assertSetEqual(set(entries), obj_entries)

    def _assert_objects(self, structure, objects):
        """Asserts that a hierarchy of objects described by `structure`
        is the same as the hierarchy of objects given by `objects`

        :param structure: A mapping of names to structure|string describing
            the layout of the objects. Another structure indicates the name
            is a directory, and a string indicates it's a file where the
            string is the file's contents.
        :param objects: A mapping of names to Object instances.

        Corresponding names in the structure and objects dictionaries are
        checked for equivalence.
        """

        # Encode all file names and we do byte comparisons throughout this
        # method and the helper methods. It's easier than decoding the object
        # file names everywhere they appear.
        structure = {os.fsencode(name): contents for name, contents in structure.items()}

        for name, contents in structure.items():
            self.assertIn(name, objects, "Object {} not found".format(name))
            obj = objects.pop(name)
            if isinstance(contents, str):
                self._assert_file_obj(obj, contents)
            elif isinstance(contents, dict):
                self._assert_dir(obj, contents)
            elif isinstance(contents, tuple) and contents[0] == "s":
                # symlink
                self._assert_symlink(obj, contents[1])
            else:
                raise TypeError("Unknown contents type")

        self.assertEqual(
            0,
            len(objects),
            "Extra objects not expected: {}".format(objects),
        )

    def assert_backupsets(self, *structures):
        """Asserts that the given structures exist in the database as objects

        The given structures describe what files we've backed up, and should
        exist as a set of object files in the local database.

        Each structure is a dictionary mapping names to values. Each value is
        either another structure (indicating the name is a directory) or a
        string (indicating the name is a file with the string as its contents)

        The names in the top level structures are the backup roots, which for
        these tests, should always be just self.backupdir unless the specific
        test adds more backup roots.

        Each structure in the structures list is a separate backup. So if a
        test does one backup, then there should be one structure given. If a
        test does two backups, then it should provide two structures
        describing the contents of each backup.
        """
        backup_dates = (
            self.snapshot.all().distinct().order_by("date").values_list("date", flat=True)
        )

        self.assertEqual(
            len(structures),
            len(backup_dates),
        )

        for structure, date in zip(structures, backup_dates):
            roots = {os.fsencode(s.path): s.root for s in self.snapshot.filter(date=date)}
            self._assert_objects(structure, roots)

    def test_objects_comitted(self):
        """Do a backup and then assert the objects actually get committed to
        the backing store"""
        self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.back.backup()

        all_objs = list(
            self.back.db.get_objects(
                models.Object,
                "SELECT * FROM objects",
            )
        )
        # Expect 2 tree and 1 inode objects
        self.assertEqual(3, len(all_objs))
        for obj in all_objs:
            obj_filepath = repoobject.make_object_path(obj.objid)
            # See that this object actually exists in the backup repo
            full_path = self.datapath(obj_filepath)
            self.assertTrue(full_path.is_file())

            # Read in the object and make sure its header is well-formed and has the same
            # type as we expect according to the database
            with full_path.open("rb") as stream:
                header = ObjectHeader.from_stream(stream)
                body = stream.read()
                self.assertEqual(obj.type, header.type)

    def test_backup(self):
        self.create_file("dir/file1", "file contents")
        self.create_file("dir/file2", "file contents 2")
        self.backathon.scan()
        self.assertEqual(4, self.fsentry.count())
        self.backathon.backup()
        self.assertTrue(all(entry.obj is not None for entry in self.fsentry.all()))
        self.assertEqual(6, self.object.count())

        self.assert_backupsets(
            {
                self.backupdir: {
                    "dir": {
                        "file1": "file contents",
                        "file2": "file contents 2",
                    }
                }
            }
        )

    def test_backup_identical_files(self):
        self.create_file("file1", "file contents")
        self.create_file("file2", "file contents")
        self.backathon.scan()
        self.backathon.backup()
        self.assert_backupsets(
            {
                self.backupdir: {
                    "file1": "file contents",
                    "file2": "file contents",
                }
            }
        )
        # Inode objects differ, so 4 total objects uploaded
        self.assertEqual(4, self.object.count())

    def test_backup_hardlinked_files(self):
        file = self.create_file("file1", "file contents")
        os.link(file, file.parent / "file2")
        self.backathon.scan()
        self.backathon.backup()
        self.assert_backupsets(
            {
                self.backupdir: {
                    "file1": "file contents",
                    "file2": "file contents",
                }
            }
        )
        # The inode objects should be identical, so 3 total objects uploaded
        self.assertEqual(3, self.object.count())

    def test_file_disappeared(self):
        file = self.create_file("dir/file1", "file contents")
        self.backathon.scan()
        self.assertEqual(3, self.fsentry.count())
        file.unlink()
        self.backathon.backup()
        self.assertEqual(2, self.fsentry.count())
        self.assertEqual(
            2,
            self.object.count(),
        )
        self.assert_backupsets({self.backupdir: {"dir": {}}})

    def test_file_changes_to_dir(self):
        """Tests if a file changes to a directory after scan before backup

        Currently the behavior is to back up the directory and update the
        fsentry in the process. The old behavior was to delete the entry and
        not back it up, but that turned out not to be necessary.
        """
        file = self.create_file("dir/file1", "file contents")
        self.backathon.scan()
        self.assertEqual(3, self.fsentry.count())
        self.assertTrue(stat.S_ISREG(self.fsentry.get(path=str(file)).st_mode))

        file.unlink()
        file.mkdir()
        self.backathon.backup()

        self.assertEqual(3, self.fsentry.count())
        self.assertTrue(stat.S_ISDIR(self.fsentry.get(path=str(file)).st_mode))

        self.assertEqual(
            3,
            self.object.count(),
        )

    def test_file_disappeared_2(self):
        # We want to delete the file after the initial lstat() call,
        # but before the file is opened for reading later on, to test this
        # race condition. So we patch os.lstat to delete the file right after
        # the lstat call.
        file = self.create_file("dir/file1", "file contents")
        self.backathon.scan()
        self.assertEqual(3, self.fsentry.count())

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

        self.backathon.backup()
        self.assertEqual(2, self.fsentry.count())
        self.assertEqual(
            2,
            self.object.count(),
        )
        self.assert_backupsets({self.backupdir: {"dir": {}}})

    def test_permission_denied_file(self):
        """A permission denied error when reading a file shouldn't cause the
        backup to fail, and other files should still get backed up"""
        self.create_file("dir/file1", "file contents")
        self.backathon.scan()
        self.assertEqual(3, self.fsentry.count())

        def raise_permissiondeined(path):
            raise PermissionError()

        with mock.patch("backathon.backup._open_file", raise_permissiondeined):
            self.backathon.backup()

        self.assertEqual(2, self.fsentry.count())
        self.assertEqual(
            2,
            self.object.count(),
        )
        self.assert_backupsets({self.backupdir: {"dir": {}}})

    def test_invalid_utf8_filename(self):
        """Tests that a file with invalid utf-8 in the name can be backed up"""
        name = os.fsdecode(b"\xff\xffhello\xff\xff")
        self.create_file(name, "file contents")
        self.backathon.scan()
        self.backathon.backup()

        self.assert_backupsets({self.backupdir: {name: "file contents"}})

    def test_symlink(self):
        """Tests that symlinks are saved properly"""
        self.create_file("file1", "file contents")

        pathobj = pathlib.Path(self.path("file2"))
        pathobj.symlink_to("file1")

        self.backathon.scan()
        self.backathon.backup()

        self.assert_backupsets(
            {self.backupdir: {"file1": "file contents", "file2": ("s", "file1")}}
        )

    def test_invalid_utf8_symlink(self):
        """Similar to the test for invalid filenames, but for symlinks: make
        sure we don't error if a symlink target is invalid utf-8

        """
        target = os.fsdecode(b"\xff\xffhello\xff\xff")

        pathobj = pathlib.Path(self.path("badsymlink"))
        pathobj.symlink_to(target)

        self.backathon.scan()
        self.backathon.backup()

        self.assert_backupsets({self.backupdir: {"badsymlink": ("s", target)}})
