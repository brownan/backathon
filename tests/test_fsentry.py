from __future__ import annotations

import os
import pathlib
import stat
from unittest import mock

from backathon import models, repoobject
from backathon.models import ObjectHeader, ObjectType, ObjIDType
from tests.base import BackathonTest

ExpectedFile = str
ExpectedDir = dict[str, "ExpectedDir | ExpectedFile"]


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

    def _assert_dir(self, objid: ObjIDType, expected: ExpectedDir):
        """Asserts that the given object is a dir object with the given
        entries

        """
        full_obj_path = self.backuppath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.TREE, header.type)

            # No body
            self.assertEqual(b"", stream.read())

            assert header.entries is not None
            obj_entries = {e.name_str: e.objid for e in header.entries}

            # All the same names should be set
            self.assertSetEqual(set(expected), set(obj_entries))

            for name, expected_val in expected.items():
                referenced_objid = obj_entries[name]
                if isinstance(expected_val, dict):
                    # Another directory
                    self._assert_dir(referenced_objid, expected_val)
                elif isinstance(expected_val, str):
                    # A file
                    pass
                elif isinstance(expected_val, tuple):
                    # A symlink
                    pass
                else:
                    raise Exception

    def assert_backupsets(self, *snapshots: ExpectedDir):
        """Asserts that one or more snapshots exist both in the database and on disk

        Each given snapshot describes a directory hierarchy of directories and
        files. The top-most ExpectedDir contains the roots of the snapshot, if there were
        more than one root in the backup set. Otherwise, the top-most ExpectedDir
        will usually just have the one entry mapping self.backupdir to another
        ExpectedDir.

        Each snapshot in the snapshots list is a separate backup. So if a
        test does one backup, then there should be one snapshot given. If a
        test does two backups, then it should provide two structures
        describing the contents of each backup.
        """
        with self.back.db.cursor() as cursor:
            cursor.execute("SELECT distinct timestamp FROM snapshots ORDER BY timestamp")
            backup_dates: list[str] = [r[0] for r in cursor]

        self.assertEqual(
            len(snapshots),
            len(backup_dates),
            "Different number of snapshots given than are in the database",
        )

        for snapshot, date in zip(snapshots, backup_dates):
            # This snapshot has len(snapshot) roots, so should have that many
            # Snapshot objects in the database.
            db_snaphots = list(
                self.back.db.get_objects(
                    models.Snapshot, "SELECT * FROM snapshots WHERE timestamp=?", (date,)
                )
            )
            self.assertEqual(
                len(snapshot), len(db_snaphots), "DB has wrong number of snapshot roots"
            )
            # Now correlated them with each other
            roots = {s.path: s.root for s in db_snaphots}
            for root_dir_name, root_dir_expected in snapshot.items():
                # Root of a snapshot is always a directory
                assert isinstance(root_dir_expected, dict)

                root_objid = roots[root_dir_name]
                self._assert_dir(root_objid, root_dir_expected)

    def test_objects_comitted(self):
        """Tests that objects being backed up are both committed to the database and
        written to the filesystem

        """
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
                self.assertEqual(obj.type, header.type)

    def test_backup(self):
        self.create_file("dir/file1", "file contents")
        self.create_file("dir/file2", "file contents 2")
        self.back.scan()
        entries = list(self.back.db.get_objects(models.FSEntry, "SELECT * FROM fsentry"))
        self.assertEqual(4, len(entries))
        self.back.backup()
        entries = list(self.back.db.get_objects(models.FSEntry, "SELECT * FROM fsentry"))
        self.assertTrue(all(entry.objid is not None for entry in entries))
        objects = list(self.back.db.get_objects(models.Object, "SELECT * FROM objects"))
        self.assertEqual(6, len(objects))

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
