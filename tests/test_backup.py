"""Tests for the backup code"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import stat
import sys
import zlib
from typing import IO
from unittest import mock

from backathon import models, repoobject
from backathon.models import ObjectHeader, ObjectType, ObjIDType
from backathon.repository import Backathon
from tests.base import BackathonTest


class ExpectedFile(str):
    pass


class ExpectedSymlink(str):
    pass


class ExpectedDir(dict[str, "ExpectedDir|ExpectedSymlink|ExpectedFile"]):
    pass


def stream_len_and_sha1(stream: IO[bytes]) -> tuple[int, bytes]:
    hasher = hashlib.sha1()
    pos = stream.tell()
    stream.seek(0)
    length = 0
    while buf := stream.read(io.DEFAULT_BUFFER_SIZE):
        length += len(buf)
        hasher.update(buf)
    stream.seek(pos)
    return length, hasher.digest()


class AssertObjHelperMixin(BackathonTest):
    back: Backathon

    def assert_relation_exists(self, parent: bytes, child: bytes, name: bytes | None):
        """Asserts that a relation exists in the object_relations table"""
        with self.back.db.cursor() as cursor:
            if name is not None:
                cursor.execute(
                    "SELECT 1 FROM object_relations WHERE parent=? AND child=? AND name=? LIMIT 1",
                    (parent, child, name),
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM object_relations WHERE parent=? AND child=? AND name IS NULL LIMIT 1",
                    (parent, child),
                )
            row = cursor.fetchone()
            self.assertIsNotNone(row, "Relation doesn't exist")

    def assert_object_header(
        self, objid: ObjIDType, header: ObjectHeader, uploaded_size: int, sha1: bytes
    ):
        """Asserts that the given object exists in the database and is consistent
        with the given object header

        """
        db_obj = next(
            self.back.db.query(
                models.Object, "SELECT * FROM objects WHERE objid=?", (objid,)
            )
        )
        self.assertEqual(db_obj.type, header.type)
        self.assertEqual(uploaded_size, db_obj.uploaded_size)
        self.assertEqual(sha1, db_obj.sha1)
        if header.stats:
            # File sizes should match
            self.assertEqual(header.stats.size, db_obj.file_size)
            # Last modified time should match
            assert header.last_modified_time
            self.assertEqual(header.last_modified_time, db_obj.last_modified_time)
        else:
            self.assertIsNone(db_obj.file_size)
            self.assertIsNone(db_obj.last_modified_time)

        # Check that all referenced entries are recorded in the object relations table
        if header.entries is not None:
            for entryref in header.entries:
                self.assert_relation_exists(objid, entryref.objid, entryref.name)
        # Same for referenced blobs
        if header.blobs is not None:
            for blobref in header.blobs:
                self.assert_relation_exists(objid, blobref.objid, None)

    def get_blob_body(self, objid: ObjIDType) -> bytes:
        """Returns the body of the given blob object"""
        full_obj_path = self.repopath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.BLOB, header.type)
            self.assert_object_header(objid, header, *stream_len_and_sha1(stream))
            self.assertIsNone(header.entries)
            self.assertIsNone(header.blobs)
            self.assertIsNone(header.stats)

            body = stream.read()
            self.assertEqual(header.length, len(body))
            return body

    def assert_file_obj(self, objid: ObjIDType, expected_file: ExpectedFile):
        """Asserts that the given object is a file object"""
        full_obj_path = self.repopath(repoobject.make_object_path(objid))
        expected_file_bytes = expected_file.encode("utf-8")
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.FILE, header.type)
            self.assert_object_header(objid, header, *stream_len_and_sha1(stream))
            self.assertIsNone(header.entries)

            body = stream.read()
            self.assertEqual(header.length, len(body))
            if header.blobs is None:
                # File contents are inlined. Check them directly
                self.assertEqual(expected_file_bytes, body)
            else:
                self.assertEqual(b"", body)
                # Assemble the blobs into the file contents
                assert header.file_size is not None
                actual_file = bytearray(header.file_size)
                for blobref in header.blobs:
                    blob_contents = self.get_blob_body(blobref.objid)
                    actual_file[
                        blobref.pos : blobref.pos + len(blob_contents)
                    ] = blob_contents

                self.assertEqual(expected_file_bytes, actual_file)

    def assert_symlink_obj(self, objid: ObjIDType, expected_symlink: ExpectedSymlink):
        """Asserts that the given object is a symlink object"""
        full_obj_path = self.repopath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.SYMLINK, header.type)
            self.assert_object_header(objid, header, *stream_len_and_sha1(stream))
            self.assertIsNone(header.entries)
            self.assertIsNone(header.blobs)

            target = stream.read()
            self.assertEqual(header.length, len(target))
            self.assertEqual(
                expected_symlink.encode("utf-8", errors="surrogateescape"), target
            )

    def assert_dir_object(self, objid: ObjIDType, expected: ExpectedDir):
        """Asserts that the given object is a dir object with the given
        entries

        """
        full_obj_path = self.repopath(repoobject.make_object_path(objid))
        with full_obj_path.open("rb") as stream:
            header = ObjectHeader.from_stream(stream)
            self.assertEqual(ObjectType.TREE, header.type)
            self.assert_object_header(objid, header, *stream_len_and_sha1(stream))

            # No body
            self.assertEqual(b"", stream.read())
            self.assertEqual(0, header.length)

            assert header.entries is not None
            obj_entries = {os.fsdecode(e.name): e.objid for e in header.entries}

            # All the same names should be set
            self.assertSetEqual(set(expected), set(obj_entries))

            for name, expected_val in expected.items():
                referenced_objid = obj_entries[name]
                if isinstance(expected_val, ExpectedDir):
                    # Another directory
                    self.assert_dir_object(referenced_objid, expected_val)
                elif isinstance(expected_val, ExpectedFile):
                    # A file
                    self.assert_file_obj(referenced_objid, expected_val)
                elif isinstance(expected_val, ExpectedSymlink):
                    # A symlink
                    self.assert_symlink_obj(referenced_objid, expected_val)
                else:
                    raise Exception

    def assert_backupsets(self, *snapshots: dict[str | os.PathLike[str], ExpectedDir]):
        """Asserts that one or more snapshots exist both in the database and on disk

        Each given snapshot is a mapping of root directories to the expected files
        that were backed up from that root.

        Each snapshot in the snapshots list is a separate backup. So if a
        test does one backup, then there should be one snapshot given. If a
        test does two backups, then it should provide two structures
        describing the contents of each backup.

        Each snapshot dict usually has one entry: the root that was backed up. For tests
        involving multiple roots, the snapshot dict will have an entry for each one.
        """
        with self.back.db.cursor() as cursor:
            cursor.execute("SELECT distinct timestamp FROM snapshots ORDER BY timestamp")
            backup_dates: list[str] = [r[0] for r in cursor]

        self.assertEqual(
            len(snapshots),
            len(backup_dates),
            "Different number of snapshots given than are in the database",
        )

        # Read in the snapshot files saved to the filesystem
        snapshot_files = [
            models.Snapshot.model_validate_json(p.read_text())
            for p in (self.repodir / "snapshots").iterdir()
        ]

        for snapshot, date in zip(snapshots, backup_dates):
            # This snapshot has len(snapshot) roots, so should have that many
            # Snapshot objects in the database.
            db_snaphots = list(
                self.back.db.query(
                    models.Snapshot, "SELECT * FROM snapshots WHERE timestamp=?", (date,)
                )
            )
            self.assertEqual(
                len(snapshot), len(db_snaphots), "DB has wrong number of snapshot roots"
            )

            # Make sure each snapshot from the database also exists on the filesystem
            for db_s in db_snaphots:
                self.assertTrue(
                    any(db_s == file_s for file_s in snapshot_files),
                    f"Database snapshot not found in filesystem: {db_s!s}",
                )

            # Now correlated them with each other
            roots = {s.path: s.root for s in db_snaphots}
            for root_dir_name, root_dir_expected in snapshot.items():
                # Root of a snapshot is always a directory
                assert isinstance(root_dir_expected, dict)

                root_objid = roots[os.fspath(root_dir_name)]
                self.assert_dir_object(root_objid, root_dir_expected)


class TestBackup(AssertObjHelperMixin, BackathonTest):
    """Tests backup functionality

    These tests invoke the backup routine and check that the correct data is written
    to the repository AND the correct information is saved to the database
    """

    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo()

        # For these tests, always inline files unless the test specifies otherwise.
        # This ensures these tests work independently of the default inline threshold
        # changing
        # The specific value here needs to be larger than any test files this test case
        # uses
        self.back.db.config_set("inline-threshold", 2**20)

    def _disable_inlining(self):
        """Disables inlining for the test"""
        self.back.db.config_set("inline-threshold", 0)

    def test_objects_committed(self):
        """Check that objects saved to the database are written to the filesystem repo
        with a well-formed header and the correct type

        """
        self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.back.backup()

        all_objs = list(
            self.back.db.query(
                models.Object,
                "SELECT * FROM objects",
            )
        )
        # Expect 2 tree and 1 file objects
        self.assertEqual(3, len(all_objs))
        for obj in all_objs:
            obj_filepath = repoobject.make_object_path(obj.objid)
            # See that this object actually exists in the backup repo
            full_path = self.repopath(obj_filepath)
            self.assertTrue(full_path.is_file())

            # Read in the object and make sure its header is well-formed and has the same
            # type as we expect according to the database
            with full_path.open("rb") as stream:
                header = ObjectHeader.from_stream(stream)
                self.assertEqual(obj.type, header.type)

    def test_backup(self):
        """Tests backing up two files in two directories"""
        self.create_file("dir/file1", "file contents")
        self.create_file("dir/file2", "file contents 2")
        self.back.scan()
        self.assert_fsentry_count(self.back, 4)
        self.back.backup()
        entries = list(self.back.db.query(models.FSEntry, "SELECT * FROM fsentry"))
        self.assertTrue(all(entry.objid is not None for entry in entries))
        self.assert_object_count(self.back, 4)

        self.assert_backupsets(
            {
                self.backupdir: ExpectedDir(
                    {
                        "dir": ExpectedDir(
                            {
                                "file1": ExpectedFile("file contents"),
                                "file2": ExpectedFile("file contents 2"),
                            }
                        )
                    }
                )
            }
        )

    def test_backup_no_inline(self):
        """Tests backing up files with inlining disabled"""
        self._disable_inlining()
        self.create_file("file1", "file contents")
        self.back.scan()
        self.back.backup()
        self.assert_object_count(self.back, 3)
        self.assert_backupsets(
            {self.backupdir: ExpectedDir(file1=ExpectedFile("file contents"))}
        )

    def test_backup_identical_files(self):
        """Tests deduplication when backing up two identical files

        Specifically, we expect one fewer blob object in the database and repo"""
        self._disable_inlining()
        self.create_file("file1", "file contents")
        self.create_file("file2", "file contents")
        self.back.scan()
        self.back.backup()
        self.assert_backupsets(
            {
                self.backupdir: ExpectedDir(
                    {
                        "file1": ExpectedFile("file contents"),
                        "file2": ExpectedFile("file contents"),
                    }
                )
            }
        )
        objects = list(self.back.db.query(models.Object, "SELECT * FROM objects"))
        # Expect a tree, two file, and one blob objects
        self.assertEqual(4, len(objects))
        blobs = [o for o in objects if o.type == ObjectType.BLOB]
        self.assertEqual(1, len(blobs))
        contents = self.get_blob_body(blobs[0].objid)
        self.assertEqual(b"file contents", contents)

    def test_backup_hardlinked_files(self):
        """Tests that two hardlinked files count as identical for deduplication"""
        file = self.create_file("file1", "file contents")
        os.link(file, file.parent / "file2")
        self.back.scan()
        self.back.backup()
        self.assert_backupsets(
            {
                self.backupdir: ExpectedDir(
                    {
                        "file1": ExpectedFile("file contents"),
                        "file2": ExpectedFile("file contents"),
                    }
                )
            }
        )
        # The file objects should be identical, so just 2 objects should get uploaded:
        # a dir object and a file object (dir will have 2 entries to the one file)
        self.assert_object_count(self.back, 2)

    def test_file_disappeared(self):
        """Tests that a file which has disappeared after scanning gets removed from
        the database after backup

        The backup process should remove the entry from the fsentry table

        """
        file = self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.assert_fsentry_count(self.back, 3)

        file.unlink()
        self.back.backup()

        self.assert_fsentry_count(self.back, 2)
        self.assert_object_count(self.back, 2)

        self.assert_backupsets({self.backupdir: ExpectedDir({"dir": ExpectedDir()})})

    def test_file_changes_to_dir(self):
        """Tests if a file changes to a directory after scan before backup

        The backup routine should back up the directory, modifying the fsentry
        to match the actual state of the filesystem

        """
        file = self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.assert_fsentry_count(self.back, 3)
        self.assertTrue(stat.S_ISREG(self.back.db.get_fsentry(file).st_mode or 0))

        file.unlink()
        file.mkdir()
        self.back.backup()

        # The fsentry for that path should now be a directory
        self.assertTrue(stat.S_ISDIR(self.back.db.get_fsentry(file).st_mode or 0))

        # Should still be 3 entries, but the file entry will be changed to a tree entry
        self.assert_fsentry_count(self.back, 3)

        # 3 objects in the repo: root tree, tree for "dir/", and tree for "dir/file/"
        self.assert_object_count(self.back, 3)

    def test_file_disappeared_after_lstat(self):
        """Tests when a file disappears after the initial lstat call, but
        before the file is opened for reading

        """
        # We want to delete the file after the initial lstat() call,
        # but before the file is opened for reading later on, to test this
        # race condition. So we patch os.lstat to delete the file right after
        # the lstat call.
        file = self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.assert_fsentry_count(self.back, 3)

        real_lstat = os.lstat

        def lstat(path):
            stat_result = real_lstat(path)
            if path == str(file).encode(sys.getfilesystemencoding()):
                file.unlink()
            return stat_result

        self.stack.enter_context(
            mock.patch(
                "os.lstat",
                lstat,
            )
        )

        self.back.backup()

        # Did the file properly get deleted? If not, the patch code above may be
        # broken or something changed in the backup code to not trigger the unlink.
        self.assertFalse(file.exists())

        # Expect only the root and the directory "dir" within
        self.assert_fsentry_count(self.back, 2)
        self.assert_object_count(self.back, 2)

        self.assert_backupsets({self.backupdir: ExpectedDir({"dir": ExpectedDir()})})

    def test_permission_denied_file(self):
        """A permission denied error when reading a file shouldn't cause the
        backup to fail, and other files should still get backed up"""
        self.create_file("dir/file1", "file contents")
        self.back.scan()
        self.assert_fsentry_count(self.back, 3)

        def raise_permissiondeined(path):
            raise PermissionError("Permission Denied")

        with mock.patch("backathon.backup._open_file", raise_permissiondeined):
            with self.assertLogs("backathon.backup", level=logging.WARNING) as cm:
                self.back.backup()

        self.assertIn("Error when reading: Permission Denied", cm.output[0])

        self.assert_fsentry_count(self.back, 2)
        self.assert_object_count(self.back, 2)
        self.assert_backupsets({self.backupdir: ExpectedDir({"dir": ExpectedDir()})})

    def test_invalid_utf8_filename(self):
        """Tests that a file with invalid utf-8 in the name can be backed up"""
        name = os.fsdecode(b"\xff\xffhello\xff\xff")
        self.create_file(name, "file contents")
        self.back.scan()
        self.back.backup()

        self.assert_backupsets(
            {self.backupdir: ExpectedDir({name: ExpectedFile("file contents")})}
        )

    def test_symlink(self):
        """Tests that symlinks are saved properly"""
        self.create_file("file1", "file contents")

        pathobj = self.backuppath("file2")
        pathobj.symlink_to("file1")

        self.back.scan()
        self.back.backup()

        self.assert_backupsets(
            {
                self.backupdir: ExpectedDir(
                    {
                        "file1": ExpectedFile("file contents"),
                        "file2": ExpectedSymlink("file1"),
                    }
                )
            }
        )

    def test_invalid_utf8_symlink(self):
        """Similar to the test for invalid filenames, but for symlinks: make
        sure we don't error if a symlink target is invalid utf-8

        """
        target = os.fsdecode(b"\xff\xffhello\xff\xff")

        pathobj = self.backuppath("badsymlink")
        pathobj.symlink_to(target)

        self.back.scan()
        self.back.backup()

        self.assert_backupsets(
            {self.backupdir: ExpectedDir({"badsymlink": ExpectedSymlink(target)})}
        )

    def test_second_snapshot_file_update(self):
        """Tests taking a second snapshot of a file"""
        file = self.create_file("file", "contents 1")
        self.back.scan()
        self.back.backup()
        self.assert_backupsets(
            {self.backupdir: ExpectedDir({"file": ExpectedFile("contents 1")})}
        )

        file.write_text("contents 2")
        self.back.scan()
        self.back.backup()

        self.assert_backupsets(
            {self.backupdir: ExpectedDir({"file": ExpectedFile("contents 1")})},
            {self.backupdir: ExpectedDir({"file": ExpectedFile("contents 2")})},
        )

    def test_compression(self):
        """Tests that uploaded objects are compressed if compression is enabled"""
        self.back.db.config_set("enable-compression", True)
        file = self.create_file("file1", "Hello, world!")
        self.back.scan()
        self.back.backup()
        entry = self.back.db.get_fsentry(file)

        assert entry.objid is not None
        obj_path = self.repopath(repoobject.make_object_path(entry.objid))
        contents = obj_path.read_bytes()

        # Should start with the zlib magic byte
        self.assertEqual(0x78, contents[0])
        decompressed = zlib.decompress(contents)
        buf = io.BytesIO(decompressed)
        header = models.ObjectHeader.from_stream(buf)
        self.assertEqual(ObjectType.FILE, header.type)
        self.assertEqual(b"Hello, world!", buf.read())

        # The database should store the compressed size and sha1, not that of the
        # decompressed data
        db_obj = next(
            self.back.db.query(
                models.Object, "SELECT * FROM objects WHERE objid=?", (entry.objid,)
            )
        )
        self.assertEqual(len(contents), db_obj.uploaded_size)
        self.assertEqual(hashlib.sha1(contents).digest(), db_obj.sha1)
