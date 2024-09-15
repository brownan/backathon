import hashlib
import io
import logging
import os
import pathlib
import stat
import tempfile
import unittest.mock
import warnings

import backathon.encryption.nacl
from backathon import models, repoobject
from backathon.encryption.base import EncrypterBase
from tests.base import BackathonTest


class AssertionHandler(logging.Handler):
    """A logging handler that will raise an AssertionError if any warnings or
    errors are emitted

    Use this in tests by attaching it to a logger to make sure no warnings are
    emitted by that logger
    """

    def emit(self, record):
        raise AssertionError("Unexpected warning: " + self.format(record))


class TestRestore(BackathonTest):
    """Tests restore functionality, and some other end-to-end scan and backup
    functionality

    """

    # Set by subclasses that test encryption
    password: str | None = None
    encrypter: EncrypterBase | None = None

    def setUp(self):
        super().setUp()
        self.back = self.init_basic_repo(self.encrypter)
        self.restoredir = self.stack.enter_context(tempfile.TemporaryDirectory())

        self.handler = AssertionHandler()
        self.handler.setLevel(logging.WARNING)
        logging.getLogger("backathon.restore").addHandler(self.handler)

        # Also watch for emitted warnings
        self.w = self.stack.enter_context(warnings.catch_warnings(record=True))

    def tearDown(self):
        logging.getLogger("backathon.restore").removeHandler(self.handler)
        super().tearDown()
        if self.w:
            self.fail(
                "Warnings emitted:\n{}".format(
                    "\n".join(
                        [
                            warnings.formatwarning(
                                w.message, w.category, w.filename, w.lineno, line=w.line
                            )
                            for w in self.w
                        ]
                    )
                )
            )

    def assert_restored_file(self, path, contents):
        fullpath = pathlib.Path(self.restoredir, path)
        self.assertEqual(
            contents,
            fullpath.read_text(),
        )

    def _get_snapshot(self) -> models.Snapshot:
        """Helper function to get a single snapshot from the database"""
        return next(
            self.back.db.query(models.Snapshot, "SELECT * FROM snapshots LIMIT 1")
        )

    def test_simple_restore(self):
        self.create_file("file1", "contents1")
        self.create_file("dir/file2", "contents2")
        self.back.scan()
        self.back.backup()

        ss = self._get_snapshot()

        self.back.restore(ss.root, self.restoredir, self.password)

        self.assert_restored_file("file1", "contents1")
        self.assert_restored_file("dir/file2", "contents2")

    def test_restore_mode(self):
        file_a = self.create_file("file1", "contents")
        file_a.chmod(0o777)

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()
        self.back.restore(ss.root, self.restoredir, self.password)

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

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()
        self.back.restore(ss.root, self.restoredir, self.password)

        file_b = pathlib.Path(self.restoredir, "file1")
        stat_result = file_b.stat()
        self.assertEqual(1, stat_result.st_uid)
        self.assertEqual(1, stat_result.st_gid)

    def test_restore_time(self):
        file_a = self.create_file("file1", "contents")
        os.utime(file_a, ns=(123456789, 987654321))

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()
        self.back.restore(ss.root, self.restoredir, self.password)

        file_b = pathlib.Path(self.restoredir, "file1")

        stat_result = file_b.stat()
        self.assertEqual(
            123456789,
            stat_result.st_atime_ns,
        )
        self.assertEqual(
            987654321,
            stat_result.st_mtime_ns,
        )

    def test_restore_time_dir(self):
        dir_a = pathlib.Path(self.backupdir, "dir1")
        dir_a.mkdir()
        os.utime(dir_a, ns=(123456789, 987654321))

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()

        # The directory atime gets reset before we back it up, so just check
        # that whatever value it had when it was backed up, that's what gets
        # restored.
        entry = self.back.db.get_fsentry(self.backuppath("dir1"))
        assert entry.objid is not None
        obj_path = self.repopath(repoobject.make_object_path(entry.objid))
        with obj_path.open("rb") as fobj:
            encrypter = self.back.get_encrypter()
            if self.password:
                encrypter.unlock(self.password)
            fobj = encrypter.decrypt(fobj)
            header = models.ObjectHeader.from_stream(io.BytesIO(fobj))
        assert header.stats is not None
        atime = header.stats.atime

        self.back.restore(ss.root, self.restoredir, self.password)

        dir1 = pathlib.Path(self.restoredir, "dir1")

        stat_result = dir1.stat()
        self.assertEqual(
            987654321,
            stat_result.st_mtime_ns,
        )
        self.assertEqual(
            atime,
            stat_result.st_atime_ns,
        )

    def test_restore_multiple_revisions(self):
        self.create_file("file", "contents A")

        self.back.scan()
        self.back.backup()

        self.create_file("file", "new contents")

        self.back.scan()
        self.back.backup()

        snapshots = list(
            self.back.db.query(
                models.Snapshot, "SELECT * FROM snapshots ORDER BY timestamp"
            )
        )

        self.assertEqual(2, len(snapshots))

        # two root dirs, two file objs
        self.assert_object_count(self.back, 4)

        restoredir = pathlib.Path(self.restoredir)
        self.back.restore(snapshots[0].root, restoredir / "ss1", self.password)
        self.back.restore(snapshots[1].root, restoredir / "ss2", self.password)

        file1 = restoredir / "ss1" / "file"
        file2 = restoredir / "ss2" / "file"

        self.assertEqual("contents A", file1.read_text())
        self.assertEqual("new contents", file2.read_text())

    def test_restore_single_file(self):
        """Tests restoring a single file instead of an entire directory"""
        self.create_file("file", "contents")

        self.back.scan()
        self.back.backup()

        ss = self._get_snapshot()

        # Should just be one child
        children = list(
            self.back.db.query(
                models.ObjectRelation,
                "SELECT * FROM object_relations WHERE parent=?",
                (ss.root,),
            )
        )
        self.assertEqual(1, len(children))
        objid = children[0].child

        filename = pathlib.Path(self.restoredir, "my_file")
        self.back.restore(objid, filename, self.password)

        self.assertEqual("contents", filename.read_text())

    def test_restore_invalid_utf8_filename(self):
        name = os.fsdecode(b"\xFF\xFFHello\xFF\xFF")

        self.assertRaises(UnicodeEncodeError, name.encode, "utf-8")

        self.create_file(name, "contents")

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()

        self.back.restore(ss.root, self.restoredir, self.password)

        self.assert_restored_file(name, "contents")

    def test_restore_large_file(self):
        """This file should take more than one blob to save, so it tests
        routines that must operate on multiple blobs.

        """
        self.back.db.config_set("inline-threshold", 0)
        self.back.db.config_set("chunk-threshold", 0)
        self.back.db.config_set("chunk-size", 2**20)
        infile = self.create_file("bigfile", "")
        h = hashlib.md5()

        with infile.open("wb") as f:
            for i in range(5):
                # Each block must be different so they don't get deduplicated and
                # we ensure the backup and restore routines are handling files made of
                # multiple different blobs
                block = i.to_bytes() * 1024 * 1024
                h.update(block)
                f.write(block)

        self.back.scan()
        self.back.backup()

        blobs = list(
            self.back.db.query(models.Object, "SELECT * FROM objects WHERE type='blob'")
        )
        self.assertEqual(5, len(blobs))

        ss = self._get_snapshot()
        self.back.restore(ss.root, self.restoredir, self.password)

        outfile = pathlib.Path(self.restoredir, "bigfile")
        h2 = hashlib.md5()
        with outfile.open("rb") as f:
            while True:
                a = f.read(64 * 2**10)
                if not a:
                    break
                h2.update(a)
        self.assertEqual(h.hexdigest(), h2.hexdigest())

    def test_restore_symlink(self):
        """Tests backing up and restoring symlinks"""
        path = self.backuppath("linkname")
        os.symlink("this is the link target", path)

        self.back.scan()
        self.back.backup()
        ss = self._get_snapshot()
        self.back.restore(ss.root, self.restoredir, self.password)

        self.assertEqual(
            os.readlink(pathlib.Path(self.restoredir, "linkname")),
            "this is the link target",
        )


class TestRestoreWithCompression(TestRestore):
    def setUp(self):
        super().setUp()
        self.back.db.config_set("enable-compression", True)


class TestRestoreWithEncryption(TestRestore):
    def setUp(self):
        self.password = "This is my password!"

        # Set the ops limit and mem limit low so the tests don't take forever
        import nacl.pwhash.argon2id

        with (
            unittest.mock.patch.object(
                backathon.encryption.nacl.NaclEncrypter,
                "DEFAULT_OPSLIMIT",
                nacl.pwhash.argon2id.OPSLIMIT_MIN,
            ),
            unittest.mock.patch.object(
                backathon.encryption.nacl.NaclEncrypter,
                "DEFAULT_MEMLIMIT",
                nacl.pwhash.argon2id.MEMLIMIT_MIN,
            ),
        ):
            # Initialize our encrypter
            self.encrypter = backathon.encryption.nacl.NaclEncrypter.new(self.password)
        super().setUp()

    def test_not_plaintext(self):
        """Tests that the plaintext of a file doesn't appear in the object
        payload on disk"""
        self.back.db.config_set("inline-threshold", 0)

        self.create_file("secret_file", "super secret contents")

        self.back.scan()
        self.back.backup()

        blobs = list(
            self.back.db.query(models.Object, "SELECT * FROM objects WHERE type='blob'")
        )
        self.assertEqual(1, len(blobs))
        blob = blobs[0]

        path = self.repopath(repoobject.make_object_path(blob.objid))
        self.assertTrue(path.exists())
        contents = path.read_bytes()
        self.assertNotIn(b"super secret contents", contents)


class TestRestoreEncryptionAndCompression(
    TestRestoreWithCompression, TestRestoreWithEncryption
):
    pass
