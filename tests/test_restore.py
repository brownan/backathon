import tempfile
import pathlib
import logging
import stat
import os
import unittest.mock
import hashlib

from django.db import connections

from backathon import models
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
    """Tests restore functionality, and some other end-to-end scan and backup
    functionality

    """
    # Set by subclasses that test encryption
    password = None

    def setUp(self):
        super().setUp()
        self.restoredir = self.stack.enter_context(
            tempfile.TemporaryDirectory()
        )

        self.handler = AssertionHandler()
        self.handler.setLevel(logging.WARNING)
        logging.getLogger("backathon.restore").addHandler(self.handler)

    def tearDown(self):
        logging.getLogger("backathon.restore").removeHandler(self.handler)
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
        self.backathon.scan()
        self.backathon.backup()

        ss = self.snapshot.get()

        self.backathon.restore(ss.root, self.restoredir, self.password)

        self.assert_restored_file("file1", "contents1")
        self.assert_restored_file("dir/file2", "contents2")

    def test_restore_mode(self):
        file_a = self.create_file("file1", "contents")
        file_a.chmod(0o777)

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()
        self.backathon.restore(ss.root, self.restoredir, self.password)

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

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()
        self.backathon.restore(ss.root, self.restoredir, self.password)

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

    def test_restore_time(self):
        file_a = self.create_file("file1", "contents")
        os.utime(file_a, ns=(123456789, 987654321))

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()
        self.backathon.restore(ss.root, self.restoredir, self.password)

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

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()

        # The directory atime gets reset before we back it up, so just check
        # that whatever value it had when it was backed up, that's what gets
        # restored.
        key = self.repo.encrypter.get_decryption_key(self.password)
        tree = ss.root.children.get()
        payload = self.repo.get_object(tree.objid, key)
        from backathon.restore import unpack_payload
        info = list(unpack_payload(payload))[1]
        atime = info['atime']

        self.backathon.restore(ss.root, self.restoredir, self.password)

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

        self.backathon.scan()
        self.backathon.backup()

        self.create_file("file", "new contents")

        self.backathon.scan()
        self.backathon.backup()

        snapshots = list(self.snapshot.order_by("date"))

        self.assertEqual(2, len(snapshots))
        self.assertEqual(
            6,
            self.object.count()
        )

        restoredir = pathlib.Path(self.restoredir)
        self.backathon.restore(snapshots[0].root, restoredir /"ss1", self.password)
        self.backathon.restore(snapshots[1].root, restoredir /"ss2", self.password)

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

        self.backathon.scan()
        self.backathon.backup()

        root = self.snapshot.get().root

        # Should just be one child
        inode = root.children.get()

        filename = pathlib.Path(self.restoredir, "my_file")
        self.backathon.restore(inode, filename, self.password)

        self.assertEqual(
            "contents",
            filename.read_text()
        )

    def test_restore_invalid_utf8_filename(self):
        name = os.fsdecode(b"\xFF\xFFHello\xFF\xFF")

        self.assertRaises(
            UnicodeEncodeError,
            name.encode,
            "utf-8"
        )

        self.create_file(name, "contents")

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()

        self.backathon.restore(ss.root, self.restoredir, self.password)

        self.assert_restored_file(name, "contents")

    def test_restore_large_file(self):
        """This file should take more than one block to save, so it tests
        routines that must operate on multiple blocks.

        """
        infile = self.create_file("bigfile", "")
        block = b"\0"*1024*1024
        h = hashlib.md5()

        with infile.open("wb") as f:
            for _ in range(50):
                h.update(block)
                f.write(block)

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()
        self.backathon.restore(ss.root, self.restoredir, self.password)

        outfile = pathlib.Path(self.restoredir, "bigfile")
        h2 = hashlib.md5()
        with outfile.open("rb") as f:
            while True:
                a = f.read(64*2**10)
                if not a:
                    break
                h2.update(a)
        self.assertEqual(
            h.hexdigest(),
            h2.hexdigest()
        )

    def test_restore_symlink(self):
        """Tests backing up and restoring symlinks"""
        path = self.path("linkname")
        os.symlink("this is the link target", path)

        self.backathon.scan()
        self.backathon.backup()
        ss = self.snapshot.get()
        self.backathon.restore(ss.root, self.restoredir, self.password)

        self.assertEqual(
            os.readlink(pathlib.Path(self.restoredir, "linkname")),
            "this is the link target"
        )

class TestRestoreWithCompression(TestRestore):
    def setUp(self):
        super().setUp()
        self.repo.set_compression(True)

class TestRestoreWithEncryption(TestRestore):
    def setUp(self):
        super().setUp()

        from backathon import encryption

        self.password = "This is my password!"

        # Set the ops limit and mem limit low so the tests don't take forever
        import nacl.pwhash.argon2id
        self.stack.enter_context(
            unittest.mock.patch.object(encryption.NaclSealedBox, "OPSLIMIT",
                                       nacl.pwhash.argon2id.OPSLIMIT_MIN)
        )
        self.stack.enter_context(
            unittest.mock.patch.object(encryption.NaclSealedBox, "MEMLIMIT",
                                       nacl.pwhash.argon2id.MEMLIMIT_MIN)
        )

        # Initialize our encrypter
        encrypter = encryption.NaclSealedBox.init_new(
            self.password
        )
        self.repo.set_encrypter(encrypter)

    def test_not_plaintext(self):
        """Tests that the plaintext of a file doesn't appear in the object
        payload on disk"""
        self.create_file("secret_file", "super secret contents")

        self.backathon.scan()
        self.backathon.backup()

        ss = self.snapshot.get()
        tree = ss.root
        inode = tree.children.get()
        blob = inode.children.get()

        path = pathlib.Path(self.datadir, "objects",
                            blob.objid.hex()[:3],
                            blob.objid.hex())
        self.assertTrue(path.exists())
        contents = path.read_bytes()
        self.assertFalse(
            b"super secret contents" in contents
        )

class TestRestoreEncryptionAndCompression(TestRestoreWithCompression,
                                          TestRestoreWithEncryption):
    pass