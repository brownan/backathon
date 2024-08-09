import stat

from backathon import models
from backathon.encryption.null import NullConfig, NullEncrypter
from backathon.repository import Backathon
from backathon.storage.local import LocalStorage, LocalStorageConfig
from tests.base import BackathonTest


class TestScan(BackathonTest):
    """Tests the scan functionality of the FSEntry class"""

    def test_scan(self):
        """Tests that a basic scan creates the expected fsentries"""
        self.create_file("dir/file1", "file contents")
        self.create_file("dir2/file2", "another file contents")
        back = self.init_basic_repo()
        back.scan()
        entries = list(back.db.get_objects(models.FSEntry, "SELECT * FROM fsentry"))
        self.assertEqual(5, len(entries))
        names = set(e.decoded_path.name for e in entries)
        self.assertSetEqual(
            {"file1", "file2", "dir", "dir2", self.backupdir.name},
            names,
        )

    def test_deleted_file(self):
        """Tests that scanning after deleteing a file will delete its fsentry"""
        file = self.create_file("dir/file1", "file contents")
        back = self.init_basic_repo()
        back.scan()
        self.assertIsNotNone(back.db.get_fsentry(file))
        file.unlink()
        back.scan()
        self.assertIsNone(back.db.get_fsentry(file))

    def test_deleted_dir(self):
        """Tests that deleting a directory will also cause all descendent fsentries
        to be deleted

        """
        file = self.create_file("dir/file1", "file contents")
        back = self.init_basic_repo()
        back.scan()
        file.unlink()
        file.parent.rmdir()
        back.scan()
        self.assertIsNone(back.db.get_fsentry(file.parent))
        self.assertIsNone(back.db.get_fsentry(file))

    def test_replace_dir_with_file(self):
        """Tests that the scan properly handles a directory being replaced by a file
        with the same name

        """
        file = self.create_file("dir/file1", "file contents")
        back = self.init_basic_repo()
        back.scan()
        file.unlink()
        file.parent.rmdir()
        file.parent.write_text("another file contents")
        back.scan()

        self.assertIsNone(back.db.get_fsentry(file))
        entry = back.db.get_fsentry(file.parent)
        assert entry is not None
        assert entry.st_mode is not None
        children = list(
            back.db.get_objects(
                models.FSEntry, "SELECT * FROM fsentry WHERE parent=?", (entry.id,)
            )
        )
        self.assertEqual(0, len(children))
        self.assertTrue(stat.S_ISREG(entry.st_mode))

    def test_dir_no_permission(self):
        """If we don't have permission to read a directory, it should be
        considered empty"""
        file = self.create_file("dir/file1", "file contents")
        file.parent.chmod(0o000)
        back = self.init_basic_repo()
        back.scan()
        self.assertIsNotNone(back.db.get_fsentry(file.parent))
        self.assertIsNone(back.db.get_fsentry(file))

    def test_root_merge(self):
        """Tests that when adding a root which is an ancestor of an existing root,
        the two roots are merged

        """
        file = self.create_file("dir1/dir2/file", "file contents")
        back = Backathon.initialize(
            self.db_path,
            LocalStorage(LocalStorageConfig(base_path=self.repodir)),
            NullEncrypter(NullConfig()),
        )
        back.add_root(file.parent)
        back.scan()

        def get_roots() -> list[models.FSEntry]:
            return list(
                back.db.get_objects(
                    models.FSEntry, "SELECT * FROM fsentry WHERE parent IS NULL"
                )
            )

        roots = get_roots()
        self.assertEqual(1, len(roots))
        self.assertEqual(file.parent, roots[0].decoded_path)

        back.add_root(self.backupdir)
        back.scan()

        roots = get_roots()
        self.assertEqual(1, len(roots))
        self.assertEqual(self.backupdir, roots[0].decoded_path)
