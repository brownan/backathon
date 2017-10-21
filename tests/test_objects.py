import os
from unittest import TestCase
from contextlib import ExitStack
import tempfile

import tests.utils
from gbackup import objects, cache

import msgpack

class TestBlob(TestCase):
    def test_blob_pack(self):
        blob = objects.Blob(b'test')
        contents = blob.backup()

        unpacker = msgpack.Unpacker(contents, use_list=False)
        self.assertEqual(next(unpacker), b'b')
        self.assertEqual(next(unpacker), (b'd', b'test'))

class TestInode(TestCase):
    def setUp(self):
        self.stack = ExitStack()

        self.tmpfile = tempfile.NamedTemporaryFile()
        self.tmpfile.write(b"This is a test")
        self.tmpfile.seek(0)

        conn = cache.get_db_conn(":memory:")
        self.cache = cache.FileCache(conn)

    def tearDown(self):
        self.stack.close()

    def test_backup(self):
        inode = objects.Inode(self.tmpfile.name, self.cache)

        backup = inode.backup()

        # First chunk should be a blob with the file contents
        chunk1 = next(backup)
        data = list(msgpack.Unpacker(chunk1, use_list=False))
        self.assertEqual(data[0], b'b')
        self.assertTupleEqual(
            data[1],
            (b'd', b'This is a test'),
        )

        chunk2 = backup.send(b'chunk1id')
        data = list(msgpack.Unpacker(chunk2, use_list=False))
        self.assertEqual(data[0], b'i')

        # Check size
        self.assertEqual(b's', data[1][0])
        self.assertEqual(14, data[1][1])

        chunks = list(e for e in data if e[0] == b'd')
        self.assertEqual(1, len(chunks))
        self.assertTupleEqual(
            chunks[0],
            (b'd', 0, b'chunk1id')
        )

        with self.assertRaises(StopIteration) as e:
            backup.send(b'chunk2id')
            self.assertEqual(e.value, b'chunk2id')

class TestTree(TestCase):
    def setUp(self):
        self.stack = ExitStack()

        self.srcdir = self.stack.enter_context(
            tempfile.TemporaryDirectory()
        )
        self.path = lambda x: os.path.join(self.srcdir, x)

    def tearDown(self):
        self.stack.close()

    def test_empty_dir(self):
        tree = objects.Tree(self.srcdir, tests.utils.FileCacheDummy())
        tree.update()

        backup = tree.backup()

        chunk1 = next(backup)
        data = list(msgpack.Unpacker(chunk1, use_list=False))
        self.assertEqual(data[0], b't')
        self.assertEqual(data[1][0], b'u')
        self.assertEqual(data[2][0], b'g')
        self.assertEqual(data[3][0], b'm')
        self.assertEqual(data[4][0], b'ct')
        self.assertEqual(data[5][0], b'mt')
        self.assertEqual(len(data), 6)

        with self.assertRaises(StopIteration) as e:
            backup.send(b"chunk1id")
            self.assertEqual(e.value, b'chunk1id')

    def test_dir_with_file(self):
        with open(self.path("file1"), "wb") as outfile:
            outfile.write(b"test file contents")

        tree = objects.Tree(self.srcdir, tests.utils.FileCacheDummy())
        tree.update()

        backup = tree.backup()

        obj1 = next(backup)
        data = list(msgpack.Unpacker(obj1, use_list=False))
        # First object should be the blob
        self.assertEqual(b'b', data[0])
        self.assertTupleEqual(data[1], (b'd', b'test file contents'))

        obj2 = backup.send(b'blobid')
        data = list(msgpack.Unpacker(obj2, use_list=False))
        # second object should be the inode
        self.assertEqual(data[0], b'i')
        chunks = list(d for d in data if d[0] == b'd')
        self.assertEqual(1, len(chunks))
        self.assertTupleEqual(chunks[0], (b'd', 0, b'blobid'))

        obj3 = backup.send(b'inodeid')
        data = list(msgpack.Unpacker(obj3, use_list=False))
        # Third object should be the tree
        self.assertEqual(data[0], b't')
        entries = list(e for e in data if e[0] == b'e')
        self.assertEqual(1, len(entries))
        self.assertTupleEqual(
            (b'e', b'file1', b'inodeid'),
            entries[0]
        )
