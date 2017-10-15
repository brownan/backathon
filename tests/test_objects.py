import os
from unittest import TestCase
from contextlib import ExitStack
import tempfile

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
        data = {k: v for k, v in data[1:]}
        self.assertEqual(data[b's'], 14)
        self.assertEqual(len(data[b'd']), 1) # one chunk
        self.assertTupleEqual(
            data[b'd'][0],
            (0, b'chunk1id')
        )

        with self.assertRaises(StopIteration) as e:
            backup.send(b'chunk2id')
            self.assertEqual(e.value, b'chunk2id')