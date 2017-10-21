"""
This module holds utilities to support the unit tests
"""
import hashlib

from gbackup import objects

class FileCacheDummy:
    def get_file_cache(self, path, inode, mtime, size):
        return None

    def set_file_cache(self, path, inode, mtime, size, objid):
        return None

class ControllerDummy:
    def __init__(self, tree_root):
        assert isinstance(tree_root, objects.Tree)
        self.root = tree_root

    def backup(self):
        obj_iter = self.root.backup()

        try:
            blob = next(obj_iter)
            while True:
                objid = hashlib.sha256(blob).hexdigest()
                blob = obj_iter.send(objid)
        except StopIteration:
            pass
