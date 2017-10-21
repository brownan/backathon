import hashlib

from gbackup import objects

class Controller:
    """The controller class controls the iteration over a tree of backup
    objects in memory

    """
    def __init__(self, tree_root):
        assert isinstance(tree_root, objects.Tree)
        self.root = tree_root

    def backup(self):
        obj_iter = self.root.backup()

        try:
            blob = next(obj_iter)
            while True:

                objid = NotImplemented # TODO

                blob = obj_iter.send(objid)
        except StopIteration:
            pass
