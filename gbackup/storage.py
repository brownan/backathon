"""
This module defines storage backends, which are an abstraction layer for the
actual backing storage. This lets us choose different services for storing
backups. This abstraction doesn't handle any encryption or compression. It is
only a storage layer.

The interface defined here is based on Django's storage abstraction,
but simplified for our needs
"""

import os.path
import shutil
from collections import namedtuple

FileInfo = namedtuple("FileInfo", [
    'exists',
])

class Storage:
    """The base class for all storage backends

    """
    def put(self, name, buf):
        raise NotImplementedError()

    def get(self, name, buf):
        raise NotImplementedError()

    def stat(self, name):
        raise NotImplementedError()

class FileSystemStorage(Storage):
    def __init__(self, basedir):
        self.basedir = basedir

    def _getpath(self, name):
        return os.path.join(self.basedir, name)

    def put(self, name, buf):
        path = self._getpath(name)
        os.makedirs(
            os.path.basename(path),
            exist_ok=True,
        )
        with open(path, 'wb') as dstfile:
            shutil.copyfileobj(buf, dstfile)

    def get(self, name, buf):
        with open(self._getpath(name), "rb") as srcfile:
            shutil.copyfileobj(srcfile, buf)

    def stat(self, name):
        return FileInfo(
            exists=os.path.isfile(name),
        )
