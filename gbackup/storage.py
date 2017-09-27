"""
This module defines storage backends, which are an abstraction layer for the
actual backing storage. This lets us choose different services for storing
backups. This abstraction doesn't handle any encryption or compression. It is
only a storage layer.

The interface defined here is based on Django's storage abstraction,
but simplified for our needs
"""

class Storage:
    """The base class for all storage backends

    """
    def open(self, name, mode='rb'):
        return self._open(name, mode)

    def _open(self, name, mode):
        raise NotImplementedError()

class FileSystemStorage(Storage):
    def _open(self, name, mode):
        return open(name, mode)
