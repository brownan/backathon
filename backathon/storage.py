import pathlib
import shutil
import os

class StorageBase:
    """Base class defining the storage interface"""
    def get_params(self):
        """Returns the parameters to initialize this class

        This is essentially used to serialize the instance
        """
        raise NotImplementedError()

    def upload_file(self, name, content):
        """Uploads a file

        :param name: The file name, including path components
        :param content: A file-like object open for reading
        """
        raise NotImplementedError()

    def download_file(self, name):
        """Downloads a file

        :param name: The name of the file to download
        :returns: file_metadata, file_obj

        """
        raise NotImplementedError()

    def delete(self, name):
        """Deletes a file"""
        raise NotImplementedError()

    def get_files_by_prefix(self, prefix):
        """Returns all files that have the given prefix

        The prefix can be a directory or a file prefix. All files below that
        prefix in the tree will be returned.
        """
        raise NotImplementedError()

class FilesystemStorage(StorageBase):
    """A filesystem storage class with an api compatible with our B2 class"""

    def __init__(self, base_dir):
        self.base_dir = pathlib.Path(base_dir)

    def _get_metadata(self, path: pathlib.Path):
        # Not all metadata that B2 calls return is computed here. Feel free
        # to add more as we need it.
        return {
            'fileName': str(path.relative_to(self.base_dir))
        }

    def get_params(self):
        return {
            'base_dir': self.base_dir
        }

    def upload_file(self, name, content):
        path = self.base_dir / name

        os.makedirs(path.parent, exist_ok=True)
        with path.open(mode="wb") as fileout:
            shutil.copyfileobj(content, fileout)

        return self._get_metadata(path)

    def download_file(self, name):
        path = self.base_dir / name

        return self._get_metadata(path), path.open("rb")

    def delete(self, name):
        path = self.base_dir / name

        path.unlink()

    def get_files_by_prefix(self, prefix):
        path = self.base_dir / prefix

        # This is a little tricky because the last component of the path
        # could be a directory name, a file name, a partial directory name,
        # a partial file name, or both a partial directory and file name. We
        # won't usually have to do anything like that, but this keeps the
        # same api as the B2 list file names API.

        # Short circuit single file case
        if path.is_file():
            yield self._get_metadata(path)
            return

        if not path.is_dir():
            prefix = path.name
            path = path.parent
        else:
            prefix = ""

        for entry in os.scandir(path):
            if entry.name.startswith(prefix):
                if entry.is_file():
                    yield self._get_metadata(pathlib.Path(entry))
                elif entry.is_dir():
                    for dirpath, dirnames, filenames in os.walk(entry):
                        for fname in filenames:
                            path = pathlib.Path(dirpath) / fname
                            yield self._get_metadata(path)


