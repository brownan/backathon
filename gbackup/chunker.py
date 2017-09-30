import mmap

class FixedChunker:
    """Really dumb chunker that breaks a bytes-like object into fixed size
    chunks

    """
    def __init__(self, fileobj):
        self.f = fileobj
        self.pos = 0

    def _get_chunksize(self):
        return 2**20

    def __iter__(self):
        pos = self.f.tell()
        data = self.f.read(self._get_chunksize())
        if not data:
            raise StopIteration()
        yield pos, data

