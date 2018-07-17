class FixedChunker:
    """Chunker that iterates over a file object and yields fixed size
    chunks.

    Yields (position, byteslike) for each chunk in a given file object

    """
    def __init__(self, fileobj):
        self.f = fileobj
        self.pos = 0

    def _get_chunksize(self):
        return 2**20

    def __iter__(self):
        while True:
            pos = self.f.tell()
            data = self.f.read(self._get_chunksize())
            if not data:
                return
            yield pos, data

