class FixedChunker:
    """Chunker that iterates over a file object and yields fixed size
    chunks.

    Yields (position, byteslike) for each chunk in a given file object

    """

    def __init__(self, fileobj, chunk_size: int):
        self.f = fileobj
        self.pos = 0
        self.chunk_size = chunk_size

    def __iter__(self):
        chunk_size = self.chunk_size
        while True:
            pos = self.f.tell()
            data = self.f.read(chunk_size)
            if not data:
                return
            yield pos, data
