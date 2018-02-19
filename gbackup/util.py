
class BytesReader:
    """A file-like object that reads from a bytes-like object

    This is meant to efficiently read into a bytes-like object for methods
    and routines that expect a file-like object.

    The standard library io.BytesIO makes a copy of the bytes object during
    initialization, which can get expensive with large byte strings. With this
    class, a reference to the bytes object is passed in, and each call to
    read() returns a slice.

    The read() method always returns bytes, as opposed to say memoryview
    slices, because the primary motivation for this class is to pass it to
    umsgpack.unpack(), which expects byte objects to be returned from read().
    You can still pass a memoryview or bytearray in just fine, but bytes are
    copied to a byte object when returned from read().

    """
    def __init__(self, byteslike):
        self.buf = byteslike
        self.pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        raise False

    def close(self):
        self.buf = None

    def tell(self):
        return self.pos

    def readinto(self, b):
        size = min(len(b), len(self.buf)-self.pos)
        b[:] = self.buf[self.pos:size]
        return size

    def read(self, size=None):
        if size is None and self.pos == 0:
            self.pos = len(self.buf)
            return self.buf

        startpos = self.pos

        if size is None:
            size = len(self.buf)

        endpos = self.pos+size

        self.pos += size
        if self.pos > len(self.buf):
            self.pos = len(self.buf)
        ret = self.buf[startpos:endpos]
        if not isinstance(ret, bytes):
            ret = bytes(ret)
        return ret

    def seek(self, pos):
        self.pos = pos

