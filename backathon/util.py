import json


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
        return False

    def close(self):
        self.buf = None

    def tell(self):
        return self.pos

    def readinto(self, b):
        size = min(len(b), len(self.buf) - self.pos)
        b[:] = self.buf[self.pos : size]
        return size

    def read(self, size=None):
        if size is None and self.pos == 0:
            self.pos = len(self.buf)
            return self.buf

        startpos = self.pos

        if size is None:
            size = len(self.buf)

        endpos = self.pos + size

        self.pos += size
        if self.pos > len(self.buf):
            self.pos = len(self.buf)
        ret = self.buf[startpos:endpos]
        if not isinstance(ret, bytes):
            ret = bytes(ret)
        return ret

    def seek(self, pos):
        self.pos = pos


# This is imported here to avoid an import loop
from backathon import models  # noqa: E402


class Settings:
    """A loose proxy for the Settings database model that does json
    encoding/decoding

    """

    def __init__(self, alias):
        self.alias = alias

    def __getitem__(self, item):
        value = models.Setting.get(item, using=self.alias)
        return json.loads(value)

    def get(self, item, default=None):
        value = models.Setting.get(item, using=self.alias, default=default)
        return json.loads(value)

    def __setitem__(self, key, value):
        value = json.dumps(value)
        models.Setting.set(key, value, using=self.alias)

    def __contains__(self, item):
        return models.Setting.objects.using(self.alias).filter(key=item).exists()


class SimpleSetting:
    """A descriptor class that is used to define a getter+setter on the
    Repository class that reads/writes a simple (immutable) value from the
    database

    """

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def __get__(self, instance, owner):
        if instance is None:
            return self

        try:
            return instance.__dict__[self.name]
        except KeyError:
            try:
                value = instance.settings[self.name]
            except KeyError:
                value = self.default

            instance.__dict__[self.name] = value
            return value

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value
        instance.settings[self.name] = value
