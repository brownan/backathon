import os

from django.db import models


class PathField(models.CharField):
    """Stores path strings as their binary version

    On Linux, filenames are binary strings, but are typically displayed using a
    system encoding. Some filenames may contain un-decodable byte sequences,
    however, and Python will automatically embed un-decodable bytes as
    unicode surrogates, as specified in PEP 383.

    This field stores file paths as binary sequences, and uses the
    os.fsencode() and os.fsdecode() functions to translate to and from
    strings when loading/saving from the database.

    This avoids encoding problems, as passing a string with surrogates to
    SQLite will raise an exception when trying to encode.

    Note that many of the common query lookups don't work on BLOB fields the
    same as TEXT fields. For example, using the __startswith lookup will
    never match because SQLite doesn't implement the LIKE operator for BLOB
    types.
    """
    def __init__(self, **kwargs):
        kwargs.setdefault("max_length", 4096)
        super().__init__(**kwargs)

    def get_internal_type(self):
        return "BinaryField"

    def get_prep_value(self, value):
        if isinstance(value, str):
            return os.fsencode(value)
        return value

    def from_db_value(self, value, expression, connection):
        if isinstance(value, bytes):
            return os.fsdecode(value)
        return value