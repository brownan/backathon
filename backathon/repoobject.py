"""Utility functions for working with repository objects"""

import io
import pathlib
import zlib

from backathon.backup import ObjectRequest
from backathon.models import ObjIDType


def make_obj_payload(obj_req: ObjectRequest) -> io.BytesIO:
    """Given an ObjectRequest, construct a bytes buffer with the object
    contents.

    These bytes will be optionally compressed and/or encrypted before actually
    being uploaded.
    """

    if (
        isinstance(obj_req.body, io.BytesIO)
        and len(obj_req.body.getbuffer()) != obj_req.header.length
    ):
        raise RuntimeError("Size mismatch between header and body")

    raw_payload = io.BytesIO()
    raw_payload.write(obj_req.header.model_dump_msgpack())
    if obj_req.body is not None:
        if isinstance(obj_req.body, io.BytesIO):
            raw_payload.write(obj_req.body.getbuffer())
        else:
            raw_payload.write(obj_req.body.read())
    raw_payload.seek(0)
    return raw_payload


def compress_payload(raw: io.BytesIO) -> io.BytesIO:
    """Compress the given bytes

    If the compressed size is not actually smaller, then the original
    buffer is returned

    """
    buf = raw.getbuffer()
    compressed_bytes = zlib.compress(buf)
    if len(compressed_bytes) < len(buf):
        return io.BytesIO(compressed_bytes)
    else:
        return raw


def decompress_payload(compressed: io.BytesIO) -> io.BytesIO:
    """Decompress the given bytes

    If the given byte buffer does not start with the zlib magic byte,
    the original buffer is returned

    """
    buf = compressed.getbuffer()
    if buf[0] == 0x78:
        # zlib identification marker
        return io.BytesIO(zlib.decompress(buf))
    else:
        return compressed


def make_object_path(objid: ObjIDType) -> pathlib.PurePosixPath:
    """Defines the path in the storage backend used to store an object
    with the given object id

    """
    objid_hex = objid.hex()
    return pathlib.PurePosixPath("objects", objid_hex[:3], objid_hex)
