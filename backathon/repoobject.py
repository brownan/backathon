"""Utility functions for working with repository objects"""

import io
import pathlib
import shutil
import tempfile
import zlib
from typing import IO

from backathon.backup import ObjectRequest
from backathon.models import ObjIDType

# Same value as shutil.COPY_BUFSIZE but that attribute isn't public
COPY_BUFSIZE = 1024 * 1024


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
            shutil.copyfileobj(obj_req.body, raw_payload)
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


def decompress_payload(compressed: IO[bytes]) -> IO[bytes]:
    """Decompress the given bytes

    If the given byte buffer does not start with the zlib magic byte,
    the original buffer is returned

    """
    # We can guarantee positive identification of compression with the first byte because
    # the only messages we compress are msgpack "map" types, which always begin with
    # 0x80 - 0x8f, 0xde, or 0xdf.
    # Note: only the msgpack serialization of the positive 7-bit integer 120 serializes
    # to the byte 0x78

    if isinstance(compressed, io.BytesIO):
        # Optimized path if we get an in-memory buffer
        buf = compressed.getbuffer()
        if buf[0] == 0x78:
            # zlib identification marker
            return io.BytesIO(zlib.decompress(buf))
        else:
            return compressed
    else:
        initial_byte = compressed.read(1)
        compressed.seek(-1, io.SEEK_CUR)
        if not initial_byte:
            return io.BytesIO()
        if initial_byte[0] == 0x78:
            decomp_buf = tempfile.SpooledTemporaryFile(max_size=10 * 2**20)
            decompressor = zlib.decompressobj()
            while chunk := compressed.read(COPY_BUFSIZE):
                decomp_buf.write(decompressor.decompress(chunk))
            decomp_buf.write(decompressor.flush())
            decomp_buf.seek(0)
            return decomp_buf
        else:
            return compressed


def make_object_path(objid: ObjIDType) -> pathlib.PurePosixPath:
    """Defines the path in the storage backend used to store an object
    with the given object id

    """
    objid_hex = objid.hex()
    return pathlib.PurePosixPath("objects", objid_hex[:3], objid_hex)
