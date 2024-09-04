"""Utility functions for working with repository objects"""

import io
import pathlib
import shutil
import zlib
from typing import IO, Self, Sequence

from typing_extensions import Buffer

from backathon.backup import ObjectRequest
from backathon.encryption.base import EncrypterBase
from backathon.models import Object, ObjectHeader, ObjIDType
from backathon.proftools import perf_block
from backathon.storage.base import StorageBase

# Same value as shutil.COPY_BUFSIZE but that attribute isn't public
COPY_BUFSIZE = 1024 * 1024


class RawPayload:
    """A "raw" payload is a byte string representing a serialized Object before
    it has been compressed or encrypted

    This class is bound to a particular encrypter which defines the object's ID.

    """

    header: ObjectHeader
    encrypter: EncrypterBase
    raw_payload_buf: Buffer
    objid: ObjIDType

    @classmethod
    def from_obj_req(cls, obj_req: ObjectRequest, encrypter: EncrypterBase) -> Self:
        self = cls()
        self.header = obj_req.header
        self.encrypter = encrypter

        self.raw_payload_buf = self.make_obj_payload(obj_req)

        with perf_block("make_objid"):
            self.objid = encrypter.make_objid(self.raw_payload_buf)
        return self

    def get_children(self) -> Sequence[tuple[ObjIDType, ObjIDType, bytes | None]]:
        """Returns the child relations for this object, used to add rows to the
        object_relations table

        """
        children = []
        if self.header.blobs:
            children.extend((self.objid, b.objid, None) for b in self.header.blobs)
        if self.header.entries:
            children.extend((self.objid, e.objid, e.name) for e in self.header.entries)
        return children

    def upload(self, storage: StorageBase) -> Object:
        """Perform final compression and encryption, upload the payload, and return a new
        Object instance representing what was uploaded

        """
        buf = compress_payload(self.raw_payload_buf)
        final_payload = self.encrypter.encrypt(buf)
        path = make_object_path(self.objid)
        storage.put_object(path, final_payload)

        return Object(
            objid=self.objid,
            type=self.header.type,
            uploaded_size=final_payload.size,
            file_size=self.header.file_size,
            last_modified_time=self.header.last_modified_time,
            sha1=final_payload.sha1,
        )

    @staticmethod
    def make_obj_payload(obj_req: ObjectRequest) -> Buffer:
        """Construct a serialized object buffer from an ObjectRequest

        The result is the uncompressed, unencrypted byte string for the object
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
        return raw_payload.getbuffer()


def compress_payload(buf: Buffer) -> Buffer:
    """Compress the given bytes

    If the compressed size is not actually smaller, then the original
    buffer is returned

    """
    length = len(memoryview(buf))
    if length < 4096:
        return buf
    with perf_block("zlib"):
        compressed_bytes = zlib.compress(buf)
    if len(compressed_bytes) < length:
        return compressed_bytes
    else:
        return buf


def decompress_payload(compressed: IO[bytes]) -> Buffer:
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
            return zlib.decompress(buf)
        else:
            return compressed.getbuffer()
    else:
        try:
            initial_byte = compressed.read(1)
            compressed.seek(-1, io.SEEK_CUR)
            if not initial_byte:
                return b""
            if initial_byte[0] == 0x78:
                decomp_buf = io.BytesIO()
                decompressor = zlib.decompressobj()
                while chunk := compressed.read(COPY_BUFSIZE):
                    decomp_buf.write(decompressor.decompress(chunk))
                decomp_buf.write(decompressor.flush())
                decomp_buf.seek(0)
                return decomp_buf.getbuffer()
            else:
                return compressed.read()
        finally:
            compressed.close()


def make_object_path(objid: ObjIDType) -> pathlib.PurePosixPath:
    """Defines the path in the storage backend used to store an object
    with the given object id

    """
    objid_hex = objid.hex()
    return pathlib.PurePosixPath("objects", objid_hex[:3], objid_hex)
