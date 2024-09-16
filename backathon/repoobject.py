"""Utility functions for working with repository objects"""

import hmac
import io
import os
import pathlib
import shutil
import zlib
from dataclasses import dataclass
from typing import IO, Self, Sequence

import lz4.frame
from typing_extensions import Buffer

from backathon.backup import ObjectRequest
from backathon.encryption.base import EncrypterBase
from backathon.exceptions import CorruptedRepository
from backathon.models import Object, ObjectHeader, ObjIDType
from backathon.proftools import perf_block
from backathon.storage.base import StorageBase

# Same value as shutil.COPY_BUFSIZE but that attribute isn't public
COPY_BUFSIZE = 1024 * 1024


@dataclass
class RawPayload:
    """A "raw" payload is a byte string representing a serialized Object before
    it has been compressed or encrypted

    This class is bound to a particular encrypter which defines the object's ID.

    """

    header: ObjectHeader
    encrypter: EncrypterBase
    raw_payload_buf: Buffer
    body_start: int
    objid: ObjIDType

    @classmethod
    def from_obj_req(cls, obj_req: ObjectRequest, encrypter: EncrypterBase) -> Self:
        """Makes a new RawPayload from an object request and an encrypter

        This is used when preparing newly constructed objects for upload
        """
        header = obj_req.header

        body_start, raw_payload_buf = cls.make_obj_payload(obj_req)

        with perf_block("make_objid"):
            objid = encrypter.make_objid(raw_payload_buf)
        return cls(
            header=header,
            encrypter=encrypter,
            raw_payload_buf=raw_payload_buf,
            body_start=body_start,
            objid=objid,
        )

    @classmethod
    def from_encrypted_payload(
        cls, objid: ObjIDType, enc_payload: IO[bytes], encrypter: EncrypterBase
    ) -> Self:
        """Reconstructs the raw payload from an encrypted payload, decrypting it using
        the given encrypter

        This is used when downloading an object from the remote repository.

        This method checks the integrity of the payload, both via the encrypter's own
        message authentication, and by verifying the objid. Will raise a CorruptedRepository
        if the object's integrity checks do not pass.
        """
        decrypted = encrypter.decrypt(enc_payload)
        enc_payload.close()
        decompressed = decompress_payload(io.BytesIO(decrypted))

        # Check obj id
        computed_objid = encrypter.make_objid(decompressed)
        if not hmac.compare_digest(computed_objid, objid):
            raise CorruptedRepository(f"Corrupted Object: {objid.hex()}")

        decompressed_stream = io.BytesIO(decompressed)
        header = ObjectHeader.from_stream(decompressed_stream)

        # Verify the body length matches the header's reported length
        body_start = decompressed_stream.tell()
        decompressed_stream.seek(0, os.SEEK_END)
        body_end = decompressed_stream.tell()
        if body_end - body_start != header.length:
            raise CorruptedRepository(f"Object length mismatch: {objid.hex()}")

        return cls(
            objid=objid,
            header=header,
            raw_payload_buf=decompressed,
            body_start=body_start,
            encrypter=encrypter,
        )

    @property
    def body(self) -> Buffer:
        """Returns just the body of this object as a buffer"""
        return memoryview(self.raw_payload_buf)[self.body_start :]

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

        Calling code will typically need to save the returned Object instance to the database

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
    def make_obj_payload(obj_req: ObjectRequest) -> tuple[int, Buffer]:
        """Construct a serialized object buffer from an ObjectRequest

        The resulting buffer is the uncompressed, unencrypted byte string for the object

        returns (body start position, raw payload buffer)
        """

        if (
            isinstance(obj_req.body, io.BytesIO)
            and len(obj_req.body.getbuffer()) != obj_req.header.length
        ):
            raise RuntimeError("Size mismatch between header and body")

        raw_payload = io.BytesIO()
        raw_payload.write(obj_req.header.model_dump_msgpack())
        body_start = raw_payload.tell()
        if obj_req.body is not None:
            if isinstance(obj_req.body, io.BytesIO):
                raw_payload.write(obj_req.body.getbuffer())
            else:
                shutil.copyfileobj(obj_req.body, raw_payload)
        raw_payload.seek(0)
        return body_start, raw_payload.getbuffer()


def compress_payload(buf: Buffer) -> Buffer:
    """Compress the given bytes

    If the compressed size is not actually smaller, then the original
    buffer is returned

    """
    length = len(memoryview(buf))
    if length < 4096:
        return buf
    with perf_block("lz4"):
        compressed_bytes = lz4.frame.compress(buf)
    if len(compressed_bytes) < length:
        return compressed_bytes
    else:
        return buf


def decompress_payload(compressed: IO[bytes]) -> Buffer:
    """Decompress the given bytes

    If the given byte buffer does not start with the zlib magic byte,
    the original buffer is returned

    """
    # We can guarantee positive identification of compression because the compression
    # signature bytes don't overlap with our msgpack messages.
    # The only messages we compress are msgpack "map" types, which always begin with
    # 0x80 - 0x8f, 0xde, or 0xdf.

    if isinstance(compressed, io.BytesIO):
        # Optimized path if we get an in-memory buffer
        buf = compressed.getbuffer()
        if buf[:4] == b"\x04\x22\x4d\x18":
            # lz4 frame format
            return lz4.frame.decompress(buf)
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
