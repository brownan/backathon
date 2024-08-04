from __future__ import annotations

import hashlib
import io
from typing import IO

from backathon.encryption.base import EncrypterBase, Payload


class NullEncrypter(EncrypterBase):
    def encrypt(self, buf: IO[bytes]) -> Payload:
        hasher = hashlib.sha1()
        size = 0
        while chunk := buf.read(io.DEFAULT_BUFFER_SIZE):
            size += len(chunk)
            hasher.update(chunk)
        buf.seek(0)
        return Payload(buf=buf, size=size, sha1=hasher.digest())

    def decrypt(self, buf: IO[bytes]) -> IO[bytes]:
        return buf
