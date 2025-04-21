"""Some common pydantic types used throughout"""
from __future__ import annotations

import base64
import os
import pathlib
from typing import Annotated

from pydantic import PlainSerializer
from pydantic import PlainValidator

from backathon.models import FSEntry
from backathon.models import make_path_printable

# This pydantic type serializes a pathlib.Path into an opaque object that
# will preserve un-decodable bytes in the path without hitting decode errors
# in serialization
PathType = Annotated[
    pathlib.Path,
    PlainSerializer(
        lambda x: base64.urlsafe_b64encode(FSEntry.encode_path(x)).decode("ascii"),
        return_type=str,
    ),
    PlainValidator(
        lambda x: pathlib.Path(os.fsdecode(base64.urlsafe_b64decode(x.encode("ascii"))))
    ),
]

# Strips unprintable characters for user display
PrintablePath = Annotated[
    pathlib.Path, PlainSerializer(lambda x: make_path_printable(os.fsencode(x)))
]
