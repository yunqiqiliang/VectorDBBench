#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

import re
from enum import Enum
from typing import List, Tuple

from clickzetta.zettapark.exceptions import ZettaparkInvalidVolumePathException

_PATH_PATTERN = (
    "^vol(?:ume)?"
    # group 1: scheme suffix
    + "(?::(table|user))?"
    + "://"
    # group 2: authority
    + "("
    # branch 2.1: volume identifier
    + "(?:(?:[a-z0-9_]+|`[a-z0-9_]+`)(?:[.](?:[a-z0-9_]+|`[a-z0-9_]+`)){0,2})"
    + "|"
    # branch 2.2: user volume placeholder
    + "[~]"
    + ")"
    # group 3: path
    + "(/.*)"
)
_PATH_REGEX = re.compile(_PATH_PATTERN, re.IGNORECASE)


class VolumeKind(Enum):
    EXTERNAL = "external"
    TABLE = "table"
    USER = "user"


class VolumePath:
    def __init__(self, volume_path: str) -> None:
        self._volume_path: str = volume_path
        self._kind, self._volume, self._path = _parse(volume_path)

    @property
    def kind(self) -> VolumeKind:
        return self._kind

    @property
    def volume_name(self) -> List[str]:
        return [x for x in self._volume]

    @property
    def path(self) -> str:
        return self._path


def _parse(path: str) -> Tuple[VolumeKind, List[str], str]:
    match = _PATH_REGEX.match(path)
    if not match:
        raise ZettaparkInvalidVolumePathException(
            f"Invalid volume path: '{path}'",
        )

    kind = VolumeKind.EXTERNAL
    if suffix := match.group(1):
        suffix = suffix.lower()
        if suffix == "user":
            kind = VolumeKind.USER
        elif suffix == "table":
            kind = VolumeKind.TABLE

    authority = match.group(2)
    if authority == "~":
        name = []
    else:
        name = [x.strip("`") for x in authority.split(".")]

    if kind == VolumeKind.USER and name != []:
        raise ZettaparkInvalidVolumePathException(
            f"Invalid volume path: '{path}'",
        )

    path_ = match.group(3).replace("//", "/")

    return kind, name, path_
