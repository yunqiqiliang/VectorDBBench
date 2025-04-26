#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

import os
from datetime import datetime
from pathlib import Path
from typing import List, NamedTuple, Union

import clickzetta.zettapark
from clickzetta.zettapark._internal.analyzer.query_plan import FileOperationPlanBuilder
from clickzetta.zettapark._internal.volume_path import VolumePath


class PutResult(NamedTuple):
    """Represents the results of uploading a local file to a volume location."""

    source: str  #: The source file path.
    target: str  #: The file path in the volume where the source file is uploaded.
    source_size: int  #: The size in bytes of the source file.
    target_size: int  #: The size in bytes of the target file.


class GetResult(NamedTuple):
    """Represents the results of downloading a file from a volume location to the local file system."""

    file: str  #: The downloaded file path.
    size: int  #: The size in bytes of the downloaded file.


class ListResult(NamedTuple):
    file: str
    size: int
    mtime: datetime


class FileOperation:
    """Provides methods for working on files in a volume.
    To access an object of this class, use :attr:`Session.file`.
    """

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self._session = session

    def _file(self) -> "FileOperationPlanBuilder":
        return self._session._analyzer.plan_builder.file()

    def _normalize_path(self, path: Union[str, Path]) -> Path:
        if isinstance(path, str):
            if path.startswith("file://") or path.lower().startswith("file://"):
                path = path[7:]
            return Path(path)
        return path

    def put(
        self,
        local_path: Union[str, Path],
        volume_path: str,
        **options,
    ) -> List[PutResult]:
        """Uploads local file to the volume."""
        options = {str(k): str(v) for k, v in options.items()}
        local_path = self._normalize_path(local_path)
        plan = self._file().put(local_path, VolumePath(volume_path), options)
        result_data, _ = self._session._conn.get_result_and_metadata(plan)
        return [
            PutResult(row[0], row[1], int(row[2]), int(row[2])) for row in result_data
        ]

    def get(
        self,
        volume_path: str,
        target_directory: Union[str, Path],
        **options,
    ) -> List[GetResult]:
        """Downloads the specified files from a volume path to a local directory."""
        options = {str(k): str(v) for k, v in options.items()}
        local_path = self._normalize_path(target_directory)
        plan = self._file().get(VolumePath(volume_path), local_path, options)
        os.makedirs(local_path, exist_ok=True)
        result_data, _ = self._session._conn.get_result_and_metadata(plan)
        return [GetResult(row[0], int(row[2])) for row in result_data]

    def list_(
        self,
        volume_path: str,
        **options,
    ) -> List[ListResult]:
        """Lists the files in a path in a volume."""
        options = {str(k): str(v) for k, v in options.items()}
        plan = self._file().list(VolumePath(volume_path), options)
        result_data, _ = self._session._conn.get_result_and_metadata(plan)
        return [
            ListResult(file=row[0], size=int(row[2]), mtime=row[1])
            for row in result_data
        ]

    def copy_files(
        self,
        src_volume_path: str,
        dest_volume_path: str,
        **options,
    ) -> List[str]:
        """Copy files from one volume to another."""
        options = {str(k): str(v) for k, v in options.items()}
        plan = self._file().copy_files(
            VolumePath(dest_volume_path), VolumePath(src_volume_path), options
        )
        result_data, _ = self._session._conn.get_result_and_metadata(plan)
        return [row[0] for row in result_data]

    def delete(self, volume_path: str) -> List[str]:
        """Delete files from a volume."""
        plan = self._file().delete(VolumePath(volume_path))
        result_data, _ = self._session._conn.get_result_and_metadata(plan)
        return [row[0] for row in result_data]
