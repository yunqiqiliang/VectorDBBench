from __future__ import absolute_import

import copy
import datetime
import functools
import operator
import typing
import warnings
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

try:
    import pandas  # type: ignore
except ImportError:  # pragma: NO COVER
    pandas = None

import pyarrow  # type: ignore

from clickzetta.connector.v0.query_result import QueryResult


class Table(object):
    def __init__(self, workspace: str, table_name: str, instance: str, vcluster: str):
        self.workspace = workspace
        self.table_name = table_name
        self.instance = instance
        self.vcluster = vcluster


class RowIterator(object):
    def __init__(self, query_result: QueryResult):
        self.query_result = query_result

    @property
    def schema(self):
        return list(self._schema)

    @property
    def total_rows(self):
        return self.query_result.total_row_count

    def to_arrow_iterable(self) -> Iterator["pyarrow.RecordBatch"]:
        return self._to_page_iterable(self.query_result)

    # If changing the signature of this method, make sure to apply the same
    # changes to job.QueryJob.to_arrow()
    def to_arrow(self) -> "pyarrow.Table":
        try:
            record_batches = []
            for record_batch in self.to_arrow_iterable():
                record_batches.append(record_batch)
        finally:
            print("convert to pyarrow table successfully.")

        return pyarrow.Table.from_batches(record_batches)

    @staticmethod
    def __can_cast_timestamp_ns(column):
        try:
            column.cast("timestamp[ns]")
        except pyarrow.lib.ArrowInvalid:
            return False
        else:
            return True


class EmptyRowIterator(RowIterator):
    def __init__(self, query_result: QueryResult):
        self.query_result = query_result

    def to_arrow(self) -> "pyarrow.Table":
        return pyarrow.Table.from_arrays(())

    def to_arrow_iterable(
        self,
        max_queue_size: Optional[int] = None,
    ) -> Iterator["pyarrow.RecordBatch"]:
        return iter((pyarrow.record_batch([]),))

    def __iter__(self):
        return iter(())
