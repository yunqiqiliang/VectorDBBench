"""Cursor for ClickZetta DB-API."""

import re
import time
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple, Union

from clickzetta.connector.v0._dbapi import ColumnDescription
from clickzetta.connector.v0.enums import JobID
from clickzetta.connector.v0.exceptions import ProgrammingError
from clickzetta.connector.v0.query_result import QueryResult
from clickzetta.connector.v0.utils import execute_with_retrying

if TYPE_CHECKING:
    import pandas

# Add an alias for compatibility
Column = ColumnDescription

_log = getLogger(__name__)

SQL_COMMENT = r"\/\*.*?\*\/"

RE_SQL_INSERT_STMT = re.compile(
    rf"({SQL_COMMENT}|\s)*INSERT({SQL_COMMENT}|\s)"
    r"*(?:IGNORE\s+)?INTO\s+[`'\"]?.+[`'\"]?(?:\.[`'\"]?.+[`'\"]?)"
    r"{0,2}\s+VALUES\s*(\(.+\)).*",
    re.I | re.M | re.S,
)

RE_SQL_ON_DUPLICATE = re.compile(
    r"""\s*ON\s+DUPLICATE\s+KEY(?:[^"'`]*["'`][^"'`]*["'`])*[^"'`]*$""",
    re.I | re.M | re.S,
)

RE_SQL_COMMENT = re.compile(
    rf"""({SQL_COMMENT})|(["'`][^"'`]*?({SQL_COMMENT})[^"'`]*?["'`])""",
    re.I | re.M | re.S,
)

RE_SQL_INSERT_VALUES = re.compile(r".*VALUES\s*(\(.+\)).*", re.I | re.M | re.S)

RE_PY_MAPPING_PARAM = re.compile(
    rb"""
    %
    \((?P<mapping_key>[^)]+)\)
    (?P<conversion_type>[diouxXeEfFgGcrs%])
    """,
    re.X,
)
COMMENT_SQL_RE = re.compile(r"^(\s*/\*[^*]*\*+(?:[^/*][^*]*\*+)*/)?((\s)*--.*?\n)*",
                            re.IGNORECASE, )
# This regex captures the first capture group after VALUES for batch insert on the client side. e.g.: (?, ?)
SQL_VALUES_RE = re.compile(
    r".*VALUES\s*(\(.*\)).*", re.IGNORECASE | re.MULTILINE | re.DOTALL
)


class _ParamSubstitutor:
    """
    Substitutes parameters into SQL statement.
    """

    def __init__(self, params: Sequence[bytes]) -> None:
        self.params: Sequence[bytes] = params
        self.index: int = 0

    def __call__(self, matchobj: re.Match) -> bytes:
        index = self.index
        self.index += 1
        try:
            return bytes(self.params[index])
        except IndexError:
            raise Exception("Not enough parameters for the SQL statement") from None

    @property
    def remaining(self) -> int:
        """Returns number of parameters remaining to be substituted"""
        return len(self.params) - self.index


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.arraysize = 100
        self._query_result = None
        self._closed = False
        self.job_id = None
        self._rows = None
        self.row_number = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def _query_data(self):
        return self._query_result.data

    def close(self):
        self._closed = True

    @property
    def rowcount(self) -> int:
        return self._query_result.total_row_count if self._query_result else -1

    @property
    def description(self) -> Optional[Tuple[ColumnDescription]]:
        if self._query_result is None or self._query_result.schema is None:
            return None
        return tuple(field.to_column_description() for field in self._query_result.schema)

    @property
    def schema(self):
        return self._query_result.schema if self._query_result else None

    def execute(self, operation: str, parameters=None, binding_params=None):
        self._execute(operation, parameters, binding_params=binding_params)

    def execute_async(self, operation: str, parameters=None, binding_params=None):
        self._execute(operation, parameters, asynchronous=True, binding_params=binding_params)
        return self.job_id

    def is_job_finished(self, job_id: str = None, retry_times=1):
        if self._query_data:
            return True
        _client = self.connection._client
        is_finished = False

        def _handle_get_job_result(job_id, client, tried, **kwargs):
            return client.get_job_result(job_id)

        def _result_check(context, tried, exception=None, **kwargs):
            nonlocal is_finished
            if _client.verify_result_dict_finished(context.result):
                is_finished = True
            if tried > 1:
                time.sleep(0.5)
            return is_finished

        def _on_failure(result, exception, *args, **kwargs):
            if exception:
                _log.error(f"[is_job_finished] Error msg: {exception}. result: {result}")
            nonlocal is_finished
            is_finished = False

        result = execute_with_retrying(
            partial(_handle_get_job_result, job_id or self.job_id, _client),
            _result_check,
            _on_failure,
            max_tries=retry_times
        )
        if is_finished:
            self._query_result = QueryResult(result, self.connection.pure_arrow_decoding, timezone_hint=_client.timezone_hint)
        return is_finished

    def _get_result_set(self, job_id=None):
        return self.connection.get_job_result(job_id or self.job_id)

    def execute_with_job_id(self, operation: str, job_id=None, parameters=None, asynchronous: bool = False,
                            binding_params=None) -> None:
        self._execute(operation, parameters, job_id, asynchronous, binding_params)

    def query_id(self) -> str:
        return self.connection._client._format_job_id()

    def cancel(self, job_id: str) -> None:
        client = self.connection._client
        client.cancel_job(JobID(job_id, client.workspace, 100), {})

    def _execute(
            self, operation: str, parameters, job_id=None, asynchronous: bool = False, binding_params=None
    ):
        if operation is None:
            raise ValueError("sql is empty")
        else:
            operation = operation.strip()
            if operation == "":
                raise ValueError("sql is empty")
        self._query_job = None
        client = self.connection._client
        operation = operation + ";" if not operation.endswith(";") else operation

        self.job_id = (
            client._format_job_id() if job_id is None or job_id == "" else job_id
        )
        self.query = operation

        # binding parameters. This is for qmarks paramstyle.
        binding_params = self.connection._process_params_qmarks(binding_params)
        operation = self.connection.get_full_sql_with_params(operation, binding_params)

        job_id = JobID(self.job_id, client.workspace, 100)

        self._query_result: QueryResult = client.submit_sql_job(token=client.token, sql=operation, job_id=job_id,
                                                                parameters=parameters, asynchronous=asynchronous)
        if len(self._query_result.total_msg) == 0:
            return

    def executemany(self, operation: str, seqparams: Union[Sequence[Any], dict, None] = None, parameters=None):
        """
        Args:
            operation: The SQL operation to be executed.
            seqparams: Parameters to be bound into the SQL statement.
            parameters: Parameters to be set the SQL statement.

        Returns:
            The cursor itself, or None if some error happened.
        """
        if not seqparams:
            self.execute(operation, parameters)
            return

        operation_wo_comments = re.sub(COMMENT_SQL_RE, "", operation)
        m = SQL_VALUES_RE.match(operation_wo_comments)
        if not m:
            raise Exception("Failed to rewrite multi-row insert")
        fmt = m.group(1)

        batch_size = 1024

        for i in range(0, len(seqparams), batch_size):
            batch = seqparams[i:i + batch_size]
            batch_values = []
            for param in batch:
                _log.debug("parameter: %s", param)
                p: tuple = self.connection._process_params_qmarks(param)
                count = -1
                def replacer(_):
                    nonlocal count
                    count += 1
                    return p[count]
                formatted_values = re.sub(r'\?', replacer, fmt)
                batch_values.append(formatted_values)
            full_sql = operation.replace(fmt, ",".join(batch_values), 1)
            self.execute(full_sql, parameters)

    def __iter__(self):
        if self._query_result is None:
            raise ProgrammingError("No result")
        return self._query_result.reader

    def fetchone(self):
        try:
            return next(self._query_result.reader)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        size = size or self.arraysize
        result = []
        try:
            for _ in range(size):
                result.append(next(self._query_result.reader))
        except StopIteration:
            ...
        return result

    def fetchall(self):
        return list(self._query_result.reader)

    def fetch_pandas(self) -> Optional["pandas.DataFrame"]:
        return self._query_data.fetch_pandas()

    def get_job_id(self):
        return self.job_id

    def setinputsizes(self, sizes):
        """No-op, but for consistency raise an error if cursor is closed."""

    def setoutputsize(self, size, column=None):
        """No-op, but for consistency raise an error if cursor is closed."""
