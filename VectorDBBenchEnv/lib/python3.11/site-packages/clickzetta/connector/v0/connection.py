"""Connection for ClickZetta DB-API."""

import time
from typing import Optional
import weakref
from typing import Sequence, Any, Union

import requests
from requests.adapters import HTTPAdapter

from clickzetta.connector.v0 import _dbapi_helpers, cursor, import_ingestion_api
from clickzetta.connector.v0._bulkload import import_bulkload_api
from clickzetta.connector.v0.client import Client

from logging import getLogger

from clickzetta.connector.v0.converter import Converter
from clickzetta.connector.v0.enums import RealtimeOperation

_log = getLogger(__name__)
is_token_init = False
https_session = None
https_session_inited = False


@_dbapi_helpers.raise_on_closed("Operating on a closed connection.")
class Connection(object):
    def __init__(self, client: Optional[Client] = None):
        if client is None:
            _log.error("Connection must has a LogParams to log in.")
            raise AssertionError("Connection must has a LogParams to log in.")
        else:
            self._owns_client = True

        self._client = client
        self._client.refresh_token()
        self.converter = Converter()

        if not globals()["https_session_inited"]:
            session = requests.Session()
            session.mount(
                self._client.service,
                HTTPAdapter(pool_connections=10, pool_maxsize=client.http_pool_size, max_retries=3),
            )
            globals()["https_session"] = session
            globals()["https_session_inited"] = True

        self._client.session = globals()["https_session"]
        self._closed = False
        self._cursors_created = weakref.WeakSet()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _set_pure_arrow_decoding(self, value: bool):
        self._client._pure_arrow_decoding = value

    @property
    def pure_arrow_decoding(self):
        return self._client._pure_arrow_decoding

    def close(self):
        self._closed = True

        if self._owns_client:
            self._client.close()

        for cursor_ in self._cursors_created:
            if not cursor_._closed:
                cursor_.close()

    def commit(self):
        """No-op, but for consistency raise an error if connection is closed."""

    def cursor(self):
        """Return a new cursor object."""
        if self._client.username is not None and self._client.password is not None:
            self._client.refresh_token()
        new_cursor = cursor.Cursor(self)
        self._cursors_created.add(new_cursor)
        return new_cursor

    def create_bulkload_stream(self, **kwargs):
        return import_bulkload_api().create_bulkload_stream(self._client, **kwargs)

    def get_bulkload_stream(
        self, stream_id: str, schema: str = None, table: str = None
    ):
        return import_bulkload_api().get_bulkload_stream(
            self._client, stream_id=stream_id, schema=schema, table=table
        )

    def get_realtime_stream(
        self, operate: RealtimeOperation = RealtimeOperation.CDC, schema: str = None, table: str = None, options = None, tablet: int = 1
    ):
        return import_ingestion_api().create_realtime_stream(
            self._client, operate=operate, schema=schema, table=table, options=options, tablet=tablet
        )

    def get_job_profile(self, job_id: str):
        return self._client.get_job_profile(job_id)

    def get_job_result(self, job_id: str):
        return self._client.get_job_result(job_id)

    def get_job_progress(self, job_id: str):
        return self._client.get_job_progress(job_id)

    def get_job_summary(self, job_id: str):
        return self._client.get_job_summary(job_id)

    def get_job_plan(self, job_id: str):
        return self._client.get_job_plan(job_id)

    def _process_params_qmarks(self, params: Union[Sequence[Any], dict, None]) -> tuple:
        """Process parameters for client-side parameter binding.

        Args:
            params: Either a sequence, or a dictionary of parameters, if anything else
                is given then it will be put into a list and processed that way.
        """
        if params is None:
            return ()
        if isinstance(params, dict):
            ret = tuple(self._process_single_param(v) for k, v in params.items())
            _log.debug("binding parameters: %s", ret)
            return ret

        res = map(self._process_single_param, params)
        ret = tuple(res)
        _log.debug("parameters: %s", ret)
        return ret

    def get_full_sql_with_params(self, command: str, params: tuple) -> str:
        if not params:
            return command
        parts = command.split('?')
        _log.info(f"get_full_sql_with_params: {command}\n, parts size:{len(parts)}, params size:{len(params)}, "
                    f"params:{params}")
        full_sql = [parts[0]]
        for i, param in enumerate(params):
            full_sql.append(str(param))
            if i + 1 < len(parts):
                full_sql.append(parts[i + 1])
        return ''.join(full_sql)

    def _process_single_param(self, param: Any) -> Any:
        """Process a single parameter to Clickzetta understandable form.

        This is a convenience function to replace repeated multiple calls with a single
        function call.

        It calls the following underlying functions in this order:
            1. self.converter.convert_to
            2. self.converter.quote
        """
        _quote = self.converter.quote
        rst = _quote(self.converter.convert_to(param))
        return rst

    def get_schema(self):
        return self._client.schema

    def use_schema(self, schema: str):
        """Set the current schema for the connection."""
        self._client.schema(schema)

    def use_workspace(self, workspace: str):
        """Set the current workspace for the connection."""
        self._client.workspace(workspace)

    def use_vcluster(self, vcluster: str):
        """Set the current vcluster for the connection."""
        self._client.vcluster(vcluster)

    def use_http(self):
        """Set the protocol to http."""
        self._client.protocol = "http"


def connect(**kwargs) -> Connection:
    client = kwargs.get("client")
    if client is None:
        # setting client or cz_url will ignore following parameters
        client = Client(**kwargs)
    return Connection(client)
