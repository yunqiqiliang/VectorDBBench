"""Client for interacting with the ClickZetta API."""

from __future__ import absolute_import, division

import os
import random
import re
import shutil
import sys
import threading
import time
import typing
from datetime import datetime, timedelta
from functools import partial
from logging import getLogger
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import unquote, urlparse

import requests
import requests.exceptions
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from clickzetta.connector.v0 import import_igs_client_builder
from clickzetta.connector.v0._bulkload import import_bulkload_api
from clickzetta.connector.v0._volume import resolve_local_path
from clickzetta.connector.v0.enums import *
from clickzetta.connector.v0.exception import ClickzettaJobNotExistsException, ClickzettaClientException
from clickzetta.connector.v0.exceptions import CZException
from clickzetta.connector.v0.parse_url import parse_url, generate_url
from clickzetta.connector.v0.query_result import QueryData, QueryDataType, QueryResult
from clickzetta.connector.v0.table import Table
from clickzetta.connector.v0.utils import (
    BaseExecuteContext,
    execute_with_retrying,
    normalize_file_path,
    split_sql, strip_leading_comment,
)

_log = getLogger(__name__)
TIMEOUT_HEADER = "X-Server-Timeout"
TimeoutType = Union[float, None]
ResumableTimeoutType = Union[None, float, Tuple[float, float]]

if typing.TYPE_CHECKING:
    PathType = Union[str, bytes, os.PathLike[str], os.PathLike[bytes]]

_DEFAULT_CHUNKSIZE = 10 * 1024 * 1024  # 10 MB
_MAX_MULTIPART_SIZE = 5 * 1024 * 1024
_DEFAULT_NUM_RETRIES = 6
_GENERIC_CONTENT_TYPE = "*/*"

_MIN_GET_QUERY_RESULTS_TIMEOUT = 120
HTTP_PROTOCOL_DEFAULT_PORT = "80"
HTTPS_PROTOCOL_DEFAULT_PORT = "443"
HTTP_PROTOCOL_PREFIX = "http://"
HTTPS_PROTOCOL_PREFIX = "https://"
IGS_PREFIX = "igs:"

DEFAULT_TIMEOUT = None
DEFAULT_MAXIMUM_TIMEOUT = 60
HYBRID_SQL_TIMEOUT = 120

HEADERS = {"Content-Type": "application/json"}

_globals = globals()
_globals["token_https_session"] = None
_globals["token_https_session_inited"] = False
DEFAULT_INSTANCE_ID = 100
DEFAULT_EXPIRED_FACTOR = 0.8
DEFAULT_TOKEN_EXPIRE_TIME_MS = 2 * 60 * 60 * 1000
user_token_dict = {}
user_token_time_dict = {}
JOB_ALREADY_EXIST = "CZLH-60007"
JOB_NOT_EXIST = "CZLH-60005"
JOB_STATUS_UNKNOWN = "CZLH-60022"
JOB_NOT_SUBMITTED = "CZLH-60023"
JOB_TIME_OUT_KILLED = "CZLH-60010"
JOB_NEED_RE_EXECUTE = "CZLH-57015"
JOB_VC_QUEUE_EXCEEDED = "CZLH-60015"


def _initialize_https_session(protocol: str = "https", http_pool_size: int = 100) -> requests.Session:
    if not _globals["token_https_session_inited"] or not _globals["token_https_session"]:
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            allowed_methods=frozenset(["GET", "POST"]),
        )
        session.mount(protocol + "://",
                      HTTPAdapter(pool_connections=10, pool_maxsize=http_pool_size, max_retries=retries))
        _globals["token_https_session"] = session
        _globals["token_https_session_inited"] = True
    return _globals["token_https_session"]


class Client(object):
    def __init__(
            self,
            username: str = None,
            password: str = None,
            instance: str = None,
            workspace: str = None,
            vcluster: str = None,
            cz_url: str = None,
            service: str = None,
            schema: str = "public",
            magic_token: str = None,
            protocol: str = None,
            hints: Dict = None,
            **extra,
    ):
        self.max_retries = 50
        self.connect_context = CZConnectContext()
        self.magic_token = None
        self.http_pool_size = 100
        self.protocol: Optional[str] = protocol
        self.hints = hints or {}
        self.lock = None
        self.url = cz_url
        self._token = None
        self.token_expire_time_ms = 0 if self.magic_token is not None else DEFAULT_TOKEN_EXPIRE_TIME_MS
        self._pure_arrow_decoding = True
        self.timezone_hint: Optional[str] = None
        self.extra = extra
        if cz_url is not None:
            [
                self.service,
                self.username,
                self.driver_name,
                self.password,
                self.instance,
                self.workspace,
                self.vcluster,
                self.schema,
                self.magic_token,
                self.protocol,
                self.host,
                self.token_expire_time_ms,
                extra_params,
            ] = parse_url(cz_url)
            if extra_params:
                self.extra.update(extra_params)
            self.instance_id = 0
        else:
            if service is None:
                raise ValueError("ClickZetta connection url `service` is required.")
            else:
                self.host = service
                if self.protocol is None:
                    self.protocol = "https"
                if self.protocol == "https":
                    self.service = (
                        HTTPS_PROTOCOL_PREFIX + service
                        if not service.startswith(HTTPS_PROTOCOL_PREFIX)
                        else service
                    )
                elif self.protocol == "http":
                    self.service = (
                        HTTP_PROTOCOL_PREFIX + service
                        if not service.startswith(HTTP_PROTOCOL_PREFIX)
                        else service
                    )
                else:
                    raise ValueError(
                        "protocol must be http or https. Other protocols are not supported yet."
                    )
            self._token = None
            self.workspace = workspace
            self.instance = instance
            self.vcluster = vcluster
            self.session = None
            self.instance_id = 0
            self.username = username
            self.schema = schema
            self.password = password
            self.magic_token = magic_token
            if "token_expire_time_ms" in self.extra:
                self.token_expire_time_ms = int(self.extra["token_expire_time_ms"])

        self.validate()

        if self.magic_token is None:
            self.lock = threading.Lock()

        _initialize_https_session(self.protocol, self.http_pool_size)
        self._token: str = self.token
        self.config_statements = []
        self.url = generate_url(self)
        if "httppoolsize" in self.extra:
            self.http_pool_size = int(self.extra.pop("httppoolsize"))

        # Add the igs client properties
        self._igs_client = None
        self._igs_client_lock = threading.Lock()
        self.igs_crl_addrs: List[Tuple[str, int]] = []
        self.igs_worker_addrs: List[Tuple[str, int]] = []
        self.worker_channels = []

    @property
    def host(self):
        return self.connect_context.host

    @host.setter
    def host(self, value):
        self.connect_context.host = value

    @property
    def instance(self):
        return self.connect_context.instance

    @instance.setter
    def instance(self, value):
        self.connect_context.instance = value

    @property
    def user(self):
        return self.connect_context.user

    @user.setter
    def user(self, value):
        self.connect_context.user = value

    @property
    def workspace(self):
        return self.connect_context.workspace

    @workspace.setter
    def workspace(self, value):
        self.connect_context.workspace = value

    @property
    def schema(self):
        return self.connect_context.schema

    @schema.setter
    def schema(self, value):
        self.connect_context.schema = value

    @property
    def vcluster(self):
        return self.connect_context.vcluster

    @vcluster.setter
    def vcluster(self, value):
        self.connect_context.vcluster = value

    @property
    def igs_client(self):
        if not self._igs_client:
            with self._igs_client_lock:
                if not self._igs_client:
                    self._igs_client = import_igs_client_builder().with_authenticate(True).with_stream_url(
                        IGS_PREFIX + self.url).with_properties({}).with_outer_client(self).build()
        return self._igs_client

    @igs_client.setter
    def igs_client(self, value):
        self._igs_client = value

    def log_in_cz(self, username: str, password: str, instance: str) -> str:
        with self.lock:
            path = "/clickzetta-portal/user/loginSingle"
            login_params = LoginParams(username, password, instance)
            api_repr = login_params.to_api_repr()
            data = json.dumps(api_repr)
            try:
                session = _initialize_https_session(self.protocol, self.http_pool_size)
                api_response = session.post(self.service + path, data=data, headers=HEADERS,
                                                                    timeout=10)
                if api_response.status_code != 200:
                    raise requests.exceptions.RequestException(
                        "user:"
                        + login_params.username
                        + " login to clickzetta failed, error:"
                        + api_response.text
                    )
                result = api_response.text
                try:
                    result_dict = json.loads(result)
                except Exception:
                    _log.error(f"login error:{result}")
                    raise Exception(f"login error:{result}")
                if result_dict["code"] != 0:
                    raise requests.exceptions.RequestException(
                        "user:"
                        + login_params.username
                        + " login to clickzetta failed, error:"
                        + result_dict["message"]
                    )
                if result_dict["data"]["token"] is None:
                    raise requests.exceptions.RequestException(
                        "user:"
                        + login_params.username
                        + " login to clickzetta  failed, error: token is None"
                    )
                else:
                    token = result_dict["data"]["token"]
                    return token
            except requests.exceptions.RequestException:
                raise

    def create_bulkload_stream(self, schema_name: str, table_name: str, options):
        return import_bulkload_api().create_bulkload_stream_metadata(
            self, schema_name, table_name, options
        )

    def commit_bulkload_stream(
            self,
            instance_id: int,
            workspace: str,
            schema_name: str,
            table_name: str,
            stream_id: str,
            execute_workspace: str,
            execute_vc: str,
            commit_mode,
    ):
        return import_bulkload_api().commit_bulkload_stream(
            self,
            instance_id,
            workspace,
            schema_name,
            table_name,
            stream_id,
            execute_workspace,
            execute_vc,
            commit_mode,
        )

    def get_bulkload_stream(self, schema_name: str, table_name: str, stream_id: str):
        return import_bulkload_api().get_bulkload_stream_metadata(
            self, schema_name, table_name, stream_id
        )

    def open_bulkload_stream_writer(
            self,
            instance_id: int,
            workspace: str,
            schema_name: str,
            table_name: str,
            stream_id: str,
            partition_id: int,
    ):
        return import_bulkload_api().open_bulkload_stream_writer(
            self,
            instance_id,
            workspace,
            schema_name,
            table_name,
            stream_id,
            partition_id,
        )

    def finish_bulkload_stream_writer(
            self,
            instance_id: int,
            workspace: str,
            schema_name: str,
            table_name: str,
            stream_id: str,
            partition_id: int,
            written_files: list,
            written_lengths: list,
    ):
        return import_bulkload_api().finish_bulkload_stream_writer(
            self,
            instance_id,
            workspace,
            schema_name,
            table_name,
            stream_id,
            partition_id,
            written_files,
            written_lengths,
        )

    def set_config(self, sql: str) -> bool:
        set_statement = sql[len("set "):]
        set_key_value = set_statement.split("=", 1)
        if len(set_key_value) == 2 and set_key_value[0].strip() and set_key_value[1].strip():
            _log.info("setting config: '%s' = '%s'", set_key_value[0].strip(), set_key_value[1].strip())
            self.connect_context.set_config(set_key_value[0].strip(), set_key_value[1].strip())
            return True
        else:
            raise Exception(f"Invalid statement {sql}")

    def get_configs(self) -> Dict[str, str]:
        return self.connect_context.configs

    def get_config(self, key: str) -> Optional[str]:
        return self.get_configs().get(key, None)

    def pre_process_sql(self, sql: str) -> Optional[str]:
        multi_queries = split_sql(sql)
        for i, query in enumerate(multi_queries):
            query = query.strip()
            if not query:
                continue

            if not self.process_use_cmd(query):
                return multi_queries[i] + "\n;"
        return None

    def check_schema(self, schema: str = None):
        if not schema:
            raise Exception("schema is required")
        from clickzetta.connector.v0.connection import connect
        with connect(client=self) as conn:
            try:
                conn.cursor().execute("desc schema " + schema)
            except Exception as e:
                if "schema not found" in str(e):
                    raise ClickzettaClientException(f"schema {schema} not found")
                else:
                    raise e
        return schema

    def process_use_cmd(self, origin_sql: str) -> bool:
        sql = strip_leading_comment(origin_sql)
        sql = re.sub(r"\s+", " ", sql)
        lower_sql = sql.lower()
        has_processed = True
        if lower_sql.startswith("use "):
            self.config_statements.append(sql)
            if lower_sql.startswith("use vcluster "):
                vc = sql[len("use vcluster "):].strip()
                if " " in vc:
                    raise ClickzettaClientException("invalid vcluster: " + vc)
                self.vcluster = vc
            elif lower_sql.startswith(sql_prefix := "use schema ") or lower_sql.startswith(sql_prefix := "use "):
                self.schema = self.check_schema(self.get_real_schema(sql[len(sql_prefix):].strip()))
        elif lower_sql.startswith("set "):
            self.config_statements.append(sql)
            self.set_config(sql)
        else:
            has_processed = False
        return has_processed

    @staticmethod
    def get_real_schema(schema: str) -> str:
        schema = schema.split(".")[-1]
        if schema.startswith("`") and schema.endswith("`"):
            schema = schema[1:-1]
        return schema

    def submit_sql_job(
            self,
            token: str,
            sql: str,
            job_id: JobID,
            parameters=None,
            schema=None,
            asynchronous: bool = False,
    ) -> QueryResult:
        sql = self.pre_process_sql(sql)
        if (not sql) or "test plain returns" in sql or "test unicode returns" in sql:
            query_result = QueryResult({}, self._pure_arrow_decoding, timezone_hint=self.timezone_hint)
            query_result.data = QueryData(
                data=[],
                data_type=QueryDataType.Memory,
                schema=[],
                time_zone=self.timezone_hint,
                pure_arrow_decoding=self._pure_arrow_decoding,
            )
            return query_result
        table = Table(self.workspace, "", self.instance, self.vcluster)
        vc = table.vcluster
        job_type = JobType.SQL_JOB
        _log.info("clickzetta connector submitting job,  id:" + job_id.id)
        job_name = "SQL_JOB"
        user_id = 0
        reqeust_mode = JobRequestMode.HYBRID
        job_config = {}
        polling_timeout = 30
        sdk_job_priority = 0
        max_retry = self.max_retries
        sdk_job_timeout = 0
        hints = self.get_configs()
        hints.update(self.hints)
        if parameters is not None and len(parameters) != 0:
            if "hints" in parameters.keys():
                hints = parameters["hints"]
            for key in parameters.keys():
                if "%(" + key + ")s" in sql:
                    sql = sql.replace("%(" + key + ")s", str(parameters[key]))
        if len(hints) > 0:
            for key in hints:
                if key == "sdk.job.polling.timeout":
                    polling_timeout = int(hints[key])
                elif key in ("sdk.job.priority", "schedule_job_queue_priority", "priority"):
                    # The schedule_job_queue_priority and priority is adaptive to the java sdk set flag.
                    sdk_job_priority = int(hints[key])
                elif key in ('sdk.query.timeout.ms', 'querytimeout'):
                    # The querytimeout is adaptive to the java sdk set flag.
                    sdk_job_timeout = int(hints[key]) / 1000
                elif key == "sdk.job.timeout":
                    sdk_job_timeout = int(hints[key])
                elif key == "sdk.query.max.retries":
                    max_retry = self.max_retries if int(hints[key]) <= 0 else int(hints[key])
                elif key == "cz.sql.timezone":
                    self.timezone_hint = hints[key]
                    self.connect_context.set_config(key, hints[key])
                elif key == "maxRowSize":
                    self.connect_context.maxRowSize = int(hints[key])
                    self.connect_context.configs["cz.sql.result.row.partial.limit"] = str(
                        self.connect_context.maxRowSize)
                else:
                    self.connect_context.set_config(key, hints[key])
        """
        The `polling_timeout` cannot exceed the threshold DEFAULT_MAXIMUM_TIMEOUT, otherwise it will be reset to the 
        default, which can avoid gateway and server timeout exceptions. If it exceeds the threshold, we will 
        switch to asynchronous result retrieval, and use `job_timeout_ms` to control the asynchronous timeout.  
        """
        if not asynchronous and (0 > polling_timeout or polling_timeout > DEFAULT_MAXIMUM_TIMEOUT):
            polling_timeout = DEFAULT_MAXIMUM_TIMEOUT
        sql_config = SQLJobConfig(0, "0", "0", self.connect_context.configs)
        schema_name = ""
        if self.schema is not None:
            schema_name = self.schema
        if schema is not None:
            schema_name = schema
        sql_job = SQLJob(sql_config, table.workspace, schema_name, [sql])
        user_agent = ""
        max_value = sys.maxsize
        job_timeout_ms = max_value if sdk_job_timeout >= max_value or sdk_job_timeout * 1000 >= max_value else sdk_job_timeout * 1000
        job_desc = JobDesc(
            vc,
            job_type,
            job_id,
            job_name,
            user_id,
            reqeust_mode,
            polling_timeout,
            job_config,
            sql_job,
            job_timeout_ms,
            user_agent,
            sdk_job_priority,
            "NORMAL",
            ClientContextInfo(self.config_statements, self.connect_context),
        )
        job_request = JobRequest(job_desc)
        data = json.dumps(job_request.to_api_repr())
        HEADERS["instanceName"] = table.instance
        HEADERS["X-ClickZetta-Token"] = token
        _log.debug("BEGIN TO SEND REQUEST:{0} TO CZ SERVER, TIME:{1}".format(sql, str(datetime.now())))
        return execute_with_retrying(
            partial(Client._handle_submit_sql, sql=sql, job_id=job_id.id, client=self, data=data),
            partial(Client._check_result_dict, sql=sql, client=self, job_id=job_id, job_timeout_ms=job_timeout_ms,
                    asynchronous=asynchronous),
            max_tries=max_retry
        )

    @staticmethod
    def sts_token_error(result_set: Dict):
        if "data" not in result_set and "location" in result_set:
            location = result_set["location"]
            if "fileSystem" in location and location["fileSystem"] == FileSystemType.GCS:
                return "sts_token" not in location or location["sts_token"] == ""
            else:
                return ("sts_ak_id" not in location or location["sts_ak_id"] == "") or (
                        "sts_ak_secret" not in location or location["sts_ak_secret"] == "")
        return False

    @staticmethod
    def _handle_submit_sql(sql, job_id: str, client, data, tried) -> dict:
        try:
            resp = client.session.post(client.service + "/lh/submitJob", data=data, headers=HEADERS)
            if resp is None:
                return {}
            resp.encoding = "utf-8"
            result = resp.text
        except Exception as e:
            _log.warning(
                f"Submit job throw Exception, {job_id} tried {tried}, got exception: {e}, "
                f"{e.msg}" if hasattr(e, "msg") else ""
            )
            raise e
        try:
            result_dict = json.loads(result)
        except Exception:
            _log.error(f"Job result parse failed, {job_id} tried {tried}, exception: {result}")
            raise Exception(f"Job result parse failed, {job_id} tried {tried}, exception: {result}")
        if not resp.ok:
            raise requests.exceptions.RequestException(
                f"submit sql job failed:{str(resp.status_code)} {resp.text}. sql:{sql}"
            )
        return result_dict

    @staticmethod
    def _check_result_dict(sql: str, client, job_id: JobID, job_timeout_ms, asynchronous, context: BaseExecuteContext,
                           tried, exception=None, **kwargs) -> bool:
        if exception is not None:
            msg = str(exception)
            if "NumberFormatException" in msg or "JSONDecodeError" in msg or "Illegal authorization" in msg:
                raise exception
            elif client.wait_continue_submit(job_id.id, tried):
                return False
            else:
                raise exception
        result = context.result
        if not result or "status" not in result:
            if client.wait_continue_submit(job_id.id, tried):
                return False
            else:
                raise Exception(
                    "Execute sql job:{0} failed, http response job status is not exist.{1}"
                    .format(str(job_id.id), (str(result) or ""))
                )
        elif Client.sts_token_error(result):
            raise Exception("Sts token is not generated")
        elif "errorCode" in result["status"] and result["status"]["errorCode"] != '':
            if (
                    result["status"]["errorCode"] == JOB_STATUS_UNKNOWN or
                    result["status"]["errorCode"] == JOB_NOT_SUBMITTED
            ):
                if client.wait_continue_submit(job_id.id, tried):
                    return False
                else:
                    raise Exception(
                        f"Job status is unknown, job_id:{job_id.id}, tried {tried} times"
                    )
            elif result["status"]["errorCode"] == JOB_ALREADY_EXIST:
                # Return True and will get the result in the next step
                pass
            elif result["status"]["errorCode"] == JOB_TIME_OUT_KILLED:
                raise Exception(
                    "submit sql job:{0} timeout. the sdk.job.timeout is {1} ms. killed by lakehouse"
                    .format(str(job_id.id), str(job_timeout_ms))
                )
            # When the error code is CZLH-57015, jobid needs to be generated to resubmit the job
            elif result["status"]["errorCode"] == JOB_NEED_RE_EXECUTE or result["status"][
                "errorCode"] == JOB_VC_QUEUE_EXCEEDED:
                if client.wait_continue_submit(job_id.id, tried):
                    return False
                else:
                    raise Exception(
                        f"Job need re-execute, job_id:{job_id.id}, error:"
                        f"{result['status']['errorCode']}"
                    )
            else:
                error_msg = result["status"]["errorMessage"] if "errorMessage" in result["status"] else ""
                success_msg = result["status"]["message"] if "message" in result["status"] else ""
                state = result["status"]["state"]
                msg = state + " " + (error_msg if error_msg != "" else success_msg)
                raise Exception(
                    f"submit sql job:{job_id.id} failed, error:{result['status']['errorCode']} {msg}"
                )
        try:
            context.result = client.retry_get_result(
                result, job_id, job_timeout_ms, asynchronous, sql
            )
        except ClickzettaJobNotExistsException as e:
            if client.wait_continue_submit(job_id.id, 1):
                return False
            else:
                raise e
        return True

    def process_volume_sql(
            self, token: str, sql: str, query_result: QueryResult
    ) -> QueryResult:
        upper_sql = sql.strip().upper()
        outcome = list(query_result.data)
        if len(outcome) != 1:
            raise Exception(f"get volume sql failed, with result: {outcome}")
        outcome_obj = json.loads(outcome[0][0])
        if outcome_obj["status"] == "FAILED":
            raise Exception(
                f"{outcome_obj['request']['command']} volume failed: {outcome_obj['error']}"
            )
        if upper_sql.startswith("GET"):
            query_result = self.gen_volume_result(outcome_obj)
            if outcome_obj["status"] == "CONTINUE" and outcome_obj["nextMarker"] != "":
                sql = "set cz.sql.volume.file.transfer.next.marker=nextMarker;" + sql
                job_id = JobID(
                    self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID
                )
                next_result = self.submit_sql_job(token=token, sql=sql, job_id=job_id)
                query_result.data = self._merge_get_query_result(query_result.data.data, next_result.data.data,
                                                                 query_result.schema)
            return query_result
        elif upper_sql.startswith("PUT"):
            if outcome_obj["status"] == "CONTINUE":
                sql = self._gen_new_put_sql(outcome_obj)
                job_id = JobID(
                    self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID
                )
                return self.submit_sql_job(token=token, sql=sql, job_id=job_id)
            else:
                return self.gen_volume_result(outcome_obj)

    def gen_volume_result(self, outcome: Any):
        result_list = []
        if outcome["request"]["command"].upper() == "GET":
            volume_files = self.get_volume_files(outcome)
            for i in range(len(volume_files)):
                response = requests.get(
                    outcome["ticket"]["presignedUrls"][i], stream=True
                )
                local_path = os.path.join(
                    normalize_file_path(outcome["request"]["localPaths"][0]), volume_files[i]
                )
                if not os.path.exists(os.path.dirname(local_path)):
                    os.makedirs(os.path.dirname(local_path))
                with open(local_path, "wb") as out_file:
                    shutil.copyfileobj(response.raw, out_file)
                _log.info(
                    f"get volume success, volume_path:{volume_files[i]}, local_path:{local_path}"
                )
                res = [volume_files[i], local_path, os.path.getsize(local_path)]
                result_list.append(res)
        elif outcome["request"]["command"].upper() == "PUT":
            if outcome["status"] == "SUCCESS":
                for i in range(len(outcome["ticket"]["presignedUrls"])):
                    local_path = normalize_file_path(outcome["request"]["localPaths"][i])
                    if not os.path.exists(local_path):
                        raise Exception(
                            f"put volume failed, local_path:{local_path} not exists"
                        )
                    with open(local_path, "rb") as f:
                        requests.put(
                            outcome["ticket"]["presignedUrls"][i],
                            data=f.read(),
                            headers={},
                        )
                    _log.info(f"put volume success, local_path:{local_path}")
                    volume_file = ""
                    if "file" in outcome["request"]:
                        volume_file = outcome["request"]["file"]
                    else:
                        if "subdirectory" in outcome["request"]:
                            path = outcome["request"]["subdirectory"]
                            path = path if path[-1] == "/" else path + "/"
                            volume_file = path + os.path.basename(local_path)
                        else:
                            volume_file = os.path.basename(local_path)
                    res = [local_path, volume_file, os.path.getsize(local_path)]
                    result_list.append(res)
        query_result = QueryResult({}, self._pure_arrow_decoding, timezone_hint=self.timezone_hint)
        query_result.data = QueryData(
            data=result_list,
            data_type=QueryDataType.Memory,
            schema=query_result.schema,
            pure_arrow_decoding=self._pure_arrow_decoding,
        )
        return query_result

    def get_volume_files(self, outcome: Any):
        volume_files = []
        if "file" in outcome["request"]:
            volume_files.append(
                os.path.basename(os.path.abspath(outcome["request"]["file"]))
            )
        else:
            for url in outcome["ticket"]["presignedUrls"]:
                parsed_url = urlparse(unquote(url))
                path_parts = parsed_url.path.split("/")
                volume_files.append(path_parts[-1])
        return volume_files

    def __check_not_submitted(self, job_id: str) -> bool:
        try:
            get_job_result_response = self.get_job_result(job_id)
            return get_job_result_response is None
        except Exception as e:
            _log.warning(f"get job result failed! e:{str(e)}")
            return True

    def wait_continue_submit(self, job_id: str, tried) -> bool:
        if tried >= self.max_retries:
            return False

        time.sleep(0.5 * tried)
        if self.__check_not_submitted(job_id):
            try:
                time.sleep(0.5 * tried)
                return True
            except InterruptedError:
                _log.error(
                    f"Receive Interrupt but Job {job_id} is still running..."
                )
        return False

    def close(self):
        try:
            if hasattr(self, '_igs_client') and self._igs_client:
                self._igs_client.close()
            if hasattr(self, '_rpc_proxy'):
                self._rpc_proxy.close()
        except Exception as e:
            raise CZException(f"Failed to close client: {e}")

    def cancel_job(self, job_id: JobID, headers=HEADERS):
        account = ClickZettaAccount(0)
        cancel_job_request = CancelJobRequest(
            account, job_id, "", False
        )
        path = "/lh/cancelJob"
        data = json.dumps(cancel_job_request.to_api_repr())
        try:
            self.session.post(self.service + path, data=data, headers=headers)
        except Exception as e:
            _log.error("clickzeta connector cancel job error:{}".format(e))

    def _check_job_timeout_with_cancel(
            self, job_timeout_ms, sql_job_start_time: datetime, job_id
    ):
        if job_timeout_ms > 0:
            time_range: timedelta = datetime.now() - sql_job_start_time
            if time_range.seconds > job_timeout_ms / 1000:
                self.cancel_job(job_id)
                return True
        return False

    def get_job_profile(self, job_id: str):
        result_dict = self._get_job(job_id, JobRequestType.PROFILE)
        return result_dict

    def get_job_result(self, job_id: str) -> dict:
        result_dict = self._get_job(job_id, JobRequestType.RESULT)
        return result_dict

    def get_job_progress(self, job_id: str):
        result_dict = self._get_job(job_id, JobRequestType.PROGRESS)
        return result_dict

    def get_job_summary(self, job_id: str):
        result_dict = self._get_job(job_id, JobRequestType.SUMMARY)
        return result_dict

    def get_job_plan(self, job_id: str):
        result_dict = self._get_job(job_id, JobRequestType.PLAN)
        return result_dict

    def _get_job(self, job_id: str, type: str):
        HEADERS["instanceName"] = self.instance
        HEADERS["X-ClickZetta-Token"] = self.token
        id = JobID(job_id, self.workspace, DEFAULT_INSTANCE_ID)
        account = ClickZettaAccount(0)
        get_job_request = GetJobRequest(account, id, 0, "")
        path = "/lh/getJob"
        try:
            api_request = APIGetJobRequest(get_job_request, "", type)
            data = json.dumps(api_request.to_api_repr())
            api_response = self.session.post(
                self.service + path, data=data, headers=HEADERS
            )

            result = ""
            try:
                result = api_response.text
                result_dict = json.loads(result)
            except Exception as ex:
                raise Exception(f"Parsing json data for job id [{job_id}] failed:{ex}. raw result is: {result}")
            if int(result_dict["code"]) != 200:
                raise requests.exceptions.RequestException(
                    f"Get job:{id.id} {type} failed:{result_dict}"
                )
            return result_dict
        except AttributeError as e:
            raise requests.exceptions.RequestException(
                f"Get job:{id.id} {type} failed:{e}, Only supports：result/progress/summary/plan/profile."
            )
        except Exception as e:
            raise Exception(f"Get job:{id.id} {type} failed:{e}")

    @staticmethod
    def verify_result_dict_finished(result_dict):
        if result_dict and "status" in result_dict:
            if "state" in result_dict["status"]:
                error_msg = result_dict["status"]["errorMessage"] if "errorMessage" in result_dict["status"] else ""
                success_msg = result_dict["status"]["message"] if "message" in result_dict["status"] else ""
                state = result_dict["status"]["state"]
                msg = state + " " + (error_msg if error_msg != "" else success_msg)
                if "errorCode" in result_dict["status"] and len(result_dict["status"]["errorCode"]) > 0:
                    error_code = result_dict["status"]["errorCode"]
                    # Return False and will retry to get the result
                    if error_code == JOB_ALREADY_EXIST:
                        return False
                    msg = f"Execute sql job failed, error: {msg or error_code}"
                    _log.error(msg)
                    if error_code == JOB_NOT_EXIST:
                        # It will raise exception and resubmit job in the next step
                        raise ClickzettaJobNotExistsException(
                            msg, error_code=error_code
                        )
                    else:
                        raise Exception(msg)
                if state == "SUCCEED":
                    return True
                elif state == "FAILED":
                    if msg.__contains__(JOB_ALREADY_EXIST):
                        return False
                    else:
                        raise Exception(
                            f"Execute sql job failed, error: {msg}"
                        )
                elif state == "CANCELLED" or state == "CANCELLING":
                    raise Exception(
                        f"Job is cancelled/cancelling. {msg}"
                    )
                elif state == "QUEUEING" or state == "SETUP" or state == "RUNNING" or state == "RESUMING_CLUSTER":
                    if "resultSet" in result_dict:
                        if "data" in result_dict["resultSet"]:
                            if "data" in result_dict["resultSet"]["data"]:
                                return True
                        elif "location" in result_dict["resultSet"]:
                            return True
                else:
                    raise Exception(
                        f"Execute sql job failed, error: {msg}"
                    )
        return False

    def retry_get_result(self, result_dict, job_id: JobID, job_timeout_ms=0, asynchronous=False,
                         sql=None) -> QueryResult:
        if not result_dict or "status" not in result_dict or "state" not in result_dict["status"]:
            raise Exception(
                "Execute sql job:{0} failed, http response job status is not exist.".format(str(job_id.id))
            )
        # The first round of result verification, if the job is finished, return the result directly
        already_finished = Client.verify_result_dict_finished(result_dict)
        if already_finished and asynchronous:
            return QueryResult(result_dict, self._pure_arrow_decoding, timezone_hint=self.timezone_hint)
        elif asynchronous:
            return QueryResult({}, self._pure_arrow_decoding, timezone_hint=self.timezone_hint)

        begin_time = datetime.now()
        sleep_time_ms = 50

        def _get_result_with_timeout(*args, **kwargs):
            if not self._check_job_timeout_with_cancel(job_timeout_ms, begin_time, job_id):
                return self.get_job_result(job_id.id)
            else:
                error_msg = (f"clickzetta sql job:{str(job_id.id)} timeout after {str(job_timeout_ms / 1000)} "
                             f"seconds. killed by sdk.")
                _log.error(error_msg)
                raise Exception(error_msg)

        def _handle_result_check(context, tried, exception=None, **kwargs):
            if exception:
                _log.warning("_handle_result_check warn: " + str(exception))
                raise exception
            result = context.result
            if not Client.verify_result_dict_finished(result):
                # Retry after `sleep_time_ms` seconds
                nonlocal sleep_time_ms
                second = sleep_time_ms / 1000.0
                time.sleep(second)
                # Randomly generate a waiting time from 5s to 30s, and avoid the latency caused by long waiting
                # time by shuffling the time series.
                if sleep_time_ms * 1.5 <= 10000:
                    sleep_time_ms = sleep_time_ms * 1.5
                else:
                    sleep_time_ms = random.randint(3000, 10000)
                    _log.info(
                        f"Get async sql job:{job_id.id} result pending, tried:{tried}, "
                        f"time cost:{begin_time - datetime.now()}, next sleep time:{sleep_time_ms}")
                return False
            else:
                return True

        if not already_finished:
            # max_tries=-1 means retrying until the job is finished
            result_dict = execute_with_retrying(
                _get_result_with_timeout,
                partial(_handle_result_check),
                lambda result, exception: self.cancel_job(job_id),
                max_tries=-1
            )
        return self._process_hint_job(sql, self.token,
                                      QueryResult(result_dict, self._pure_arrow_decoding,
                                                  timezone_hint=self.timezone_hint))

    def _process_hint_job(self, sql, token, query_result: QueryResult):
        _log.debug(
            "GET RESPONSE FROM CZ SERVER FOR REQUEST:{0} TIME:{1}".format(sql, str(datetime.now()))
        )
        if sql.lstrip().upper().startswith(
                "PUT "
        ) or sql.lstrip().upper().startswith("GET "):
            check_volume_result = self.process_volume_sql(token, sql, query_result)
            return check_volume_result
        return query_result

    def _format_job_id(self):
        unique_id = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        format_unique_id = unique_id.replace("-", "").replace(":", "").replace(
            ".", ""
        ).replace(" ", "") + str(random.randint(10000, 99999))
        return format_unique_id

    def generate_job_id(self):
        return self._format_job_id()

    def get_table_names(self, schema: str):
        job_id = JobID(self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID)
        query_result = self.submit_sql_job(
            token=self.token, sql="show tables;", schema=schema, job_id=job_id
        )
        query_data = list(query_result.data)
        table_names = []
        for entry in query_data:
            table_names.append(entry[1])

        return table_names

    def get_schemas(self):
        job_id = JobID(self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID)
        query_result = self.submit_sql_job(
            token=self.token, sql="show schemas;", job_id=job_id
        )
        query_data = list(query_result.data)
        schema_names = []
        for entry in query_data:
            schema_names.append(entry[0])

        return schema_names

    def get_columns(self, table_name: str, schema: str):
        job_id = JobID(self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID)
        if "." in table_name:
            schema = table_name.split(".")[0]
            table_name = table_name.split(".")[1]
        query_result = self.submit_sql_job(
            token=self.token,
            sql="select * from " + schema + "." + table_name + " limit 1;",
            job_id=job_id,
        )
        schema = query_result.schema

        return schema

    def has_table(self, full_table_name: str):
        job_id = JobID(self._format_job_id(), self.workspace, DEFAULT_INSTANCE_ID)
        try:
            query_result = self.submit_sql_job(
                token=self.token,
                sql="show create table " + full_table_name + ";",
                job_id=job_id,
            )
        except Exception as e:
            return False
        if query_result.state != "FAILED":
            return True
        else:
            return False

    def _merge_get_query_result(self, res1: List, res2: List, schema=None):
        new_data = res1 + res2
        data = QueryData(
            data=new_data,
            data_type=QueryDataType.Memory,
            schema=schema,
            pure_arrow_decoding=self._pure_arrow_decoding,
        )
        return data

    def _gen_new_put_sql(self, outcome_obj):
        prefix = ""
        option_part = ""
        for option in outcome_obj["request"]["options"]:
            if option["name"].upper() == "SOURCE_PREFIX":
                prefix = option["value"]
            option_part += " " + option["name"] + " = " + option["value"]
        requested_paths = outcome_obj["request"]["localPaths"]
        resolved_paths = []
        for p in requested_paths:
            resolved_paths += resolve_local_path(p)
        if len(resolved_paths) == 0:
            raise Exception("No local file to put into volume")
        sql = "PUT "
        sql += ", ".join(f"'{p}'" for p in resolved_paths)
        sql += " TO " + outcome_obj["request"]["volumeIdentifier"]
        if "subdirectory" in outcome_obj["request"]:
            sql += " SUBDIRECTORY '" + outcome_obj["request"]["subdirectory"] + "'"
        elif "file" in outcome_obj["request"]:
            sql += " FILE '" + outcome_obj["request"]["file"] + "'"
        sql += option_part
        sql += ";"
        return sql

    @property
    def unique_key(self):
        return self.username + "?" + self.instance

    @property
    def token(self) -> str:
        if self.magic_token is not None:
            return self.magic_token
        if _globals["user_token_dict"].__contains__(self.unique_key):
            if not self._token or self.will_soon_expire():
                now = int(round(time.time() * 1000))
                new_token = self.log_in_cz(self.username, self.password, self.instance)
                _globals["user_token_dict"][self.unique_key] = new_token
                _globals["user_token_time_dict"][self.unique_key] = now
                self.token = new_token
            else:
                self.token = _globals["user_token_dict"][self.unique_key]
        else:
            now = int(round(time.time() * 1000))
            new_token = self.log_in_cz(self.username, self.password, self.instance)
            _globals["user_token_dict"][self.unique_key] = new_token
            _globals["user_token_time_dict"][self.unique_key] = now
            self.token = new_token
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    def refresh_token(self):
        return self.token

    def will_soon_expire(self) -> bool:
        if not self.token_expire_time_ms or self.token_expire_time_ms == 0:
            return False
        expire_time = self.token_expire_time_ms * DEFAULT_EXPIRED_FACTOR
        now = int(round(time.time() * 1000))
        return now - _globals["user_token_time_dict"][self.unique_key] > expire_time

    def validate(self):
        if not self.service:
            raise ValueError("service must be specified")
        if not self.instance:
            raise ValueError("instance must be specified")
        if not self.workspace:
            raise ValueError("workspace must be specified")
        if not self.vcluster:
            raise ValueError("vcluster must be specified")
        if not ((self.username and self.password) or self.magic_token):
            raise ValueError("username and password or token must be specified")
        if not self.schema:
            self.schema = "public"


def _add_server_timeout_header(headers: Optional[Dict[str, str]], kwargs):
    timeout = kwargs.get("timeout")
    if timeout is not None:
        if headers is None:
            headers = {}
        headers[TIMEOUT_HEADER] = str(timeout)

    if headers:
        kwargs["headers"] = headers

    return kwargs
