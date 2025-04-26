# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import re
from datetime import datetime
from logging import getLogger
from re import Pattern
from typing import Any, Optional

import pandas as pd
from flask import current_app
from flask_babel import gettext as __
from sqlalchemy import types, Column
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.expression import Select
from sqlalchemy.sql.sqltypes import NullType
from sqlglot import Dialects

from superset import sql_parse, cache_manager
from superset.constants import TimeGrain, QUERY_CANCEL_KEY, USER_AGENT
from superset.db_engine_specs import BaseEngineSpec
from superset.db_engine_specs.hive import HiveEngineSpec
from superset.errors import SupersetErrorType
from superset.exceptions import DisallowedSQLFunction, SupersetTemplateException
from superset.models.core import Database
from superset.models.sql_lab import Query
from superset.models.sql_types.presto_sql_types import (
    Array,
    Interval,
    Map,
    Row,
    TinyInteger,
)
from superset.sql.parse import SQLGLOT_DIALECTS
from superset.sql_parse import ParsedQuery
from superset.utils.core import GenericDataType

_log = getLogger(__name__)

# Regular expressions to catch custom errors
CONNECTION_ACCESS_DENIED_REGEX = re.compile(
    "user:(?P<username>.*?) login to clickzetta failed"
)
SYNTAX_ERROR_REGEX = re.compile(
    "Syntax error at or near '(?P<server_error>.*)'"
)

SQLGLOT_DIALECTS["clickzetta"] = Dialects.CLICKZETTA

class ClickZettaEngineSpec(BaseEngineSpec):
    engine = "clickzetta"
    engine_name = "ClickZetta"

    # pylint: disable=line-too-long
    _time_grain_expressions = {
        None: "{col}",
        TimeGrain.SECOND: "date_trunc('second', {col})",
        TimeGrain.MINUTE: "date_trunc('minute', {col})",
        TimeGrain.HOUR: "date_trunc('hour', {col})",
        TimeGrain.DAY: "date_trunc('day', {col})",
        TimeGrain.WEEK: "date_trunc('week', {col})",
        TimeGrain.MONTH: "date_trunc('month', {col})",
        TimeGrain.QUARTER: "date_trunc('quarter', {col})",
        TimeGrain.YEAR: "date_trunc('year', {col})",
        TimeGrain.WEEK_ENDING_SATURDAY: (
            "date_trunc('week', {col} + interval '1 day') + interval '5 days'"
        ),
        TimeGrain.WEEK_STARTING_SUNDAY: (
            "date_trunc('week', {col} + interval '1 day') - interval '1 day'"
        ),
    }

    allows_alias_to_source_column = False
    # Order by aggregate can only be used on columns in the GROUP BY clause that do not
    # appear in the select list
    allows_hidden_orderby_agg = True
    # The database will run the query against the default schema, public, returning
    # data. Superset will check if the user has access to the secret schema instead,
    # allowing the query to run and returning results to the user.
    # This requires implementing a custom `adjust_engine_params` method.
    supports_dynamic_schema = True
    supports_catalog = True

    sqlalchemy_uri_placeholder = (
        "clickzetta://user:password@instance_name.host/workspace[?key=value&key=value...]"
    )

    _show_functions_column = "name"

    @classmethod
    def quote(cls, database: Database, dbtable: str, column=True, *args, **kwargs) -> str:
        dialect = database.get_dialect()
        if not dbtable.startswith(dialect.identifier_preparer.initial_quote):
            dbtable = dialect.identifier_preparer.quote(
                ident=dbtable, column=column)
        return dbtable

    @classmethod
    def adjust_engine_params(  # pylint: disable=unused-argument
            cls,
            uri: URL,
            connect_args: dict[str, Any],
            catalog: str | None = None,
            schema: str | None = None, *args, **kwargs
    ) -> tuple[URL, dict[str, Any]]:
        """
               Return a new URL and ``connect_args`` for a specific catalog/schema.
        """
        if schema:
            uri_schema = uri.query.get('schema', None)
            if not uri_schema or uri_schema != schema:
                query_ = dict(uri.query)
                query_['schema'] = schema
                uri = uri.set(query=query_)

        return uri, connect_args

    @classmethod
    def execute(  # pylint: disable=unused-argument
            cls,
            cursor: Any,
            query: str, *args, **kwargs
    ) -> None:
        """
        Execute a SQL query

        :param cursor: Cursor instance
        :param query: Query to execute
        :param kwargs: kwargs to be passed to cursor.execute()
        :return:
        """
        if not cls.allows_sql_comments:
            query = sql_parse.strip_comments_from_sql(query, engine=cls.engine)
        disallowed_functions = current_app.config["DISALLOWED_SQL_FUNCTIONS"].get(
            cls.engine, set()
        )
        if sql_parse.check_sql_functions_exist(query, disallowed_functions, cls.engine):
            raise DisallowedSQLFunction(disallowed_functions)

        if cls.arraysize:
            cursor.arraysize = cls.arraysize
        cancel_query_id = kwargs.get("cancel_query_id")
        try:
            # https://github.com/apache/superset/pull/4319
            # #4319 datetime might be tz aware, we should set it to utc to avoid
            # conversion exceptions caused by Tz-aware datetime.
            # also, as following:
            # https://github.com/apache/superset/discussions/19176
            # The superset timezone is always UTC.
            # But we need local time, so ignore the documentation's restriction
            my_param = {'hints': {}}
            # my_param['hints']['cz.sql.timezone'] = 'UTC'
            cursor.execute_with_job_id(query, job_id=cancel_query_id,
                                       parameters=my_param)
        except Exception as ex:
            _log.error("ClickZettaEngineSpec.execute: %s", ex)
            raise cls.get_dbapi_mapped_exception(ex) from ex

    @classmethod
    def execute_with_cursor(cls, cursor: Any, sql: str, query: Query, *args, **kwargs) -> None:
        """
        Trigger execution of a query and handle the resulting cursor.

        For most implementations this just makes calls to `execute` and
        `handle_cursor` consecutively, but in some engines (e.g. Trino) we may
        need to handle client limitations such as lack of async support and
        perform a more complicated operation to get information from the cursor
        in a timely manner and facilitate operations such as query stop
        """
        cancel_query_id = query.extra.get(QUERY_CANCEL_KEY)
        _log.debug("Query %s: Running query: %s", cancel_query_id, sql)
        cls.execute(cursor, sql, cancel_query_id=cancel_query_id)
        _log.debug("Query %s: Handling cursor", cancel_query_id)
        cls.handle_cursor(cursor, query)

    @classmethod
    @cache_manager.data_cache.memoize()
    def get_function_names(cls, database: Database, *args, **kwargs) -> list[str]:
        """
        Get a list of function names that are able to be called on the database.
        Used for SQL Lab autocomplete.

        :param database: The database to get functions for
        :return: A list of function names useable in the database
        """
        return HiveEngineSpec.get_function_names(cls, database)

    @classmethod
    def is_readonly_query(cls, parsed_query: ParsedQuery, *args, **kwargs) -> bool:
        """Pessimistic readonly, 100% sure statement won't mutate anything"""
        return HiveEngineSpec.is_readonly_query(parsed_query)

    @staticmethod
    def get_extra_params(database: Database, *args, **kwargs) -> dict[str, Any]:
        """
        Add a user agent to be used in the requests.
        Trim whitespace from connect_args to avoid databricks driver errors
        """
        extra: dict[str, Any] = BaseEngineSpec.get_extra_params(database)
        engine_params: dict[str, Any] = extra.setdefault("engine_params", {})
        connect_args: dict[str, Any] = engine_params.setdefault("connect_args", {})

        connect_args.setdefault("http_headers", [("User-Agent", USER_AGENT)])
        connect_args.setdefault("_user_agent_entry", USER_AGENT)

        # trim whitespace from http_path to avoid databricks errors on connecting
        if http_path := connect_args.get("http_path"):
            connect_args["http_path"] = http_path.strip()

        return extra

    @classmethod
    def get_allow_cost_estimate(cls, extra: dict[str, Any], *args, **kwargs) -> bool:
        return False

    @classmethod
    def get_catalog_names(
            cls,
            database: Database,
            inspector: Inspector, *args, **kwargs
    ) -> list[str]:
        """
        Get all catalogs.
        """
        catalogs = inspector.bind.execute("SHOW CATALOG") or []
        return [catalog["workspace_name"] for catalog in catalogs]

    column_type_mappings = (
        (
            re.compile(r"^bool(ean)?", re.IGNORECASE),
            types.BOOLEAN(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^tinyint", re.IGNORECASE),
            TinyInteger(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^smallint", re.IGNORECASE),
            types.SmallInteger(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^integer", re.IGNORECASE),
            types.INTEGER(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^bigint", re.IGNORECASE),
            types.BigInteger(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^float.*", re.IGNORECASE),
            types.FLOAT(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^double", re.IGNORECASE),
            types.FLOAT(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^decimal.*", re.IGNORECASE),
            types.DECIMAL(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^binary", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^string", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^varchar(\((\d+)\))*$", re.IGNORECASE),
            lambda match: types.VARCHAR(int(match[2])) if match[2] else types.VARCHAR(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^char(\((\d+)\))*$", re.IGNORECASE),
            lambda match: types.CHAR(int(match[2])) if match[2] else types.CHAR(1),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^vector", re.IGNORECASE),
            types.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^json", re.IGNORECASE),
            types.JSON(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^date.*", re.IGNORECASE),
            types.Date(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^timestamp_ltz", re.IGNORECASE),
            types.TIMESTAMP(timezone=True),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^timestamp_ntz", re.IGNORECASE),
            types.TIMESTAMP(timezone=False),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^interval.*", re.IGNORECASE),
            Interval(),
            GenericDataType.TEMPORAL,
        ),
        (re.compile(r"^array.*", re.IGNORECASE), Array(), GenericDataType.STRING),
        (re.compile(r"^map.*", re.IGNORECASE), Map(), GenericDataType.STRING),
        (re.compile(r"^struct.*", re.IGNORECASE), Row(), GenericDataType.STRING),
        (
            re.compile(r"^null", re.IGNORECASE), NullType(), GenericDataType.STRING),
        (re.compile(r"^bitmap", re.IGNORECASE), types.String(), GenericDataType.STRING),
    )

    custom_errors: dict[Pattern[str], tuple[str, SupersetErrorType, dict[str, Any]]] = {
        CONNECTION_ACCESS_DENIED_REGEX: (
            __('Either the username "%(username)s" or the password is incorrect.'),
            SupersetErrorType.CONNECTION_ACCESS_DENIED_ERROR,
            {"invalid": ["username", "password"]},
        ),
        SYNTAX_ERROR_REGEX: (
            __(
                'Please check your query for syntax errors near "%(server_error)s". '
                "Then, try running your query again."
            ),
            SupersetErrorType.SYNTAX_ERROR,
            {},
        ),
    }

    @classmethod
    def convert_dttm(
            cls, target_type: str, dttm: datetime, db_extra: dict[str, Any] | None = None
            , *args, **kwargs
    ) -> str | None:
        return HiveEngineSpec.convert_dttm(target_type, dttm, db_extra=db_extra)

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return HiveEngineSpec.epoch_to_dttm()

    @classmethod
    def get_cancel_query_id(cls, cursor: Any, query: Query, *args, **kwargs) -> str | None:
        """
        Select identifiers from the database engine that uniquely identifies the
        queries to cancel. The identifier is typically a session id, process id
        or similar.

        :param cursor: Cursor instance in which the query will be executed
        :param query: Query instance
        :return: Query identifier
        """
        return cursor.query_id()

    @classmethod
    def cancel_query(cls, cursor: Any, query: Query, cancel_query_id: str, *args, **kwargs) -> bool:
        """
        Cancel query in the underlying database.

        :param cursor: New cursor instance to the db of the query
        :param query: Query instance
        :param cancel_query_id: ClickZetta job id
        :return: True if query cancelled successfully, False otherwise
        """
        try:
            cursor.cancel(cancel_query_id)
        except Exception:  # pylint: disable=broad-except
            return False

        return True

    @classmethod
    def get_schema_from_engine_params(
            cls,
            sqlalchemy_uri: URL,
            connect_args: dict[str, Any], *args, **kwargs
    ) -> Optional[str]:
        """
        Return the configured schema.

        For Clickzetta the SQLAlchemy URI looks like this:

            clickzetta://user:password@instance_name.host/workspace[?schema=schema_name&key=value...]

        """
        return sqlalchemy_uri.query.get("schema", None)

    @classmethod
    def _partition_query(  # pylint: disable=too-many-arguments
            cls,
            table_name: str,
            schema: str | None,
            indexes: list[dict[str, Any]],
            database: Database, *args, **kwargs
    ) -> str:
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        return f"SHOW PARTITIONS {cls.quote(database, full_table_name, column=True)}"

    @classmethod
    def _extract_partition_info_from_df(cls, df: pd.DataFrame, *args, **kwargs) -> tuple[
        list[str], list[str] | None]:
        """Clickzetta partitions look like ds={partition name}/ds={partition name}"""
        if df.empty:
            return [], None
        partitions = df.iloc[:, 0].max().split("/")
        names, values = zip(*(partition.split("=") for partition in partitions))
        return list(names), list(values)

    @classmethod
    def is_partitioned_table(
            cls,
            database: Database,
            schema: str | None,
            dbtable: str, *args, **kwargs
    ) -> bool:
        desc_sql = f"DESC TABLE {dbtable}"

        try:
            df = database.get_df(sql=desc_sql, schema=schema)

            found_partition_info = False
            found_col = False

            for row in df.itertuples(index=False):
                if row[0] == "# Partition Information":
                    found_partition_info = True
                elif found_partition_info and row[0] == "# col_name":
                    found_col = True
                elif found_col:
                    # If we've found both "# Partition Information" and "# col_name",
                    # and we're now on a subsequent row, it's a partitioned table
                    return True

            return False

        except Exception as e:
            # Handle any exceptions (e.g., database errors)
            print(f"Error checking if table is partitioned: {e}")
            return False

    @classmethod
    @cache_manager.data_cache.memoize(timeout=60)
    def latest_partition(  # pylint: disable=too-many-arguments
            cls,
            table_name: str,
            schema: str | None,
            database: Database,
            show_first: bool = False,
            indexes: list[dict[str, Any]] | None = None, *args, **kwargs
    ) -> tuple[list[str], list[str] | None]:
        """Returns col name and the latest (max) partition value for a table

        :param table_name: the name of the table
        :param schema: schema / database / namespace
        :param database: database query will be run against
        :type database: models.Database
        :param show_first: displays the value for the first partitioning key
          if there are many partitioning keys
        :param indexes: indexes from the database
        :type show_first: bool

        >>> latest_partition('foo_table')
        (['ds'], ('2018-01-01',))
        """
        dbtable = cls.quote(database,
                            dbtable=schema + "." + table_name if schema else table_name)

        if not cls.is_partitioned_table(database, None, dbtable):
            raise SupersetTemplateException(
                f"Error getting partition for {schema}.{table_name}. "
                "Verify that this table has a partition."
            )

        return cls._extract_partition_info_from_df(
            df=database.get_df(
                sql=cls._partition_query(
                    table_name,
                    schema,
                    indexes,
                    database,
                    limit=1,
                    order_by=None,
                ),
                schema=schema,
            )
        )

    @classmethod
    def where_latest_partition(  # pylint: disable=too-many-arguments
            cls, *args, **kwargs
    ) -> Select | None:
        try:
            table_name = kwargs["table_name"]
            schema = kwargs["schema"]
            database = kwargs["database"]
            columns = kwargs.get("columns")
            query = kwargs.get("query") or database.get_sqla_engine().query

            col_names, values = cls.latest_partition(
                table_name, schema, database, show_first=True
            )
        except Exception:  # pylint: disable=broad-except
            # table is not partitioned
            return None

        if values is not None and columns is not None:
            for col_name, value in zip(col_names, values):
                for clm in columns:
                    if clm.get("name") == col_name:
                        query = query.where(Column(col_name) == value)

            return query
        return None

    @classmethod
    def get_url_for_impersonation(
        cls, url: URL, *args, **kwargs
    ) -> URL:
        """
        Return a modified URL with the username set.

        :param url: SQLAlchemy URL object
        """
        # Do nothing in the URL object since instead this should modify
        # the configuration dictionary.
        # Superset and clickzetta's username are not equivalent,
        # so no processing is done here.
        return url

    @classmethod
    def get_view_names(
            cls,
            database: Database,
            inspector: Inspector,
            schema: str | None, *args, **kwargs
    ) -> set[str]:
        """
        Get all the view names within the specified schema.

        Per the SQLAlchemy definition if the schema is omitted the database’s default
        schema is used, however some dialects infer the request as schema agnostic.

        :param database: The database to inspect
        :param inspector: The SQLAlchemy inspector
        :param schema: The schema to inspect
        :returns: The view names
        """
        query = (f"SHOW TABLES IN {cls.quote(database, dbtable=schema, column=False)}"
                 f" WHERE is_view=true") if schema else "SHOW TABLES WHERE is_view=true"

        try:
            result = database.get_df(query)
            views = set(
                result['table_name'].tolist() if 'table_name' in result.columns else [])
        except Exception as ex:
            raise cls.get_dbapi_mapped_exception(ex) from ex

        return views
