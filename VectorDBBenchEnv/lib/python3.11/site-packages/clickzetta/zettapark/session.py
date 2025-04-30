#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import atexit
import logging
import re
import sys
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime
from functools import reduce
from logging import getLogger
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from clickzetta.zettapark._connector import (
    ClickzettaConnection as Connection,
    ProgrammingError,
)
from clickzetta.zettapark._internal.analyzer import analyzer_utils
from clickzetta.zettapark._internal.analyzer.analyzer import Analyzer
from clickzetta.zettapark._internal.analyzer.expression import Attribute
from clickzetta.zettapark._internal.analyzer.query_plan import QueryPlanBuilder
from clickzetta.zettapark._internal.analyzer.query_plan_node import Range, Values
from clickzetta.zettapark._internal.analyzer.select_statement import (
    SelectSQL,
    SelectStatement,
    SelectTableFunction,
    SelectValuesSQL,
)
from clickzetta.zettapark._internal.analyzer.table_function import (
    GeneratorTableFunction,
    TableFunctionRelation,
)
from clickzetta.zettapark._internal.error_message import (
    ZettaparkClientExceptionMessages,
)
from clickzetta.zettapark._internal.server_connection import ServerConnection
from clickzetta.zettapark._internal.telemetry import set_api_call_source
from clickzetta.zettapark._internal.type_utils import (
    ColumnOrName,
    infer_schema,
    infer_type,
    merge_type,
)
from clickzetta.zettapark._internal.udf.udaf import UDAFRegistration
from clickzetta.zettapark._internal.udf.udf import UDFRegistration
from clickzetta.zettapark._internal.udf.udtf import UDTFRegistration
from clickzetta.zettapark._internal.utils import (
    TempObjectType,
    _is_pandas_df,
    generate_random_alphanumeric,
    get_connector_version,
    get_os_name,
    get_python_version,
    get_version,
    quote_name,
    random_name_for_temp_object,
    trim_trailing_semicolon_and_whitespace,
    validate_object_name,
)
from clickzetta.zettapark.column import Column
from clickzetta.zettapark.dataframe import DataFrame
from clickzetta.zettapark.dataframe_reader import DataFrameReader
from clickzetta.zettapark.exceptions import ZettaparkClientException
from clickzetta.zettapark.file_operation import FileOperation
from clickzetta.zettapark.functions import column, lit
from clickzetta.zettapark.query_history import QueryHistory
from clickzetta.zettapark.row import Row
from clickzetta.zettapark.table import Table
from clickzetta.zettapark.table_function import (
    TableFunctionCall,
    _create_table_function_expression,
)
from clickzetta.zettapark.types import DataType, StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDF

_logger = getLogger(__name__)

_session_management_lock = RLock()
_active_sessions: Set["Session"] = set()
WRITE_PANDAS_CHUNK_SIZE: int = None


def _get_active_session() -> "Session":
    with _session_management_lock:
        if len(_active_sessions) == 1:
            return next(iter(_active_sessions))
        elif len(_active_sessions) > 1:
            raise ZettaparkClientExceptionMessages.MORE_THAN_ONE_ACTIVE_SESSIONS()
        else:
            raise ZettaparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()


def _get_active_sessions() -> Set["Session"]:
    with _session_management_lock:
        if len(_active_sessions) >= 1:
            # TODO: This function is allowing unsafe access to a mutex protected data
            #  structure, we should ONLY use it in tests
            return _active_sessions
        else:
            raise ZettaparkClientExceptionMessages.SERVER_NO_DEFAULT_SESSION()


def _add_session(session: "Session") -> None:
    with _session_management_lock:
        _active_sessions.add(session)


def _close_session_atexit():
    """
    This is the helper function to close all active sessions at interpreter shutdown. For example, when a jupyter
    notebook is shutting down, this will also close all active sessions and make sure send all telemetry to the server.
    """
    with _session_management_lock:
        for session in _active_sessions.copy():
            try:
                session.close()
            except Exception:
                pass


# Register _close_session_atexit so it will be called at interpreter shutdown
atexit.register(_close_session_atexit)


def _remove_session(session: "Session") -> None:
    with _session_management_lock:
        try:
            _active_sessions.remove(session)
        except KeyError:
            pass


class Session:
    """
    Establishes a connection with a ClickZetta database and provides methods for creating DataFrames
    and accessing objects for working with files in volumes.

    When you create a :class:`Session` object, you provide connection parameters to establish a
    connection with a ClickZetta database (e.g. an account, a user name, etc.). You can
    specify these settings in a dict that associates connection parameters names with values.
    The Zettapark library uses `the Clickzetta Connector for Python <https://doc.clickzetta.com/>`_
    to connect to ClickZetta. Refer to
    `Connecting to ClickZetta using the Python Connector <https://doc.clickzetta.com/>`_
    for the details of `Connection Parameters <https://doc.clickzetta.com/>`_.

    To create a :class:`Session` object from a ``dict`` of connection parameters::

        >>> connection_parameters = {
        ...     "user": "<user_name>",
        ...     "password": "<password>",
        ...     "account": "<account_name>",
        ...     "role": "<role_name>",
        ...     "warehouse": "<warehouse_name>",
        ...     "database": "<database_name>",
        ...     "schema": "<schema_name>",
        ... }
        >>> session = Session.builder.configs(connection_parameters).create() # doctest: +SKIP

    To create a :class:`Session` object from an existing Python Connector connection::

        >>> session = Session.builder.configs({"connection": <your python connector connection>}).create() # doctest: +SKIP

    :class:`Session` contains functions to construct a :class:`DataFrame` like :meth:`table`,
    :meth:`sql` and :attr:`read`, etc.

    A :class:`Session` object is not thread-safe.
    """

    class RuntimeConfig:
        def __init__(self, session: "Session", conf: Dict[str, Any]) -> None:
            self._session = session
            self._conf = {
                "use_constant_subquery_alias": True,
                "flatten_select_after_filter_and_orderby": True,
            }  # For config that's temporary/to be removed soon
            for key, val in conf.items():
                if self.is_mutable(key):
                    self.set(key, val)

        def get(self, key: str, default=None) -> Any:
            if hasattr(Session, key):
                return getattr(self._session, key)
            if hasattr(self._session._conn._conn, key):
                return getattr(self._session._conn._conn, key)
            return self._conf.get(key, default)

        def is_mutable(self, key: str) -> bool:
            if hasattr(Session, key) and isinstance(getattr(Session, key), property):
                return getattr(Session, key).fset is not None
            if hasattr(Connection, key) and isinstance(
                getattr(Connection, key), property
            ):
                return getattr(Connection, key).fset is not None
            return key in self._conf

        def set(self, key: str, value: Any) -> None:
            if self.is_mutable(key):
                if hasattr(Session, key):
                    setattr(self._session, key, value)
                if hasattr(Connection, key):
                    setattr(self._session._conn._conn, key, value)
                if key in self._conf:
                    self._conf[key] = value
            else:
                raise AttributeError(
                    f'Configuration "{key}" does not exist or is not mutable in runtime'
                )

    class SessionBuilder:
        """
        Provides methods to set connection parameters and create a :class:`Session`.
        """

        def __init__(self) -> None:
            self._options = {}
            self._app_name = None

        def _remove_config(self, key: str) -> "Session.SessionBuilder":
            """Only used in test."""
            self._options.pop(key, None)
            return self

        def app_name(self, app_name: str) -> "Session.SessionBuilder":
            """
            Adds the app name to the :class:`SessionBuilder`
            """
            self._app_name = app_name
            return self

        def config(self, key: str, value: Union[int, str]) -> "Session.SessionBuilder":
            """
            Adds the specified connection parameter to the :class:`SessionBuilder` configuration.
            """
            self._options[key] = value
            return self

        def configs(
            self, options: Dict[str, Union[int, str]]
        ) -> "Session.SessionBuilder":
            """
            Adds the specified :class:`dict` of connection parameters to
            the :class:`SessionBuilder` configuration.

            Note:
                Calling this method overwrites any existing connection parameters
                that you have already set in the SessionBuilder.
            """
            self._options = {**self._options, **options}
            return self

        def create(self) -> "Session":
            """Creates a new Session."""
            session = self._create_internal(self._options.get("connection"))
            return session

        def getOrCreate(self) -> "Session":
            """Gets the last created session or creates a new one if needed."""
            try:
                session = _get_active_session()
                if session._conn._conn.expired:
                    _remove_session(session)
                    return self.create()
                return session
            except ZettaparkClientException as ex:
                if ex.error_code == "1403":  # No session, ok lets create one
                    return self.create()
                raise

        def _create_internal(
            self,
            conn: Optional[Connection] = None,
        ) -> "Session":
            # Set paramstyle to qmark by default to be consistent with previous behavior
            if "paramstyle" not in self._options:
                self._options["paramstyle"] = "qmark"
            new_session = Session(
                ServerConnection({}, conn) if conn else ServerConnection(self._options),
                self._options,
            )

            if "password" in self._options:
                self._options["password"] = None
            _add_session(new_session)
            return new_session

        def __get__(self, obj, objtype=None):
            return Session.SessionBuilder()

    #: Returns a builder you can use to set configuration properties
    #: and create a :class:`Session` object.
    builder: SessionBuilder = SessionBuilder()

    def __init__(
        self,
        conn: ServerConnection,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._conn = conn
        self._import_paths: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self._packages: Dict[str, str] = {}
        self._session_id = self._conn.get_session_id()
        self._session_info = f"""
"version" : {get_version()},
"python.version" : {get_python_version()},
"python.connector.version" : {get_connector_version()},
"python.connector.session.id" : {self._session_id},
"os.name" : {get_os_name()}
"""
        now = datetime.now()
        self._session_volume_directory = (
            f"volume:user://~/zettapark/sessions/"
            f"{now.strftime('%Y%m%d')}/"
            f"{now.strftime('%H')}/"
            f"{self._session_id}/"
        )
        self._volume_created = False
        self._udf_registration = UDFRegistration(self)
        self._udtf_registration = UDTFRegistration(self)
        self._udaf_registration = UDAFRegistration(self)
        self._plan_builder = QueryPlanBuilder(self)
        self._last_action_id = 0
        self._last_canceled_id = 0
        self._file = FileOperation(self)
        self._analyzer = Analyzer(self)
        self._sql_simplifier_enabled: bool = True
        self._cte_optimization_enabled: bool = False
        self._use_logical_type_for_create_df: bool = True
        self._custom_package_usage_config: Dict = {}
        self._conf = self.RuntimeConfig(self, options or {})
        self._tmpdir_handler: Optional[tempfile.TemporaryDirectory] = None
        self._runtime_version_from_requirement: str = None
        self._created_temp_objects: Dict[TempObjectType, Set[str]] = defaultdict(set)

        _logger.info("Zettapark Session information: %s", self._session_info)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __str__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}: "
            f"database={self.get_current_catalog()}, "
            f"schema={self.get_current_schema()}>"
        )

    def _generate_new_action_id(self) -> int:
        self._last_action_id += 1
        return self._last_action_id

    def _register_temp_object(self, t: TempObjectType, name: str) -> None:
        if t not in (
            TempObjectType.TABLE,
            TempObjectType.VIEW,
            TempObjectType.FUNCTION,
        ):
            raise ZettaparkClientException(f"Cannot register temp object: {t}")
        self._created_temp_objects[t].add(name)

    def _drop_temp_objects(self):
        objects = self._created_temp_objects.copy()
        self._created_temp_objects.clear()
        for t, objs in objects.items():
            for obj in objs:
                self._conn.run_query(f"DROP {t.name.upper()} IF EXISTS {obj}")
                _logger.info(f"Dropped temp {t.name.lower()}: {obj}")

    def close(self) -> None:
        """Close this session."""
        try:
            if self._conn.is_closed():
                _logger.debug(
                    "No-op because session %s had been previously closed.",
                    self._session_id,
                )
            else:
                self._file.delete(self._session_volume_directory)
                self._drop_temp_objects()
                _logger.info("Closing session: %s", self._session_id)
                self.cancel_all()
        except Exception as ex:
            raise ZettaparkClientExceptionMessages.SERVER_FAILED_CLOSE_SESSION(
                str(ex)
            ) from ex
        finally:
            try:
                self._conn.close()
                _logger.info("Closed session: %s", self._session_id)
            finally:
                _remove_session(self)

    @property
    def conf(self) -> RuntimeConfig:
        return self._conf

    @property
    def sql_simplifier_enabled(self) -> bool:
        """Set to ``True`` to use the SQL simplifier (defaults to ``True``).
        The generated SQLs from ``DataFrame`` transformations would have fewer layers of nested queries if the SQL simplifier is enabled.
        """
        return self._sql_simplifier_enabled

    @sql_simplifier_enabled.setter
    def sql_simplifier_enabled(self, value: bool) -> None:
        self._conn._telemetry_client.send_sql_simplifier_telemetry(
            self._session_id, value
        )
        self._sql_simplifier_enabled = value

    def cancel_all(self) -> None:
        """
        Cancel all action methods that are running currently.
        This does not affect any action methods called in the future.
        """
        _logger.info("Canceling all running queries")
        self._last_canceled_id = self._last_action_id
        # self._conn.run_query(f"select system$cancel_all_queries({self._session_id})")

    def table(self, name: Union[str, Iterable[str]]) -> Table:
        """
        Returns a Table that points the specified table.

        Args:
            name: A string or list of strings that specify the table name or
                fully-qualified object identifier (database name, schema name, and table name).

            Note:
                If your table name contains special characters, use double quotes to mark it like this, ``session.table('"my table"')``.
                For fully qualified names, you need to use double quotes separately like this, ``session.table('"my db"."my schema"."my.table"')``.
                Refer to `Identifier Requirements <https://doc.clickzetta.com/>`_.

        Examples::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df1.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> session.table("my_table").collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> current_db = session.get_current_catalog()
            >>> current_schema = session.get_current_schema()
            >>> session.table([current_db, current_schema, "my_table"]).collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
        """

        if not isinstance(name, str) and isinstance(name, Iterable):
            name = ".".join(name)
        validate_object_name(name)
        t = Table(name, self)
        # Replace API call origin for table
        set_api_call_source(t, "Session.table")
        return t

    def table_function(
        self,
        func_name: Union[str, List[str], TableFunctionCall],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ) -> DataFrame:
        """Creates a new DataFrame from the given SQL table function.

        References: `SQL functions <https://doc.clickzetta.com/>`_.

        Example 1
            Query a table function by function name:

            >>> from clickzetta.zettapark.functions import lit
            >>> session.table_function("split_to_table", lit("split words to table"), lit(" ")).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]

        Example 2
            Define a table function variable and query it:

            >>> from clickzetta.zettapark.functions import table_function, lit
            >>> split_to_table = table_function("split_to_table")
            >>> session.table_function(split_to_table(lit("split words to table"), lit(" "))).collect()
            [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]

        Example 3
            If you want to call a UDTF right after it's registered, the returned ``UserDefinedTableFunction`` is callable:

            >>> from clickzetta.zettapark.types import IntegerType, StructField, StructType
            >>> from clickzetta.zettapark.functions import udtf, lit
            >>> class GeneratorUDTF:
            ...     def process(self, n):
            ...         for i in range(n):
            ...             yield (i, )
            >>> generator_udtf = udtf(GeneratorUDTF, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
            >>> session.table_function(generator_udtf(lit(3))).collect()
            [Row(NUMBER=0), Row(NUMBER=1), Row(NUMBER=2)]

        Args:
            func_name: The SQL function name.
            func_arguments: The positional arguments for the SQL function.
            func_named_arguments: The named arguments for the SQL function, if it accepts named arguments.

        Returns:
            A new :class:`DataFrame` with data from calling the table function.

        See Also:
            - :meth:`DataFrame.join_table_function`, which lateral joins an existing :class:`DataFrame` and a SQL function.
            - :meth:`Session.generator`, which is used to instantiate a :class:`DataFrame` using Generator table function.
                Generator functions are not supported with :meth:`Session.table_function`.
        """
        func_expr = _create_table_function_expression(
            func_name, *func_arguments, **func_named_arguments
        )

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=SelectTableFunction(func_expr, analyzer=self._analyzer),
                    analyzer=self._analyzer,
                ),
            )
        else:
            d = DataFrame(
                self,
                TableFunctionRelation(func_expr),
            )
        set_api_call_source(d, "Session.table_function")
        return d

    def generator(
        self, *columns: Column, rowcount: int = 0, timelimit: int = 0
    ) -> DataFrame:
        """Creates a new DataFrame using the Generator table function.

        References: `Generator function <https://doc.clickzetta.com/>`_.

        Args:
            columns: List of data generation function that work in tandem with generator table function.
            rowcount: Resulting table with contain ``rowcount`` rows if only this argument is specified. Defaults to 0.
            timelimit: The query runs for ``timelimit`` seconds, generating as many rows as possible within the time frame. The
                exact row count depends on the system speed. Defaults to 0.

        Usage Notes:
                - When both ``rowcount`` and ``timelimit`` are specified, then:

                    + if the ``rowcount`` is reached before the ``timelimit``, the resulting table with contain ``rowcount`` rows.
                    + if the ``timelimit`` is reached before the ``rowcount``, the table will contain as many rows generated within this time.
                - If both ``rowcount`` and ``timelimit`` are not specified, 0 rows will be generated.

        Example 1
            >>> from clickzetta.zettapark.functions import seq1, seq8, uniform
            >>> df = session.generator(seq1(1).as_("sequence one"), uniform(1, 10, 2).as_("uniform"), rowcount=3)
            >>> df.show()
            ------------------------------
            |"sequence one"  |"UNIFORM"  |
            ------------------------------
            |0               |3          |
            |1               |3          |
            |2               |3          |
            ------------------------------
            <BLANKLINE>

        Example 2
            >>> df = session.generator(seq8(0), uniform(1, 10, 2), timelimit=1).order_by(seq8(0)).limit(3)
            >>> df.show()
            -----------------------------------
            |"SEQ8(0)"  |"UNIFORM(1, 10, 2)"  |
            -----------------------------------
            |0          |3                    |
            |1          |3                    |
            |2          |3                    |
            -----------------------------------
            <BLANKLINE>

        Returns:
            A new :class:`DataFrame` with data from calling the generator table function.
        """
        if not columns:
            raise ValueError("Columns cannot be empty for generator table function")
        named_args = {}
        if rowcount != 0:
            named_args["rowcount"] = lit(rowcount)._expression
        if timelimit != 0:
            named_args["timelimit"] = lit(timelimit)._expression

        operators = [self._analyzer.analyze(col._expression, {}) for col in columns]
        func_expr = GeneratorTableFunction(args=named_args, operators=operators)

        if self.sql_simplifier_enabled:
            d = DataFrame(
                self,
                SelectStatement(
                    from_=SelectTableFunction(
                        func_expr=func_expr, analyzer=self._analyzer
                    ),
                    analyzer=self._analyzer,
                ),
            )
        else:
            d = DataFrame(
                self,
                TableFunctionRelation(func_expr),
            )
        set_api_call_source(d, "Session.generator")
        return d

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> DataFrame:
        """
        Returns a new DataFrame representing the results of a SQL query.

        Note:
            You can use this method to execute a SQL query lazily,
            which means the SQL is not executed until methods like :func:`DataFrame.collect`
            or :func:`DataFrame.to_pandas` evaluate the DataFrame.
            For **immediate execution**, chain the call with the collect method: `session.sql(query).collect()`.

        Args:
            query: The SQL statement to execute.
            params: binding parameters. We only support qmark bind variables. For more information, check
                https://doc.clickzetta.com/

        Example::

            >>> # create a dataframe from a SQL query
            >>> df = session.sql("select 1/2")
            >>> # execute the query
            >>> df.collect()
            [Row(1/2=Decimal('0.500000'))]

            >>> # Use params to bind variables
            >>> session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]).sort("column1").collect()
            [Row(COLUMN1=1, COLUMN2='a'), Row(COLUMN1=2, COLUMN2='b')]
        """
        trim = trim_trailing_semicolon_and_whitespace(query)
        if len(trim) != len(query):
            _logger.warning("Removed trailing semicolon and whitespace from query")
            query = trim
        pattern = re.compile(r"\(\s*\?\s*(?:,\s*\?\s*)*\)")
        if "values" in query and len(pattern.findall(query)) >= 1:
            matches = pattern.findall(query)
            if len(matches) >= 1 and not params:
                raise TypeError(f"error sql values: {params}")
            columns = []
            match = matches[0]
            if not match.count("?") - match.count(",") == 1:
                raise TypeError(f"error sql format: {match}")
            count = match.count("?")
            for i in range(count):
                columns.append(Attribute(f"col{i}", infer_type(params[i])))
            d = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=SelectValuesSQL(
                        query, analyzer=self._analyzer, params=params, columns=columns
                    ),
                    analyzer=self._analyzer,
                ),
            )
        else:
            # replace placeholder first.
            if "?" in query and params:
                for param in params:
                    query = query.replace("?", str(param), 1)
                params = None
            if self.sql_simplifier_enabled:
                d = DataFrame(
                    self,
                    self._analyzer.create_select_statement(
                        from_=SelectSQL(query, analyzer=self._analyzer, params=params),
                        analyzer=self._analyzer,
                    ),
                )
            else:
                d = DataFrame(
                    self,
                    self._analyzer.plan_builder.query(
                        query, source_plan=None, params=params
                    ),
                )
        set_api_call_source(d, "Session.sql")
        return d

    @property
    def read(self) -> "DataFrameReader":
        """Returns a :class:`DataFrameReader` that you can use to read data from various
        supported sources (e.g. a file in a volume) as a DataFrame."""
        return DataFrameReader(self)

    @property
    def session_id(self) -> uuid.UUID:
        """Returns a UUID that represents the session ID of this session."""
        return self._session_id

    @property
    def connection(self) -> "Connection":
        """Returns a :class:`ClickzettaConnection` object that allows you to access the connection between the current session
        and ClickZetta server."""
        return self._conn._conn

    def _run_query(
        self,
        query: str,
        is_ddl_on_temp_object: bool = False,
        log_on_exception: bool = True,
    ) -> List[Any]:
        return self._conn.run_query(
            query,
            is_ddl_on_temp_object=is_ddl_on_temp_object,
            log_on_exception=log_on_exception,
        )["data"]

    def _get_result_attributes(self, query: str) -> List[Attribute]:
        return self._conn.get_result_attributes(query)

    def _dump_pandas(self, df: "PandasDF") -> DataFrame:
        subdir = f"df_{datetime.now().strftime('%Y%m%d%H%M%S')}_{generate_random_alphanumeric(16)}"
        volume_path = f"{self._session_volume_directory}df/{subdir}/data.parquet"
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            df.to_parquet(temp_file)
            temp_file.close()
            self.file.put(temp_file.name, volume_path)
        return self.read.parquet(volume_path)

    def create_dataframe(
        self,
        data: Union[List, Tuple, "PandasDF"],
        schema: Optional[Union[StructType, Iterable[str]]] = None,
    ) -> DataFrame:
        """Creates a new DataFrame containing the specified values from the local data.

        If creating a new DataFrame from a pandas Dataframe, we will store the pandas
        DataFrame in a temporary table and return a DataFrame pointing to that temporary
        table for you to then do further transformations on. This temporary table will be
        dropped at the end of your session. If you would like to save the pandas DataFrame,
        use the :meth:`write_pandas` method instead.

        Args:
            data: The local data for building a :class:`DataFrame`. ``data`` can only
                be a :class:`list`, :class:`tuple` or pandas DataFrame. Every element in
                ``data`` will constitute a row in the DataFrame.
            schema: A :class:`~clickzetta.zettapark.types.StructType` containing names and
                data types of columns, or a list of column names, or ``None``.
                When ``schema`` is a list of column names or ``None``, the schema of the
                DataFrame will be inferred from the data across all rows. To improve
                performance, provide a schema. This avoids the need to infer data types
                with large data sets.

        Examples::

            >>> # create a dataframe with a schema
            >>> from clickzetta.zettapark.types import IntegerType, StringType, StructField
            >>> schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
            >>> session.create_dataframe([[1, "alice"], [3, "bob"]], schema).collect()
            [Row(A=1, B='alice'), Row(A=3, B='bob')]

            >>> # create a dataframe by inferring a schema from the data
            >>> from clickzetta.zettapark import Row
            >>> # infer schema
            >>> session.create_dataframe([1, 2, 3, 4], schema=["a"]).collect()
            [Row(A=1), Row(A=2), Row(A=3), Row(A=4)]
            >>> session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"]).collect()
            [Row(A=1, B=2, C=3, D=4)]
            >>> session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).collect()
            [Row(A=1, B=2), Row(A=3, B=4)]
            >>> session.create_dataframe([Row(a=1, b=2, c=3, d=4)]).collect()
            [Row(A=1, B=2, C=3, D=4)]
            >>> session.create_dataframe([{"a": 1}, {"b": 2}]).collect()
            [Row(A=1, B=None), Row(A=None, B=2)]

            >>> # create a dataframe from a pandas Dataframe
            >>> import pandas as pd
            >>> session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"])).collect()
            [Row(a=1, b=2, c=3, d=4)]
        """
        if data is None:
            raise ValueError("data cannot be None.")

        # check the type of data
        if isinstance(data, Row):
            raise TypeError("create_dataframe() function does not accept a Row object.")

        if not isinstance(data, (list, tuple)) and not _is_pandas_df(data):
            raise TypeError(
                "create_dataframe() function only accepts data as a list, tuple or a pandas DataFrame."
            )

        # check to see if it is a pandas DataFrame and if so, write that to a temp
        # table and return as a DataFrame
        if _is_pandas_df(data):
            t = self._dump_pandas(data)
            set_api_call_source(t, "Session.create_dataframe[pandas]")
            return t

        # infer the schema based on the data
        names = None
        schema_query = None
        if isinstance(schema, StructType):
            new_schema = schema
            # SELECT query has an undefined behavior for nullability, so if the schema requires non-nullable column and
            # all columns are primitive type columns, we use a temp table to lock in the nullabilities.
            if any([field.nullable is False for field in schema.fields]) and all(
                [field.datatype.is_primitive() for field in schema.fields]
            ):
                temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
                schema_string = analyzer_utils.attribute_to_schema_string(
                    schema._to_attributes()
                )
                try:
                    self._register_temp_object(
                        TempObjectType.TABLE,
                        self.get_fully_qualified_name_if_possible(temp_table_name),
                    )
                    self._run_query(
                        f"CREATE TABLE {self.get_fully_qualified_name_if_possible(temp_table_name)} ({schema_string})"
                    )
                    schema_query = f"SELECT * FROM {self.get_fully_qualified_name_if_possible(temp_table_name)}"
                except ProgrammingError as e:
                    logging.debug(
                        f"Cannot create temp table for specified non-nullable schema, fall back to using schema "
                        f"string from select query. Exception: {str(e)}"
                    )
        else:
            if not data:
                raise ValueError("Cannot infer schema from empty data")
            if isinstance(schema, Iterable):
                names = list(schema)
            new_schema = reduce(
                merge_type,
                (infer_schema(row, names) for row in data),
            )
        if len(new_schema.fields) == 0:
            raise ValueError(
                "The provided schema or inferred schema cannot be None or empty"
            )

        def convert_row_to_list(
            row: Union[Iterable[Any], Any], names: List[str]
        ) -> List:
            row_dict = None
            if row is None:
                row = [None]
            elif isinstance(row, (tuple, list)):
                if not row:
                    row = [None]
                elif getattr(row, "_fields", None):  # Row or namedtuple
                    row_dict = row.as_dict() if isinstance(row, Row) else row._asdict()
            elif isinstance(row, dict):
                row_dict = row.copy()
            else:
                row = [row]

            if row_dict:
                # fill None if the key doesn't exist
                row_dict = {quote_name(k): v for k, v in row_dict.items()}
                return [row_dict.get(name) for name in names]
            else:
                # check the length of every row, which should be same across data
                if len(row) != len(names):
                    raise ValueError(
                        f"{len(names)} fields are required by schema "
                        f"but {len(row)} values are provided. This might be because "
                        f"data consists of rows with different lengths, or mixed rows "
                        f"with column names or without column names"
                    )
                return list(row)

        # always overwrite the column names if they are provided via schema
        if not names:
            names = [f.name for f in new_schema.fields]
        quoted_names = [quote_name(name) for name in names]
        rows = [convert_row_to_list(row, quoted_names) for row in data]

        # get attributes and data types
        attrs, data_types = [], []
        for field, quoted_name in zip(new_schema.fields, quoted_names):
            data_type = field.datatype
            if not isinstance(data_type, DataType):
                raise TypeError(
                    f"Type of field {field.name} ({data_type}) is not a DataType"
                )
            attrs.append(Attribute(quoted_name, data_type, field.nullable))
            data_types.append(field.datatype)

        converted = []
        for row in rows:
            converted.append(Row(*row))
        project_columns = [column(name) for name in names]

        if self.sql_simplifier_enabled:
            df = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=self._analyzer.create_select_query_plan(
                        Values(attrs, converted, schema_query=schema_query),
                        analyzer=self._analyzer,
                    ),
                    analyzer=self._analyzer,
                ),
            ).select(project_columns)
        else:
            df = DataFrame(
                self, Values(attrs, converted, schema_query=schema_query)
            ).select(project_columns)
        set_api_call_source(df, "Session.create_dataframe[values]")

        return df

    def range(self, start: int, end: Optional[int] = None, step: int = 1) -> DataFrame:
        """
        Creates a new DataFrame from a range of numbers. The resulting DataFrame has
        single column named ``ID``, containing elements in a range from ``start`` to
        ``end`` (exclusive) with the step value ``step``.

        Args:
            start: The start of the range. If ``end`` is not specified,
                ``start`` will be used as the value of ``end``.
            end: The end of the range.
            step: The step of the range.

        Examples::

            >>> session.range(10).collect()
            [Row(ID=0), Row(ID=1), Row(ID=2), Row(ID=3), Row(ID=4), Row(ID=5), Row(ID=6), Row(ID=7), Row(ID=8), Row(ID=9)]
            >>> session.range(1, 10).collect()
            [Row(ID=1), Row(ID=2), Row(ID=3), Row(ID=4), Row(ID=5), Row(ID=6), Row(ID=7), Row(ID=8), Row(ID=9)]
            >>> session.range(1, 10, 2).collect()
            [Row(ID=1), Row(ID=3), Row(ID=5), Row(ID=7), Row(ID=9)]
        """
        range_plan = Range(0, start, step) if end is None else Range(start, end, step)

        if self.sql_simplifier_enabled:
            df = DataFrame(
                self,
                self._analyzer.create_select_statement(
                    from_=self._analyzer.create_select_query_plan(
                        range_plan, analyzer=self._analyzer
                    ),
                    analyzer=self._analyzer,
                ),
            )
        else:
            df = DataFrame(self, range_plan)
        set_api_call_source(df, "Session.range")
        return df

    def get_current_catalog(self) -> Optional[str]:
        return self._conn._get_current_parameter("workspace")

    def get_current_schema(self) -> Optional[str]:
        return self._conn._get_current_parameter("schema")

    def get_fully_qualified_name_if_possible(
        self, name: Union[str, List[str], Tuple[str]]
    ) -> str:
        """
        Returns the fully qualified object name if current database/schema exists, otherwise returns the object name
        """
        if len(name) == 0:
            raise ValueError("Empty name")
        database = self.get_current_catalog()
        schema = self.get_current_schema()
        if isinstance(name, (List, Tuple)):
            database = name[-3] if len(name) >= 3 else database
            schema = name[-2] if len(name) >= 2 else schema
            name = name[-1]
        if database and schema:
            return f"`{database}`.`{schema}`.`{name}`"
        return name

    def use_catalog(self, catalog: str) -> None:
        self._use_object(catalog, "workspace")

    def use_schema(self, schema: str) -> None:
        self._use_object(schema, "schema")

    def _use_object(self, object_name: str, object_type: str) -> None:
        if object_name:
            validate_object_name(object_name)
            self._conn._conn.use_object(object_name, object_type)
        else:
            raise ValueError(f"'{object_type}' must not be empty or None.")

    @property
    def telemetry_enabled(self) -> bool:
        """Controls whether or not the Zettapark client sends usage telemetry to ClickZetta.
        This typically includes information like the API calls invoked, libraries used in conjunction with Zettapark,
        and information that will let us better diagnose and fix client side errors.

        The default value is ``True``.

        Example::

            >>> session.telemetry_enabled
            True
            >>> session.telemetry_enabled = False
            >>> session.telemetry_enabled
            False
            >>> session.telemetry_enabled = True
            >>> session.telemetry_enabled
            True
        """
        return self._conn._conn.telemetry_enabled

    @telemetry_enabled.setter
    def telemetry_enabled(self, value):
        # Set both in-band and out-of-band telemetry to True/False
        if value:
            self._conn._conn.telemetry_enabled = True
            self._conn._telemetry_client.telemetry._enabled = True
        else:
            self._conn._conn.telemetry_enabled = False
            self._conn._telemetry_client.telemetry._enabled = False

    @property
    def file(self) -> FileOperation:
        """
        Returns a :class:`FileOperation` object that you can use to perform file operations on volumes.
        See details of how to use this object in :class:`FileOperation`.
        """
        return self._file

    @property
    def udf(self) -> UDFRegistration:
        """
        Returns a :class:`udf.UDFRegistration` object that you can use to register UDFs.
        See details of how to use this object in :class:`udf.UDFRegistration`.
        """
        return self._udf_registration

    @property
    def udtf(self) -> UDTFRegistration:
        """
        Returns a :class:`udtf.UDTFRegistration` object that you can use to register UDTFs.
        See details of how to use this object in :class:`udtf.UDTFRegistration`.
        """
        return self._udtf_registration

    @property
    def udaf(self) -> UDAFRegistration:
        """
        Returns a :class:`udaf.UDAFRegistration` object that you can use to register UDAFs.
        See details of how to use this object in :class:`udaf.UDAFRegistration`.
        """
        return self._udaf_registration

    def query_history(self) -> QueryHistory:
        """Create an instance of :class:`QueryHistory` as a context manager to record queries that are pushed down to the ClickZetta database.

        >>> with session.query_history() as query_history:
        ...     df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        ...     df = df.filter(df.a == 1)
        ...     res = df.collect()
        >>> assert len(query_history.queries) == 1
        """
        query_listener = QueryHistory(self)
        self._conn.add_query_listener(query_listener)
        return query_listener

    def _table_exists(self, raw_table_name: Iterable[str]):
        if len(raw_table_name) > 3:
            raise ZettaparkClientExceptionMessages.GENERAL_INVALID_OBJECT_NAME(
                ".".join(raw_table_name)
            )
        sql = (
            "SELECT `table_name` FROM ( SHOW TABLES "
            + (
                (" IN " + ".".join(f"`{i}`" for i in raw_table_name[:-1]))
                if len(raw_table_name) > 1
                else ""
            )
            + f" ) WHERE `table_name` = '{raw_table_name[-1]}'"
        )
        tables = self._run_query(sql)
        return tables is not None and len(tables) > 0

    def _explain_query(self, query: str) -> Optional[str]:
        try:
            return self._run_query(f"explain {query}")[0][1]
        # return None for queries which can't be explained
        except ProgrammingError:
            _logger.warning("query `%s` cannot be explained", query)
            return None

    createDataFrame = create_dataframe
