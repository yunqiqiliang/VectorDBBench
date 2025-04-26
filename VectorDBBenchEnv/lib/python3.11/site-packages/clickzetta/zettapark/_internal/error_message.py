#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

from typing import Optional

from clickzetta.zettapark._connector import OperationalError, ProgrammingError
from clickzetta.zettapark.exceptions import (
    ZettaparkColumnException,
    ZettaparkCreateDynamicTableException,
    ZettaparkCreateViewException,
    ZettaparkDataframeException,
    ZettaparkDataframeReaderException,
    ZettaparkFetchDataException,
    ZettaparkInvalidObjectNameException,
    ZettaparkJoinException,
    ZettaparkMissingDbOrSchemaException,
    ZettaparkPandasException,
    ZettaparkPlanException,
    ZettaparkQueryCancelledException,
    ZettaparkSessionException,
    ZettaparkSQLAmbiguousJoinException,
    ZettaparkSQLException,
    ZettaparkSQLInvalidIdException,
    ZettaparkSQLUnexpectedAliasException,
    ZettaparkTableException,
    ZettaparkUploadFileException,
    ZettaparkUploadUdfFileException,
    _ZettaparkInternalException,
)


class ZettaparkClientExceptionMessages:
    """Holds all of the error messages that could be used in the ZettaparkClientException Class.

    IMPORTANT: keep this file in numerical order of the error-code."""

    # Internal Error messages 001X

    @staticmethod
    def INTERNAL_TEST_MESSAGE(message: str) -> _ZettaparkInternalException:
        return _ZettaparkInternalException(
            f"internal test message: {message}.", error_code="1010"
        )

    # DataFrame Error Messages 01XX

    @staticmethod
    def DF_CANNOT_DROP_COLUMN_NAME(col_name: str) -> ZettaparkColumnException:
        return ZettaparkColumnException(
            f"Unable to drop the column {col_name}. You must specify the column by name "
            f'(e.g. df.drop(col("a"))).',
            error_code="1100",
        )

    @staticmethod
    def DF_CANNOT_DROP_ALL_COLUMNS() -> ZettaparkColumnException:
        return ZettaparkColumnException("Cannot drop all columns", error_code="1101")

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
        col_name: str, all_columns: str
    ) -> ZettaparkColumnException:
        return ZettaparkColumnException(
            f'Cannot combine the DataFrames by column names. The column "{col_name}" is '
            f"not a column in the other DataFrame ({all_columns}).",
            error_code="1102",
        )

    @staticmethod
    def DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST(
        old_name: str, new_name: str, times: int
    ) -> ZettaparkColumnException:
        return ZettaparkColumnException(
            f"Unable to rename the column {old_name} as {new_name} because this DataFrame has {times} columns named {old_name}."
        )

    @staticmethod
    def DF_SELF_JOIN_NOT_SUPPORTED() -> ZettaparkJoinException:
        return ZettaparkJoinException(
            "You cannot join a DataFrame with itself because the column references cannot "
            "be resolved correctly. Instead, create a copy of the DataFrame with copy.copy(), "
            "and join the DataFrame with this copy.",
            error_code="1103",
        )

    @staticmethod
    def DF_FLATTEN_UNSUPPORTED_INPUT_MODE(mode: str) -> ZettaparkDataframeException:
        return ZettaparkDataframeException(
            f"Unsupported input mode {mode}. For the mode parameter in flatten(), you must "
            f"specify OBJECT, ARRAY, or BOTH.",
            error_code="1104",
        )

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME(col_name: str) -> ZettaparkColumnException:
        return ZettaparkColumnException(
            f"The DataFrame does not contain the column named '{col_name}'.",
            error_code="1105",
        )

    @staticmethod
    def DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE() -> ZettaparkDataframeReaderException:
        return ZettaparkDataframeReaderException(
            "You must call DataFrameReader.schema() and specify the schema for the file.",
            error_code="1106",
        )

    @staticmethod
    def DF_COPY_INTO_CANNOT_CREATE_TABLE(
        table_name: str,
    ) -> ZettaparkDataframeReaderException:
        return ZettaparkDataframeReaderException(
            f"Cannot create the target table {table_name} because Zettapark cannot determine the column names to use. You should create the table before calling copy_into_table()."
        )

    @staticmethod
    def DF_CROSS_TAB_COUNT_TOO_LARGE(
        count: int, max_count: int
    ) -> ZettaparkDataframeException:
        return ZettaparkDataframeException(
            f"The number of distinct values in the second input column ({count}) exceeds "
            f"the maximum number of distinct values allowed ({max_count}).",
            error_code="1107",
        )

    @staticmethod
    def DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
        count: int, columns: str
    ) -> ZettaparkDataframeException:
        return ZettaparkDataframeException(
            f"The DataFrame passed in to this function must have only one output column. "
            f"This DataFrame has {count} output columns: {columns}",
            error_code="1108",
        )

    @staticmethod
    def DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR() -> ZettaparkDataframeException:
        return ZettaparkDataframeException(
            "You can apply only one aggregate expression to a RelationalGroupedDataFrame "
            "returned by the pivot() method.",
            error_code="1109",
        )

    @staticmethod
    def DF_JOIN_INVALID_JOIN_TYPE(type1: str, types: str) -> ZettaparkJoinException:
        return ZettaparkJoinException(
            f"Unsupported join type '{type1}'. Supported join types include: {types}.",
            error_code="1110",
        )

    @staticmethod
    def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe: str) -> ZettaparkJoinException:
        return ZettaparkJoinException(
            f"Unsupported natural join type '{tpe}'.", error_code="1111"
        )

    @staticmethod
    def DF_JOIN_INVALID_USING_JOIN_TYPE(tpe: str) -> ZettaparkJoinException:
        return ZettaparkJoinException(
            f"Unsupported using join type '{tpe}'.", error_code="1112"
        )

    @staticmethod
    def DF_PANDAS_GENERAL_EXCEPTION(msg: str) -> ZettaparkPandasException:
        return ZettaparkPandasException(
            f"Unable to write pandas dataframe to ClickZetta. COPY INTO command output {msg}",
            error_code="1113",
        )

    @staticmethod
    def DF_PANDAS_TABLE_DOES_NOT_EXIST_EXCEPTION(
        location: str,
    ) -> ZettaparkPandasException:
        return ZettaparkPandasException(
            f"Cannot write pandas DataFrame to table {location} "
            f"because it does not exist. Create table before "
            f"trying to write a pandas DataFrame",
            error_code="1114",
        )

    @staticmethod
    def MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
        action: str, clause: str
    ) -> ZettaparkTableException:
        return ZettaparkTableException(
            f"{action} has been specified for {clause} to merge table",
            error_code="1115",
        )

    # Plan Analysis error codes 02XX

    @staticmethod
    def PLAN_ANALYZER_INVALID_IDENTIFIER(name: str) -> ZettaparkPlanException:
        return ZettaparkPlanException(f"Invalid identifier {name}", error_code="1200")

    @staticmethod
    def PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(
        type_name: str,
    ) -> ZettaparkPlanException:
        return ZettaparkPlanException(
            f"Internal Error: Only PersistedView and LocalTempView are supported. "
            f"view type: {type_name}",
            error_code="1201",
        )

    @staticmethod
    def PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(value: str) -> ZettaparkPlanException:
        return ZettaparkPlanException(
            f"The COPY option 'FORCE = {value}' is not supported by the Zettapark library. "
            f"The Clickzetta library loads all files, even if the files have been loaded "
            f"previously and have not changed since they were loaded.",
            error_code="1202",
        )

    @staticmethod
    def PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS() -> ZettaparkCreateViewException:
        return ZettaparkCreateViewException(
            "Your dataframe may include DDL or DML operations. Creating a view from "
            "this DataFrame is currently not supported.",
            error_code="1203",
        )

    @staticmethod
    def PLAN_CREATE_VIEWS_FROM_SELECT_ONLY() -> ZettaparkCreateViewException:
        return ZettaparkCreateViewException(
            "Creating views from SELECT queries supported only.", error_code="1204"
        )

    @staticmethod
    def PLAN_INVALID_TYPE(type: str) -> ZettaparkPlanException:
        return ZettaparkPlanException(
            f"Invalid type, analyze. {type}", error_code="1205"
        )

    @staticmethod
    def PLAN_CANNOT_CREATE_LITERAL(type: str) -> ZettaparkPlanException:
        return ZettaparkPlanException(
            f"Cannot create a Literal for {type}", error_code="1206"
        )

    @staticmethod
    def PLAN_CREATE_DYNAMIC_TABLE_FROM_DDL_DML_OPERATIONS() -> (
        ZettaparkCreateDynamicTableException
    ):
        return ZettaparkCreateDynamicTableException(
            "Your dataframe may include DDL or DML operations. Creating a dynamic table from "
            "this DataFrame is currently not supported.",
            error_code="1207",
        )

    @staticmethod
    def DF_ALIAS_NOT_RECOGNIZED(alias: str) -> ZettaparkDataframeException:
        return ZettaparkDataframeException(
            f"DataFrame alias unrecognized. A subset of columns corresponding to Dataframe alias '{alias}' can not be found. ",
            error_code="1208",
        )

    @staticmethod
    def PLAN_CREATE_DYNAMIC_TABLE_FROM_SELECT_ONLY() -> (
        ZettaparkCreateDynamicTableException
    ):
        return ZettaparkCreateDynamicTableException(
            "Creating dynamic tables from SELECT queries supported only.",
            error_code="1208",
        )

    # SQL Execution error codes 03XX

    @staticmethod
    def SQL_LAST_QUERY_RETURN_RESULTSET() -> ZettaparkSQLException:
        return ZettaparkSQLException(
            "Internal error: The execution for the last query "
            "in the Clickzetta plan doesn't return a ResultSet.",
            error_code="1300",
        )

    @staticmethod
    def SQL_PYTHON_REPORT_UNEXPECTED_ALIAS(
        query: Optional[str] = None,
    ) -> ZettaparkSQLUnexpectedAliasException:
        return ZettaparkSQLUnexpectedAliasException(
            "You can only define aliases for the root Columns in a DataFrame returned by "
            "select() and agg(). You cannot use aliases for Columns in expressions.",
            error_code="1301",
            query=query,
        )

    @staticmethod
    def SQL_PYTHON_REPORT_INVALID_ID(
        name: str, query: Optional[str] = None
    ) -> ZettaparkSQLInvalidIdException:
        return ZettaparkSQLInvalidIdException(
            f'The column specified in df("{name}") '
            f"is not present in the output of the DataFrame.",
            error_code="1302",
            query=query,
        )

    @staticmethod
    def SQL_PYTHON_REPORT_JOIN_AMBIGUOUS(
        c1: str,
        c2: str,
        query: Optional[str] = None,
    ) -> ZettaparkSQLAmbiguousJoinException:
        return ZettaparkSQLAmbiguousJoinException(
            f"The reference to the column '{c1}' is ambiguous. The column is "
            f"present in both DataFrames used in the join. To identify the "
            f"DataFrame that you want to use in the reference, use the syntax "
            f'<df>["{c2}"] in join conditions and in select() calls on the '
            f"result of the join. Alternatively, you can rename the column in "
            f"either DataFrame for disambiguation. See the API documentation of "
            f"the DataFrame.join() method for more details.",
            error_code="1303",
            query=query,
        )

    @staticmethod
    def SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
        pe: ProgrammingError,
    ) -> ZettaparkSQLException:
        return ZettaparkSQLException(pe.msg, error_code="1304", conn_error=pe)

    @staticmethod
    def SQL_EXCEPTION_FROM_OPERATIONAL_ERROR(
        oe: OperationalError,
    ) -> ZettaparkSQLException:
        return ZettaparkSQLException(oe.msg, error_code="1305", conn_error=oe)

    # Server Error Messages 04XX

    @staticmethod
    def SERVER_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
        v1: str, v2: str, v3: str
    ) -> ZettaparkMissingDbOrSchemaException:
        return ZettaparkMissingDbOrSchemaException(
            f"The {v1} is not set for the current session. To set this, either run "
            f'session.sql("USE {v2}").collect() or set the {v3} connection property in '
            f"the dict or properties file that you specify when creating a session.",
            error_code="1400",
        )

    @staticmethod
    def SERVER_QUERY_IS_CANCELLED() -> ZettaparkQueryCancelledException:
        return ZettaparkQueryCancelledException(
            "The query has been cancelled by the user.", error_code="1401"
        )

    @staticmethod
    def SERVER_SESSION_EXPIRED(error_message: str) -> ZettaparkSessionException:
        return ZettaparkSessionException(
            f"Your Zettapark session has expired. You must recreate your "
            f"session.\n{error_message}",
            error_code="1402",
        )

    @staticmethod
    def SERVER_NO_DEFAULT_SESSION() -> ZettaparkSessionException:
        return ZettaparkSessionException(
            "No default Session is found. "
            "Please create a session before you call function 'udf' or use decorator '@udf'.",
            error_code="1403",
        )

    @staticmethod
    def SERVER_SESSION_HAS_BEEN_CLOSED() -> ZettaparkSessionException:
        return ZettaparkSessionException(
            "Cannot perform this operation because the session has been closed.",
            error_code="1404",
        )

    @staticmethod
    def SERVER_FAILED_CLOSE_SESSION(message: str) -> ZettaparkSessionException:
        return ZettaparkSessionException(
            f"Failed to close this session. The error is: {message}", error_code="1405"
        )

    @staticmethod
    def SERVER_FAILED_FETCH_PANDAS(message: str) -> ZettaparkFetchDataException:
        return ZettaparkFetchDataException(
            f"Failed to fetch a pandas Dataframe. The error is: {message}",
            error_code="1406",
        )

    @staticmethod
    def SERVER_UDF_UPLOAD_FILE_STREAM_CLOSED(
        dest_filename: str,
    ) -> ZettaparkUploadUdfFileException:
        return ZettaparkUploadUdfFileException(
            "A file stream was closed when uploading UDF files. "
            f"The destination file name is: {dest_filename}. "
            "If you were creating a UDF, this is probably caused "
            "by an oversized generated UDF file. Please don't use "
            "global variables that reference to large data (e.g., "
            "a ML model with hundreds of parameters) in a UDF, and "
            "consider uploading the large data to a volume, then the "
            "UDF can be read it from the volume while also retain a "
            "small size.",
            error_code="1407",
        )

    @staticmethod
    def SERVER_UPLOAD_FILE_STREAM_CLOSED(
        dest_filename: str,
    ):
        return ZettaparkUploadFileException(
            "A file stream was closed when uploading files to the server."
            f"The destination file name is: {dest_filename}. ",
            error_code="1408",
        )

    @staticmethod
    def MORE_THAN_ONE_ACTIVE_SESSIONS() -> ZettaparkSessionException:
        return ZettaparkSessionException(
            "More than one active session is detected. "
            "When you call function 'udf' or use decorator '@udf', "
            "you must specify the 'session' parameter if you created multiple sessions."
            "Alternatively, you can use 'session.udf.register' to register UDFs",
            error_code="1409",
        )

    # General Error codes 15XX

    @staticmethod
    def GENERAL_INVALID_OBJECT_NAME(
        type_name: str,
    ) -> ZettaparkInvalidObjectNameException:
        return ZettaparkInvalidObjectNameException(
            f"The object name '{type_name}' is invalid.", error_code="1500"
        )
