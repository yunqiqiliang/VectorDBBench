#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

"""This package contains all Zettapark client-side exceptions."""

import logging
from typing import Optional

from clickzetta.zettapark._connector import ConnectorDatabaseError

_logger = logging.getLogger(__name__)


class ZettaparkClientException(Exception):
    """Base Zettapark exception class"""

    def __init__(
        self,
        message: str,
        *,
        error_code: Optional[str] = None,
    ) -> None:
        self.message: str = message
        self.error_code: Optional[str] = error_code
        self.telemetry_message: str = message

        self._pretty_msg = (
            f"({self.error_code}): {self.message}" if self.error_code else self.message
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message!r}, {self.error_code!r})"

    def __str__(self):
        return self._pretty_msg


class _ZettaparkInternalException(ZettaparkClientException):
    """Exception for internal errors. For internal use only.

    Includes all error codes in 10XX (where XX is 0-9).
    """

    pass


class ZettaparkDataframeException(ZettaparkClientException):
    """Exception for dataframe related errors.

    Includes all error codes in range 11XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1104, 1107, 1108, 1109.
    """

    pass


class ZettaparkPlanException(ZettaparkClientException):
    """Exception for plan analysis errors.

    Includes all error codes in range 12XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1200, 1201, 1202, 1205.
    """

    pass


class ZettaparkSQLException(ZettaparkClientException):
    """Exception for errors related to the executed SQL statement that was generated
    from the ClickZetta plan.

    Includes all error codes in range 13XX (where XX is 0-9).

    This exception is specifically raised for error codes: 1300, 1304.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: Optional[str] = None,
        conn_error: Optional[ConnectorDatabaseError] = None,
        job_id: Optional[str] = None,
        query: Optional[str] = None,
        sql_error_code: Optional[int] = None,
        raw_message: Optional[str] = None,
    ) -> None:
        super().__init__(message, error_code=error_code)

        self.conn_error = conn_error
        self.job_id = job_id or getattr(self.conn_error, "job_id", None)
        self.query = query or getattr(self.conn_error, "query", None)
        self.sql_error_code = sql_error_code or getattr(self.conn_error, "errno", None)
        self.raw_message = raw_message or getattr(self.conn_error, "raw_msg", None)

        pretty_error_code = f"({self.error_code}): " if self.error_code else ""
        pretty_job_id = f"{self.job_id}: " if self.job_id else ""
        self._pretty_msg = f"{pretty_error_code}{pretty_job_id}{self.message}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message!r}, {self.error_code!r}, {self.job_id!r})"


class ZettaparkServerException(ZettaparkClientException):
    """Exception for miscellaneous related errors.

    Includes all error codes in range 14XX (where XX is 0-9).
    """

    pass


class ZettaparkGeneralException(ZettaparkClientException):
    """Exception for general exceptions.

    Includes all error codes in range 15XX (where XX is 0-9).
    """

    pass


class ZettaparkColumnException(ZettaparkDataframeException):
    """Exception for column related errors during dataframe operations.

    Includes error codes: 1100, 1101, 1102, 1105.
    """

    pass


class ZettaparkJoinException(ZettaparkDataframeException):
    """Exception for join related errors during dataframe operations.

    Includes error codes: 1103, 1110, 1111, 1112.
    """

    pass


class ZettaparkDataframeReaderException(ZettaparkDataframeException):
    """Exception for dataframe reader errors.

    Includes error codes: 1106.
    """

    pass


class ZettaparkPandasException(ZettaparkDataframeException):
    """Exception for pandas related errors.

    Includes error codes: 1106.
    """

    pass


class ZettaparkTableException(ZettaparkDataframeException):
    """Exception for table related errors.

    Includes error codes: 1115.
    """

    pass


class ZettaparkCreateViewException(ZettaparkPlanException):
    """Exception for errors while trying to create a view.

    Includes error codes: 1203, 1204, 1205, 1206.
    """

    pass


class ZettaparkCreateDynamicTableException(ZettaparkPlanException):
    """Exception for errors while trying to create a dynamic table.

    Includes error codes: 1207, 1208.
    """

    pass


class ZettaparkSQLAmbiguousJoinException(ZettaparkSQLException):
    """Exception for ambiguous joins that are created from the
    translated SQL statement.

    Includes error codes: 1303.
    """

    pass


class ZettaparkSQLInvalidIdException(ZettaparkSQLException):
    """Exception for having an invalid ID (usually a missing ID)
    that are created from the translated SQL statement.

    Includes error codes: 1302.
    """

    pass


class ZettaparkSQLUnexpectedAliasException(ZettaparkSQLException):
    """Exception for having an unexpected alias that are created
    from the translated SQL statement.

    Includes error codes: 1301.
    """

    pass


class ZettaparkSessionException(ZettaparkServerException):
    """Exception for any session related errors.

    Includes error codes: 1402, 1403, 1404, 1405.
    """

    pass


class ZettaparkMissingDbOrSchemaException(ZettaparkServerException):
    """Exception for when a schema or database is missing in the session connection.
    These are needed to run queries.

    Includes error codes: 1400.
    """

    pass


class ZettaparkQueryCancelledException(ZettaparkServerException):
    """Exception for when we are trying to interact with a cancelled query.

    Includes error codes: 1401.
    """

    pass


class ZettaparkFetchDataException(ZettaparkServerException):
    """Exception for when we are trying to fetch data from ClickZetta.

    Includes error codes: 1406.
    """

    pass


class ZettaparkUploadFileException(ZettaparkServerException):
    """Exception for when we are trying to upload files to the server.

    Includes error codes: 1408.
    """

    pass


class ZettaparkUploadUdfFileException(ZettaparkUploadFileException):
    """Exception for when we are trying to upload UDF files to the server.

    Includes error codes: 1407.
    """

    pass


class ZettaparkInvalidObjectNameException(ZettaparkGeneralException):
    """Exception for inputting an invalid object name. Checked locally.

    This exception is specifically raised for error codes: 1500.
    """

    pass


class ZettaparkInvalidVolumePathException(ZettaparkGeneralException):
    """Exception for invalid volume path.

    This exception is specifically raised for error codes: 1501.
    """

    pass
