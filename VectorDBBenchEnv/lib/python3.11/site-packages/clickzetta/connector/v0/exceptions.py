"""Exceptions used in the ClickZetta DB-API."""

class CZException(Exception):
    """Base exception for ClickZetta DB-API."""

class Warning(Exception):
    """Exception raised for important DB-API warnings."""


class Error(Exception):
    """Exception representing all non-warning DB-API errors."""


class InterfaceError(Error):
    """DB-API error related to the database interface."""


class DatabaseError(Error):
    """DB-API error related to the database."""


class DataError(DatabaseError):
    """DB-API error due to problems with the processed data."""


class OperationalError(DatabaseError):
    """DB-API error related to the database operation.

    These errors are not necessarily under the control of the programmer.
    """


class IntegrityError(DatabaseError):
    """DB-API error when integrity of the database is affected."""


class InternalError(DatabaseError):
    """DB-API error when the database encounters an internal error."""


class ProgrammingError(DatabaseError):
    """DB-API exception raised for programming errors."""


class NotSupportedError(DatabaseError):
    """DB-API error for operations not supported by the database or API."""
