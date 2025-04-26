from typing import Optional, List, Any, Type, Dict, Callable, Literal
from datetime import date, datetime

from sqlalchemy.engine import Dialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.dialects.mysql.types import TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, FLOAT, CHAR, VARCHAR, \
    DATETIME
from sqlalchemy.dialects.mysql.json import JSON


class BITMAP(sqltypes.Numeric):
    __visit_name__ = "BITMAP"


class ARRAY(TypeEngine):
    __visit_name__ = "ARRAY"

    @property
    def python_type(self) -> Optional[Type[List[Any]]]:
        return list


class MAP(TypeEngine):
    __visit_name__ = "MAP"

    @property
    def python_type(self) -> Optional[Type[Dict[Any, Any]]]:
        return dict


class STRUCT(TypeEngine):
    __visit_name__ = "STRUCT"

    @property
    def python_type(self) -> Optional[Type[Any]]:
        return dict


class TIMESTAMP_LTZ(sqltypes.TIMESTAMP):
    """The SQL TIMESTAMP_LTZ type.

    :class:`datatype.TIMESTAMP_LTZ` datatypes have support for timezone
    storage on some backends, such as PostgreSQL and Oracle.  Use the
    :paramref:`~types.TIMESTAMP.timezone` argument in order to enable
    "TIMESTAMP WITH TIMEZONE" for these backends.

    """

    __visit_name__ = "TIMESTAMP_LTZ"

    def __init__(self, timezone=True):
        """Construct a new :class:`datatype.TIMESTAMP_LTZ`.

        :param timezone: boolean.  Indicates that the TIMESTAMP_LTZ type should
         enable timezone support, if available on the target database.
         On a per-dialect basis is similar to "TIMESTAMP WITH TIMEZONE".
         If the target database does not support timezones, this flag is
         ignored.


        """
        super().__init__(timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.TIMESTAMP


class TIMESTAMP_NTZ(sqltypes.TIMESTAMP):
    """The SQL TIMESTAMP_NTZ type.

    :class:`datatype.TIMESTAMP_NTZ` datatypes have support for timezone
    storage on some backends, such as PostgreSQL and Oracle.  Use the
    :paramref:`~types.TIMESTAMP.timezone` argument in order to enable
    "TIMESTAMP WITH TIMEZONE" for these backends.

    """

    __visit_name__ = "TIMESTAMP_NTZ"

    def __init__(self, timezone=True):
        """Construct a new :class:`datatype.TIMESTAMP_NTZ`.

        :param timezone: boolean.  Indicates that the TIMESTAMP_NTZ type should
         enable timezone support, if available on the target database.
         On a per-dialect basis is similar to "TIMESTAMP WITHOUT TIMEZONE".
         If the target database does not support timezones, this flag is
         ignored.


        """
        super().__init__(timezone)

    def get_dbapi_type(self, dbapi):
        return dbapi.TIMESTAMP


class VECTOR(sqltypes.String):
    __visit_name__ = "VECTOR"

    @property
    def python_type(self) -> Optional[Type[str]]:
        return str
