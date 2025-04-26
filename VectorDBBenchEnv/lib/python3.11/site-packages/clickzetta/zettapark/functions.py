#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


"""
Provides utility and SQL functions that generate :class:`~clickzetta.zettapark.Column` expressions that you can pass to :class:`~clickzetta.zettapark.DataFrame` transformation methods.

These utility functions generate references to columns, literals, and SQL expressions (e.g. "c + 1").

  - Use :func:`col()` to convert a column name to a :class:`Column` object. Refer to the API docs of :class:`Column` to know more ways of referencing a column.
  - Use :func:`lit()` to convert a Python value to a :class:`Column` object that represents a constant value in ClickZetta SQL.
  - Use :func:`sql_expr()` to convert a ClickZetta SQL expression to a :class:`Column`.

    >>> df = session.create_dataframe([[1, 'a', True, '2022-03-16'], [3, 'b', False, '2023-04-17']], schema=["a", "b", "c", "d"])
    >>> res1 = df.filter(col("a") == 1).collect()
    >>> res2 = df.filter(lit(1) == col("a")).collect()
    >>> res3 = df.filter(sql_expr("a = 1")).collect()
    >>> assert res1 == res2 == res3
    >>> res1
    [Row(A=1, B='a', C=True, D='2022-03-16')]

Some :class:`DataFrame` methods accept column names or SQL expressions text aside from a Column object for convenience.
For instance:

    >>> df.filter("a = 1").collect()  # use the SQL expression directly in filter
    [Row(A=1, B='a', C=True, D='2022-03-16')]
    >>> df.select("a").collect()
    [Row(A=1), Row(A=3)]

whereas :class:`Column` objects enable you to use chained column operators and transformations with
Python code fluently:

    >>> # Use columns and literals in expressions.
    >>> df.select(((col("a") + 1).cast("string")).alias("add_one")).show()
    -------------
    |"ADD_ONE"  |
    -------------
    |2          |
    |4          |
    -------------
    <BLANKLINE>

The ClickZetta database has hundreds of `SQL functions <https://doc.clickzetta.com/>`_
This module provides Python functions that correspond to the ClickZetta SQL functions. They typically accept :class:`Column`
objects or column names as input parameters and return a new :class:`Column` objects.
The following examples demonstrate the use of some of these functions:

    >>> # This example calls the function that corresponds to the TO_DATE() SQL function.
    >>> df.select(dateadd('day', lit(1), to_date(col("d")))).show()
    ---------------------------------------
    |"DATEADD('DAY', 1, TO_DATE(""D""))"  |
    ---------------------------------------
    |2022-03-17                           |
    |2023-04-18                           |
    ---------------------------------------
    <BLANKLINE>

If you want to use a SQL function in ClickZetta but can't find the corresponding Python function here,
you can create your own Python function with :func:`function`:

    >>> my_radians = function("radians")  # "radians" is the SQL function name.
    >>> df.select(my_radians(col("a")).alias("my_radians")).show()
    ------------------------
    |"MY_RADIANS"          |
    ------------------------
    |0.017453292519943295  |
    |0.05235987755982988   |
    ------------------------
    <BLANKLINE>

or call the SQL function directly:

    >>> df.select(call_function("radians", col("a")).as_("call_function_radians")).show()
    ---------------------------
    |"CALL_FUNCTION_RADIANS"  |
    ---------------------------
    |0.017453292519943295     |
    |0.05235987755982988      |
    ---------------------------
    <BLANKLINE>

Similarly, to call a table function, you can use :func:`~clickzetta.zettapark.functions.table_function`, or :func:`~clickzetta.zettapark.functions.call_table_function`.

**How to find help on input parameters of the Python functions for SQL functions**
The Python functions have the same name as the corresponding `SQL functions <https://doc.clickzetta.com/>`_.

By reading the API docs or the source code of a Python function defined in this module, you'll see the type hints of the input parameters and return type.
The return type is always ``Column``. The input types tell you the acceptable values:

  - ``ColumnOrName`` accepts a :class:`Column` object, or a column name in str. Most functions accept this type.
    If you still want to pass a literal to it, use `lit(value)`, which returns a ``Column`` object that represents a literal value.

    >>> df.select(avg("a")).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>
    >>> df.select(avg(col("a"))).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>

  - ``LiteralType`` accepts a value of type ``bool``, ``int``, ``float``, ``str``, ``bytearray``, ``decimal.Decimal``,
    ``datetime.date``, ``datetime.datetime``, ``datetime.time``, or ``bytes``. An example is the third parameter of :func:`lead`.

    >>> import datetime
    >>> from clickzetta.zettapark.window import Window
    >>> df.select(col("d"), lead("d", 1, datetime.date(2024, 5, 18), False).over(Window.order_by("d")).alias("lead_day")).show()
    ---------------------------
    |"D"         |"LEAD_DAY"  |
    ---------------------------
    |2022-03-16  |2023-04-17  |
    |2023-04-17  |2024-05-18  |
    ---------------------------
    <BLANKLINE>

  - ``ColumnOrLiteral`` accepts a ``Column`` object, or a value of ``LiteralType`` mentioned above.
    The difference from ``ColumnOrLiteral`` is ``ColumnOrLiteral`` regards a str value as a SQL string value instead of
    a column name. When a function is much more likely to accept a SQL constant value than a column expression, ``ColumnOrLiteral``
    is used. Yet you can still pass in a ``Column`` object if you need to. An example is the second parameter of
    :func:``when``.

    >>> df.select(when(df["a"] > 2, "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>

  - ``int``, ``bool``, ``str``, or another specific type accepts a value of that type. An example is :func:`to_decimal`.

    >>> df.with_column("e", lit("1.2")).select(to_decimal("e", 5, 2)).show()
    -----------------------------
    |"TO_DECIMAL(""E"", 5, 2)"  |
    -----------------------------
    |1.20                       |
    |1.20                       |
    -----------------------------
    <BLANKLINE>

  - ``ColumnOrSqlExpr`` accepts a ``Column`` object, or a SQL expression. For instance, the first parameter in :func:``when``.

    >>> df.select(when("a > 2", "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>
"""

import sys
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple, Union, overload

import clickzetta.zettapark
import clickzetta.zettapark.table_function
from clickzetta.zettapark._internal.analyzer.expression import (
    CaseWhen,
    FunctionExpression,
    ListAgg,
    Literal,
    MultipleExpression,
    Star,
)
from clickzetta.zettapark._internal.analyzer.unary_expression import Alias
from clickzetta.zettapark._internal.analyzer.window_expression import (
    FirstValue,
    Lag,
    LastValue,
    Lead,
)
from clickzetta.zettapark._internal.type_utils import (
    ColumnOrLiteral,
    ColumnOrLiteralStr,
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
    convert_data_type_to_name,
)
from clickzetta.zettapark._internal.udf.pandas_udf import pandas_udf  # noqa: F401
from clickzetta.zettapark._internal.udf.udaf import udaf  # noqa: F401
from clickzetta.zettapark._internal.udf.udf import udf  # noqa: F401
from clickzetta.zettapark._internal.udf.udtf import udtf  # noqa: F401
from clickzetta.zettapark._internal.utils import (
    parse_positional_args_to_list,
    validate_object_name,
)
from clickzetta.zettapark.column import (
    CaseExpr,
    Column,
    _to_col_if_lit,
    _to_col_if_sql_expr,
    _to_col_if_str,
    _to_col_if_str_or_int,
)
from clickzetta.zettapark.types import (
    ArrayType,
    DataType,
    FloatType,
    StringType,
    StructType,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

if TYPE_CHECKING:
    import clickzetta.zettapark.session


@overload
def col(col_name: str) -> Column:
    """Returns the :class:`~clickzetta.zettapark.Column` with the specified name.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.select(col("a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


@overload
def col(df_alias: str, col_name: str) -> Column:
    """Returns the :class:`~clickzetta.zettapark.Column` with the specified dataframe alias and column name.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.alias("df").select(col("df", "a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


def col(name1: str, name2: Optional[str] = None) -> Column:
    if name2 is None:
        return Column(name1)
    else:
        return Column(name1, name2)


@overload
def column(col_name: str) -> Column:
    """Returns a :class:`~clickzetta.zettapark.Column` with the specified name. Alias for col.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.select(column("a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


@overload
def column(df_alias: str, col_name: str) -> Column:
    """Returns a :class:`~clickzetta.zettapark.Column` with the specified name and dataframe alias name. Alias for col.

    Example::
        >>> df = session.sql("select 1 as a")
        >>> df.alias("df").select(column("df", "a")).collect()
        [Row(A=1)]
    """
    ...  # pragma: no cover


def column(name1: str, name2: Optional[str] = None) -> Column:
    if name2 is None:
        return Column(name1)
    else:
        return Column(name1, name2)


def lit(literal: LiteralType) -> Column:
    """
    Creates a :class:`~clickzetta.zettapark.Column` expression for a literal value.
    It supports basic Python data types, including ``int``, ``float``, ``str``,
    ``bool``, ``bytes``, ``bytearray``, ``datetime.time``, ``datetime.date``,
    ``datetime.datetime`` and ``decimal.Decimal``. Also, it supports Python structured data types,
    including ``list``, ``tuple`` and ``dict``, but this container must
    be JSON serializable.
    """
    return literal if isinstance(literal, Column) else Column(Literal(literal))


def sql_expr(sql: str) -> Column:
    """Creates a :class:`~clickzetta.zettapark.Column` expression from raw SQL text.
    Note that the function does not interpret or check the SQL text.

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
        >>> df.filter("a > 1").collect()  # use SQL expression
        [Row(A=3, B=4)]
    """
    return Column._expr(sql)


def current_session() -> Column:
    """
    Returns a unique system identifier for the ClickZetta session corresponding to the present connection.
    This will generally be a system-generated alphanumeric string. It is NOT derived from the user name or user account.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_session()).collect()
        >>> assert result[0]['CURRENT_SESSION()'] is not None
    """
    return builtin("current_session")()


def current_statement() -> Column:
    """
    Returns the SQL text of the statement that is currently executing.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> session.create_dataframe([1]).select(current_statement()).collect()
        [Row(CURRENT_STATEMENT()='SELECT current_statement() FROM ( SELECT "_1" FROM ( SELECT $1 AS "_1" FROM  VALUES (1 :: INT)))')]
    """
    return builtin("current_statement")()


def current_user() -> Column:
    """
    Returns the name of the user currently logged into the system.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_user()).collect()
        >>> assert result[0]['CURRENT_USER()'] is not None
    """
    return builtin("current_user")()


def current_version() -> Column:
    """
    Returns the current ClickZetta version.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_version()).collect()
        >>> assert result[0]['CURRENT_VERSION()'] is not None
    """
    return builtin("current_version")()


def current_warehouse() -> Column:
    """
    Returns the name of the warehouse in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_warehouse()).collect()
        >>> assert result[0]['CURRENT_WAREHOUSE()'] is not None
    """
    return builtin("current_warehouse")()


def current_database() -> Column:
    """Returns the name of the database in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_database()).collect()
        >>> assert result[0]['CURRENT_DATABASE()'] is not None
    """
    return builtin("current_database")()


def current_role() -> Column:
    """Returns the name of the role in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_role()).collect()
        >>> assert result[0]['CURRENT_ROLE()'] is not None
    """
    return builtin("current_role")()


def current_schema() -> Column:
    """Returns the name of the schema in use for the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schema()).collect()
        >>> assert result[0]['CURRENT_SCHEMA()'] is not None
    """
    return builtin("current_schema")()


def current_schemas() -> Column:
    """Returns active search path schemas.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_schemas()).collect()
        >>> assert result[0]['CURRENT_SCHEMAS()'] is not None
    """
    return builtin("current_schemas")()


def current_region() -> Column:
    """Returns the name of the region for the account where the current user is logged in.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_region()).collect()
        >>> assert result[0]['CURRENT_REGION()'] is not None
    """
    return builtin("current_region")()


def current_account() -> Column:
    """Returns the name of the account used in the current session.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_account()).collect()
        >>> assert result[0]['CURRENT_ACCOUNT()'] is not None
    """
    return builtin("current_account")()


def current_available_roles() -> Column:
    """Returns a JSON string that lists all roles granted to the current user.

    Example:
        >>> # Return result is tied to session, so we only test if the result exists
        >>> result = session.create_dataframe([1]).select(current_available_roles()).collect()
        >>> assert result[0]['CURRENT_AVAILABLE_ROLES()'] is not None
    """
    return builtin("current_available_roles")()


def add_months(
    date_or_timestamp: ColumnOrName, number_of_months: Union[Column, int]
) -> Column:
    """Adds or subtracts a specified number of months to a date or timestamp, preserving the end-of-month information.

    Example:
        >>> import datetime
        >>> df = session.create_dataframe([datetime.date(2022, 4, 6)], schema=["d"])
        >>> df.select(add_months("d", 4)).collect()[0][0]
        datetime.date(2022, 8, 6)
    """
    c = _to_col_if_str(date_or_timestamp, "add_months")
    return builtin("add_months")(c, number_of_months)


def any_value(e: ColumnOrName) -> Column:
    """Returns a non-deterministic any value for the specified column.
    This is an aggregate and window function.

    Example:
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> result = df.select(any_value("a")).collect()
        >>> assert len(result) == 1  # non-deterministic value in result.
    """
    c = _to_col_if_str(e, "any_value")
    return call_builtin("any_value", c)


def bitnot(e: ColumnOrName) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(bitnot("a")).collect()[0][0]
        -2
    """
    c = _to_col_if_str(e, "bitnot")
    return call_builtin("bitnot", c)


def bitshiftleft(to_shift_column: ColumnOrName, n: Union[Column, int]) -> Column:
    """Returns the bitwise negation of a numeric expression.

    Example:
        >>> df = session.create_dataframe([2], schema=["a"])
        >>> df.select(bitshiftleft("a", 1)).collect()[0][0]
        4
    """
    c = _to_col_if_str(to_shift_column, "bitshiftleft")
    return call_builtin("bitshiftleft", c, n)


# no equivalent function in clickzetta lakehouse
#
# def bitshiftright(to_shift_column: ColumnOrName, n: Union[Column, int]) -> Column:
#     """Returns the bitwise negation of a numeric expression.

#     Example:
#         >>> df = session.create_dataframe([2], schema=["a"])
#         >>> df.select(bitshiftright("a", 1)).collect()[0][0]
#         1
#     """
#     c = _to_col_if_str(to_shift_column, "bitshiftright")
#     return call_builtin("bitshiftright", c, n)


def bround(col: ColumnOrName, scale: Union[Column, int]) -> Column:
    """
    Rounds the number using `HALF_TO_EVEN` option. The `HALF_TO_EVEN` rounding mode rounds the given decimal value to the specified scale (number of decimal places) as follows:
    * If scale is greater than or equal to 0, round to the specified number of decimal places using half-even rounding. This rounds towards the nearest value with ties (equally close values) rounding to the nearest even digit.
    * If scale is less than 0, round to the integral part of the decimal. This rounds towards 0 and truncates the decimal places.

    Note:

        1. The data type of the expression must be one of the `data types for a fixed-point number
        <https://doc.clickzetta.com/>`_.

        2. Data types for floating point numbers (e.g. FLOAT) are not supported with this argument.

        3. If the expression data type is not supported, the expression must be explicitly cast to decimal before calling.

    Example:
        >>> import decimal
        >>> data = [(decimal.Decimal(1.235)),(decimal.Decimal(3.5))]
        >>> df = session.createDataFrame(data, ["value"])
        >>> df.select(bround('VALUE',1).alias("VALUE")).show() # Rounds to 1 decimal place
        -----------
        |"VALUE"  |
        -----------
        |1.2      |
        |3.5      |
        -----------
        <BLANKLINE>
        >>> df.select(bround('VALUE',0).alias("VALUE")).show() # Rounds to 1 decimal place
        -----------
        |"VALUE"  |
        -----------
        |1        |
        |4        |
        -----------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "bround")
    scale = _to_col_if_lit(scale, "bround")
    return call_builtin("ROUND", col, scale)


def convert_timezone(
    target_timezone: ColumnOrName,
    source_time: ColumnOrName,
    source_timezone: Optional[ColumnOrName] = None,
) -> Column:
    """Converts the given source_time to the target timezone.

    For timezone information, refer to the `SQL convert_timezone notes <https://doc.clickzetta.com/>`_

        Args:
            target_timezone: The time zone to which the input timestamp should be converted.=
            source_time: The timestamp to convert. When it's a TIMESTAMP_LTZ, use ``None`` for ``source_timezone``.
            source_timezone: The time zone for the ``source_time``. Required for timestamps with no time zone (i.e. TIMESTAMP_NTZ). Use ``None`` if the timestamps have a time zone (i.e. TIMESTAMP_LTZ). Default is ``None``.

        Note:
            The sequence of the 3 params is different from the SQL function, which two overloads:

              - ``CONVERT_TIMEZONE( <source_tz> , <target_tz> , <source_timestamp_ntz> )``
              - ``CONVERT_TIMEZONE( <target_tz> , <source_timestamp> )``

            The first parameter ``source_tz`` is optional. But in Python an optional argument shouldn't be placed at the first.
            So ``source_timezone`` is after ``source_time``.

        Example:
            >>> import datetime
            >>> from dateutil import tz
            >>> datetime_with_tz = datetime.datetime(2022, 4, 6, 9, 0, 0, tzinfo=tz.tzoffset("myzone", -3600*7))
            >>> datetime_with_no_tz = datetime.datetime(2022, 4, 6, 9, 0, 0)
            >>> df = session.create_dataframe([[datetime_with_tz, datetime_with_no_tz]], schema=["a", "b"])
            >>> result = df.select(convert_timezone(lit("UTC"), col("a")), convert_timezone(lit("UTC"), col("b"), lit("Asia/Shanghai"))).collect()
            >>> result[0][0]
            datetime.datetime(2022, 4, 6, 16, 0, tzinfo=<UTC>)
            >>> result[0][1]
            datetime.datetime(2022, 4, 6, 1, 0)
    """
    source_tz = (
        _to_col_if_str(source_timezone, "convert_timezone")
        if source_timezone is not None
        else None
    )
    target_tz = _to_col_if_str(target_timezone, "convert_timezone")
    source_time_to_convert = _to_col_if_str(source_time, "convert_timezone")

    if source_timezone is None:
        return call_builtin("convert_timezone", target_tz, source_time_to_convert)
    return call_builtin(
        "convert_timezone", source_tz, target_tz, source_time_to_convert
    )


def approx_count_distinct(e: ColumnOrName) -> Column:
    """Uses HyperLogLog to return an approximation of the distinct cardinality of the input (i.e. HLL(col1, col2, ... )
    returns an approximation of COUNT(DISTINCT col1, col2, ... )).

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
        >>> df.select(approx_count_distinct("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3         |
        ------------
        <BLANKLINE>

    """
    c = _to_col_if_str(e, "approx_count_distinct")
    return builtin("approx_count_distinct")(c)


def avg(e: ColumnOrName) -> Column:
    """Returns the average of non-NULL records. If all records inside a group are NULL,
    the function returns NULL.

    Example::
        >>> df = session.create_dataframe([[1], [2], [2]], schema=["d"])
        >>> df.select(avg(df.d).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.666667  |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "avg")
    return builtin("avg")(c)


def corr(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the correlation coefficient for non-null pairs in a group.

    Example:
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(corr(col("a"), col("b")).alias("result")).show()
        ---------------------
        |"RESULT"           |
        ---------------------
        |0.813681508328809  |
        ---------------------
        <BLANKLINE>
    """
    raise ValueError("not support current function.")
    # c1 = _to_col_if_str(column1, "corr")
    # c2 = _to_col_if_str(column2, "corr")
    # return builtin("corr")(c1, c2)


def count(e: ColumnOrName) -> Column:
    """Returns either the number of non-NULL records for the specified columns, or the
    total number of records.

    Example:
        >>> df = session.create_dataframe([[1], [None], [3], [4], [None]], schema=["a"])
        >>> df.select(count(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3         |
        ------------
        <BLANKLINE>
        >>> df.select(count("*").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |5         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "count")
    return (
        builtin("count")(Literal(1))
        if isinstance(c._expression, Star)
        else builtin("count")(c._expression)
    )


def count_distinct(*cols: ColumnOrName) -> Column:
    """Returns either the number of non-NULL distinct records for the specified columns,
    or the total number of the distinct records.

    Example:

        >>> df = session.create_dataframe([[1, 2], [1, 2], [3, None], [2, 3], [3, None], [4, None]], schema=["a", "b"])
        >>> df.select(count_distinct(col("a"), col("b")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2         |
        ------------
        <BLANKLINE>
        >>> #  The result should be 2 for {[1,2],[2,3]} since the rest are either duplicate or NULL records
    """
    cs = [_to_col_if_str(c, "count_distinct") for c in cols]
    return Column(
        FunctionExpression("count", [c._expression for c in cs], is_distinct=True)
    )


def covar_pop(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the population covariance for non-null pairs in a group.

    Example:
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(covar_pop(col("a"), col("b")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.840     |
        ------------
        <BLANKLINE>
    """
    raise ValueError("not support current function.")
    # col1 = _to_col_if_str(column1, "covar_pop")
    # col2 = _to_col_if_str(column2, "covar_pop")
    # return builtin("covar_pop")(col1, col2)


def covar_samp(column1: ColumnOrName, column2: ColumnOrName) -> Column:
    """Returns the sample covariance for non-null pairs in a group.

    Example:
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [4, 5], [2, 3], [3, None], [4, None], [6,4]], schema=["a", "b"])
        >>> df.select(covar_samp(col("a"), col("b")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2.300     |
        ------------
        <BLANKLINE>
    """
    raise ValueError("not support current function.")
    # col1 = _to_col_if_str(column1, "covar_samp")
    # col2 = _to_col_if_str(column2, "covar_samp")
    # return builtin("covar_samp")(col1, col2)


def create_map(*cols: Union[ColumnOrName, Iterable[ColumnOrName]]) -> Column:
    """Transforms multiple column pairs into a single map :class:`~clickzetta.zettapark.Column` where each pair of
    columns is treated as a key-value pair in the resulting map.

    Args:
        *cols: A variable number of column names or :class:`~clickzetta.zettapark.Column` objects that can also be
               expressed as a list of columns.
               The function expects an even number of arguments, where each pair of arguments represents a key-value
               pair for the map.

    Returns:
        A :class:`~clickzetta.zettapark.Column` where each row contains a map created from the provided column pairs.

    Example:
        >>> from clickzetta.zettapark.functions import create_map
        >>> df = session.create_dataframe([("Paris", "France"), ("Tokyo", "Japan")], ("city", "country"))
        >>> df.select(create_map("city", "country").alias("map")).show()
        -----------------------
        |"MAP"                |
        -----------------------
        |{                    |
        |  "Paris": "France"  |
        |}                    |
        |{                    |
        |  "Tokyo": "Japan"   |
        |}                    |
        -----------------------
        <BLANKLINE>

        >>> df.select(create_map([df.city, df.country]).alias("map")).show()
        -----------------------
        |"MAP"                |
        -----------------------
        |{                    |
        |  "Paris": "France"  |
        |}                    |
        |{                    |
        |  "Tokyo": "Japan"   |
        |}                    |
        -----------------------
        <BLANKLINE>
    """
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]

    has_odd_columns = len(cols) & 1
    if has_odd_columns:
        raise ValueError(
            f"The 'create_map' function requires an even number of parameters but the actual number is {len(cols)}"
        )

    cols = [_to_col_if_str(c, "create_map") for c in cols]
    return builtin("map")(*cols)


def kurtosis(e: ColumnOrName) -> Column:
    """
    Returns the population excess kurtosis of non-NULL records. If all records
    inside a group are NULL, the function returns NULL.

    Example::

        >>> from clickzetta.zettapark.functions import kurtosis
        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(kurtosis("x").as_("x")).collect()
        [Row(X=Decimal('3.613736609956'))]
    """
    raise ValueError("not support current function.")
    # c = _to_col_if_str(e, "kurtosis")
    # return builtin("kurtosis")(c)


def max(e: ColumnOrName) -> Column:
    """
    Returns the maximum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(max("x").as_("x")).collect()
        [Row(X=10)]
    """
    c = _to_col_if_str(e, "max")
    return builtin("max")(c)


def mean(e: ColumnOrName) -> Column:
    """
    Return the average for the specific numeric columns. Alias of :func:`avg`.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(mean("x").as_("x")).collect()
        [Row(X=Decimal('3.600000'))]
    """
    c = _to_col_if_str(e, "mean")
    return avg(c)


def median(e: ColumnOrName) -> Column:
    """
    Returns the median value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(median("x").as_("x")).collect()
        [Row(X=Decimal('3.000'))]
    """
    c = _to_col_if_str(e, "median")
    return builtin("median")(c)


def min(e: ColumnOrName) -> Column:
    """
    Returns the minimum value for the records in a group. NULL values are ignored
    unless all the records are NULL, in which case a NULL value is returned.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 3], schema=["x"])
        >>> df.select(min("x").as_("x")).collect()
        [Row(X=1)]
    """
    c = _to_col_if_str(e, "min")
    return builtin("min")(c)


def mode(e: ColumnOrName) -> Column:
    """
    Returns the most frequent value for the records in a group. NULL values are ignored.
    If all the values are NULL, or there are 0 rows, then the function returns NULL.

    Example::

        >>> df = session.create_dataframe([1, 3, 10, 1, 4], schema=["x"])
        >>> df.select(mode("x").as_("x")).collect()
        [Row(X=1)]
    """
    c = _to_col_if_str(e, "mode")
    return builtin("mode")(c)


def skew(e: ColumnOrName) -> Column:
    """Returns the sample skewness of non-NULL records. If all records inside a group
    are NULL, the function returns NULL.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [10, 10, 20, 25, 30],
        ...     schema=["a"]
        ... ).select(skew("a").cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SKEW("A") AS NUMBER(38, 4))=Decimal('0.0524'))]
    """
    raise ValueError("not support current function.")
    # c = _to_col_if_str(e, "skew")
    # return builtin("skew")(c)


def stddev(e: ColumnOrName) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev(col("N")).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (STDDEV("N") AS NUMBER(38, 4))=Decimal('3.5355'))]
    """
    c = _to_col_if_str(e, "stddev")
    return builtin("stddev_samp")(c)


def stddev_samp(e: ColumnOrName) -> Column:
    """Returns the sample standard deviation (square root of sample variance) of
    non-NULL values. If all records inside a group are NULL, returns NULL. Alias of
    :func:`stddev`.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev_samp(col("N")).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (STDDEV_SAMP("N") AS NUMBER(38, 4))=Decimal('3.5355'))]
    """
    c = _to_col_if_str(e, "stddev_samp")
    return builtin("stddev_samp")(c)


def stddev_pop(e: ColumnOrName) -> Column:
    """Returns the population standard deviation (square root of variance) of non-NULL
    values. If all records inside a group are NULL, returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(stddev_pop(col("N")))
        >>> df.collect()
        [Row(STDDEV_POP("N")=2.5)]
    """
    c = _to_col_if_str(e, "stddev_pop")
    return builtin("stddev_pop")(c)


def sum(e: ColumnOrName) -> Column:
    """Returns the sum of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the sum of unique non-null values. If all records inside a group are
    NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(sum(col("N")))
        >>> df.collect()
        [Row(SUM("N")=13)]
    """
    c = _to_col_if_str(e, "sum")
    return builtin("sum")(c)


def sum_distinct(e: ColumnOrName) -> Column:
    """Returns the sum of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [1, 2, None, 3],
        ...     schema=["N"],
        ... ).select(sum_distinct(col("N")))
        >>> df.collect()
        [Row(SUM( DISTINCT "N")=6)]
    """
    c = _to_col_if_str(e, "sum_distinct")
    return _call_function("sum", True, c)


def bitand(e: ColumnOrName) -> Column:
    """Returns the bitand of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the bitand of unique non-null values. If all records inside a group are
    NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(bitand(col("N")))
        >>> df.collect()
        [Row(BIT_AND("N")=13)]
    """
    c = _to_col_if_str(e, "bitand")
    return builtin("bit_and")(c)


def bitand_distinct(e: ColumnOrName) -> Column:
    """Returns the bitand of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [1, 2, None, 3],
        ...     schema=["N"],
        ... ).select(bitand_distinct(col("N")))
        >>> df.collect()
        [Row(BIT_AND( DISTINCT "N")=6)]
    """
    c = _to_col_if_str(e, "bitand_distinct")
    return _call_function("bit_and", True, c)


def bitor(e: ColumnOrName) -> Column:
    """Returns the bitor of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the bitor of unique non-null values. If all records inside a group are
    NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(bitor(col("N")))
        >>> df.collect()
        [Row(BIT_OR("N")=13)]
    """
    c = _to_col_if_str(e, "bitor")
    return builtin("bit_or")(c)


def bitor_distinct(e: ColumnOrName) -> Column:
    """Returns the bitor of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [1, 2, None, 3],
        ...     schema=["N"],
        ... ).select(bitor_distinct(col("N")))
        >>> df.collect()
        [Row(BIT_OR( DISTINCT "N")=6)]
    """
    c = _to_col_if_str(e, "bitor_distinct")
    return _call_function("bit_or", True, c)


def bitxor(e: ColumnOrName) -> Column:
    """Returns the bitxor of non-NULL records in a group. You can use the DISTINCT keyword
    to compute the bitxor of unique non-null values. If all records inside a group are
    NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(bitxor(col("N")))
        >>> df.collect()
        [Row(BIT_XOR("N")=13)]
    """
    c = _to_col_if_str(e, "bitxor")
    return builtin("bit_xor")(c)


def bitxor_distinct(e: ColumnOrName) -> Column:
    """Returns the bitxor of non-NULL distinct records in a group. You can use the
    DISTINCT keyword to compute the sum of unique non-null values. If all records
    inside a group are NULL, the function returns NULL.

    Example::
        >>> df = session.create_dataframe(
        ...     [1, 2, None, 3],
        ...     schema=["N"],
        ... ).select(bitxor_distinct(col("N")))
        >>> df.collect()
        [Row(BIT_XOR( DISTINCT "N")=6)]
    """
    c = _to_col_if_str(e, "bitxor_distinct")
    return _call_function("bit_xor", True, c)


def variance(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. For a single row, NULL is returned as sample variance.

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('1.200000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('4.300000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(variance(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

    """
    c = _to_col_if_str(e, "variance")
    return builtin("var_samp")(c)


def var_samp(e: ColumnOrName) -> Column:
    """Returns the sample variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. For a single row, NULL is returned as sample variance.
    Alias of :func:`variance`

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('1.200000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=Decimal('4.300000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(var_samp(col("a"))).collect()
        [Row(VARIANCE("A")=None)]

    """
    c = _to_col_if_str(e, "var_samp")
    return variance(c)


def var_pop(e: ColumnOrName) -> Column:
    """Returns the population variance of non-NULL records in a group. If all records
    inside a group are NULL, a NULL is returned. The population variance of a single row is 0.0.

    Example::

        >>> df = session.create_dataframe([1, -1, 1, -1, -1], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('0.960000'))]

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('3.440000'))]

        >>> df = session.create_dataframe([None, None, None], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=None)]

        >>> df = session.create_dataframe([42], schema=["a"])
        >>> df.select(var_pop(col("a"))).collect()
        [Row(VAR_POP("A")=Decimal('0.000000'))]

    """
    c = _to_col_if_str(e, "var_pop")
    return builtin("var_pop")(c)


def approx_percentile(col: ColumnOrName, percentile: float) -> Column:
    """Returns an approximated value for the desired percentile. This function uses the t-Digest algorithm.

    Example::
        >>> df = session.create_dataframe([0,1,2,3,4,5,6,7,8,9], schema=["a"])
        >>> df.select(approx_percentile("a", 0.5).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |4.5       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "approx_percentile")
    return builtin("approx_percentile")(c, lit(percentile))


def approx_percentile_accumulate(col: ColumnOrName) -> Column:
    """Returns the internal representation of the t-Digest state (as a JSON object) at the end of aggregation.
    This function uses the t-Digest algorithm.

    Example::
        >>> df = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df.select(approx_percentile_accumulate("a").alias("result")).show()
        ------------------------------
        |"RESULT"                    |
        ------------------------------
        |{                           |
        |  "state": [                |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    2.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    3.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    4.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    5.000000000000000e+00,  |
        |    1.000000000000000e+00   |
        |  ],                        |
        |  "type": "tdigest",        |
        |  "version": 1              |
        |}                           |
        ------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "approx_percentile_accumulate")
    return builtin("approx_percentile_accumulate")(c)


def approx_percentile_estimate(state: ColumnOrName, percentile: float) -> Column:
    """Returns the desired approximated percentile value for the specified t-Digest state.
    APPROX_PERCENTILE_ESTIMATE(APPROX_PERCENTILE_ACCUMULATE(.)) is equivalent to
    APPROX_PERCENTILE(.).

    Example::
        >>> df = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df_accu = df.select(approx_percentile_accumulate("a").alias("app_percentile_accu"))
        >>> df_accu.select(approx_percentile_estimate("app_percentile_accu", 0.5).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |3.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(state, "approx_percentile_estimate")
    return builtin("approx_percentile_estimate")(c, lit(percentile))


def approx_percentile_combine(state: ColumnOrName) -> Column:
    """Combines (merges) percentile input states into a single output state.
    This allows scenarios where APPROX_PERCENTILE_ACCUMULATE is run over horizontal partitions
    of the same table, producing an algorithm state for each table partition. These states can
    later be combined using APPROX_PERCENTILE_COMBINE, producing the same output state as a
    single run of APPROX_PERCENTILE_ACCUMULATE over the entire table.

    Example::
        >>> df1 = session.create_dataframe([1,2,3,4,5], schema=["a"])
        >>> df2 = session.create_dataframe([6,7,8,9,10], schema=["b"])
        >>> df_accu1 = df1.select(approx_percentile_accumulate("a").alias("app_percentile_accu"))
        >>> df_accu2 = df2.select(approx_percentile_accumulate("b").alias("app_percentile_accu"))
        >>> df_accu1.union(df_accu2).select(approx_percentile_combine("app_percentile_accu").alias("result")).show()
        ------------------------------
        |"RESULT"                    |
        ------------------------------
        |{                           |
        |  "state": [                |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    2.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    3.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    4.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    5.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    6.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    7.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    8.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    9.000000000000000e+00,  |
        |    1.000000000000000e+00,  |
        |    1.000000000000000e+01,  |
        |    1.000000000000000e+00   |
        |  ],                        |
        |  "type": "tdigest",        |
        |  "version": 1              |
        |}                           |
        ------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(state, "approx_percentile_combine")
    return builtin("approx_percentile_combine")(c)


def explode(
    col: ColumnOrName,
) -> "clickzetta.zettapark.table_function.TableFunctionCall":
    """Flattens a given array or map type column into individual rows. The default
    column name for the output column in case of array input column is ``VALUE``,
    and is ``KEY`` and ``VALUE`` in case of map input column.

    Examples::
        >>> df = session.create_dataframe([[1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}, "Kimura"],
        ...                                [2, [11, 22], {"Sankaku": "Triangle"}, "Coffee"]],
        ...                                schema=["idx", "lists", "maps", "strs"])
        >>> df.select(df.idx, explode(df.lists)).sort(col("idx")).show()
        -------------------
        |"IDX"  |"VALUE"  |
        -------------------
        |1      |1        |
        |1      |2        |
        |1      |3        |
        |2      |11       |
        |2      |22       |
        -------------------
        <BLANKLINE>

        >>> df.select(df.strs, explode(df.maps)).sort(col("strs")).show()
        -----------------------------------------
        |"STRS"  |"KEY"        |"VALUE"         |
        -----------------------------------------
        |Coffee  |Sankaku      |"Triangle"      |
        |Kimura  |Ashi Garami  |"Single Leg X"  |
        -----------------------------------------
        <BLANKLINE>

        >>> df.select(explode(col("lists")).alias("uno")).sort(col("uno")).show()
        ---------
        |"UNO"  |
        ---------
        |1      |
        |2      |
        |3      |
        |11     |
        |22     |
        ---------
        <BLANKLINE>

        >>> df.select(explode('maps').as_("primo", "secundo")).sort(col("primo")).show()
        --------------------------------
        |"PRIMO"      |"SECUNDO"       |
        --------------------------------
        |Ashi Garami  |"Single Leg X"  |
        |Sankaku      |"Triangle"      |
        --------------------------------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "explode")
    func_call = clickzetta.zettapark.table_function._ExplodeFunctionCall(
        col, lit(False)
    )
    func_call._set_api_call_source("functions.explode")
    return func_call


def explode_outer(
    col: ColumnOrName,
) -> "clickzetta.zettapark.table_function.TableFunctionCall":
    """Flattens a given array or map type column into individual rows. Unlike :func:`explode`,
    if array or map is empty, null or empty, then null values are produced. The default column
    name for the output column in case of array input column is ``VALUE``, and is ``KEY`` and
    ``VALUE`` in case of map input column.

    Args:
        col: Column object or string name of the desired column

    Examples::
        >>> df = session.create_dataframe([[1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}],
        ...                                [2, [11, 22], {"Sankaku": "Triangle"}],
        ...                                [3, [], {}]],
        ...                                schema=["idx", "lists", "maps"])
        >>> df.select(df.idx, explode_outer(df.lists)).sort(col("idx")).show()
        -------------------
        |"IDX"  |"VALUE"  |
        -------------------
        |1      |1        |
        |1      |2        |
        |1      |3        |
        |2      |11       |
        |2      |22       |
        |3      |NULL     |
        -------------------
        <BLANKLINE>

        >>> df.select(df.idx, explode_outer(df.maps)).sort(col("idx")).show()
        ----------------------------------------
        |"IDX"  |"KEY"        |"VALUE"         |
        ----------------------------------------
        |1      |Ashi Garami  |"Single Leg X"  |
        |2      |Sankaku      |"Triangle"      |
        |3      |NULL         |NULL            |
        ----------------------------------------
        <BLANKLINE>

    See Also:
        :func:`explode`
    """
    col = _to_col_if_str(col, "explode_outer")
    func_call = clickzetta.zettapark.table_function._ExplodeFunctionCall(col, lit(True))
    func_call._set_api_call_source("functions.explode_outer")
    return func_call


def flatten(col: ColumnOrName) -> "Column":
    """Collection function: creates a single array from an array of arrays.
    If a structure of nested arrays is deeper than two levels,
    only one level of nesting is removed.

    Examples::
        >>> df = session.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])
        >>> df.show(truncate=False)
        +------------------------+
        |data                    |
        +------------------------+
        |[[1, 2, 3], [4, 5], [6]]|
        |[NULL, [4, 5]]          |
        +------------------------+
        >>> df.select(flatten(df.data).alias('r')).show()
        +------------------+
        |                 r|
        +------------------+
        |[1, 2, 3, 4, 5, 6]|
        |              NULL|
        +------------------+
    """
    col = _to_col_if_str(col, "flatten")
    return builtin("flatten")(col)


def grouping(*cols: ColumnOrName) -> Column:
    """
    Describes which of a list of expressions are grouped in a row produced by a GROUP BY query.

    :func:`grouping_id` is an alias of :func:`grouping`.

    Example::
        >>> from clickzetta.zettapark import GroupingSets
        >>> df = session.create_dataframe([[1, 2, 3], [4, 5, 6]],schema=["a", "b", "c"])
        >>> grouping_sets = GroupingSets([col("a")], [col("b")], [col("a"), col("b")])
        >>> df.group_by_grouping_sets(grouping_sets).agg([count("c"), grouping("a"), grouping("b"), grouping("a", "b")]).collect()
        [Row(A=1, B=2, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=0, GROUPING(A, B)=0), \
Row(A=4, B=5, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=0, GROUPING(A, B)=0), \
Row(A=1, B=None, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=1, GROUPING(A, B)=1), \
Row(A=4, B=None, COUNT(C)=1, GROUPING(A)=0, GROUPING(B)=1, GROUPING(A, B)=1), \
Row(A=None, B=2, COUNT(C)=1, GROUPING(A)=1, GROUPING(B)=0, GROUPING(A, B)=2), \
Row(A=None, B=5, COUNT(C)=1, GROUPING(A)=1, GROUPING(B)=0, GROUPING(A, B)=2)]
    """
    columns = [_to_col_if_str(c, "grouping") for c in cols]
    return builtin("grouping")(*columns)


grouping_id = grouping


def coalesce(*e: ColumnOrName) -> Column:
    """Returns the first non-NULL expression among its arguments, or NULL if all its
    arguments are NULL.

    Example::

        >>> df = session.create_dataframe([[1, 2, 3], [None, 2, 3], [None, None, 3], [None, None, None]], schema=['a', 'b', 'c'])
        >>> df.select(df.a, df.b, df.c, coalesce(df.a, df.b, df.c).as_("COALESCE")).show()
        -----------------------------------
        |"A"   |"B"   |"C"   |"COALESCE"  |
        -----------------------------------
        |1     |2     |3     |1           |
        |NULL  |2     |3     |2           |
        |NULL  |NULL  |3     |3           |
        |NULL  |NULL  |NULL  |NULL        |
        -----------------------------------
        <BLANKLINE>
    """
    c = [_to_col_if_str(ex, "coalesce") for ex in e]
    return builtin("coalesce")(*c)


def equal_nan(e: ColumnOrName) -> Column:
    """
    Return true if the value in the column is not a number (NaN).

    Example::

        >>> import math
        >>> df = session.create_dataframe([1.1, math.nan, 2.3], schema=["a"])
        >>> df.select(equal_nan(df["a"]).alias("equal_nan")).collect()
        [Row(EQUAL_NAN=False), Row(EQUAL_NAN=True), Row(EQUAL_NAN=False)]
    """
    c = _to_col_if_str(e, "equal_nan")
    return c.equal_nan()


def is_null(e: ColumnOrName) -> Column:
    """
    Return true if the value in the column is null.

    Example::

        >>> from clickzetta.zettapark.functions import is_null
        >>> df = session.create_dataframe([1.2, float("nan"), None, 1.0], schema=["a"])
        >>> df.select(is_null("a").as_("a")).collect()
        [Row(A=False), Row(A=False), Row(A=True), Row(A=False)]
    """
    c = _to_col_if_str(e, "is_null")
    return c.is_null()


def negate(e: ColumnOrName) -> Column:
    """Returns the negation of the value in the column (equivalent to a unary minus).

    Example::

        >>> df = session.create_dataframe([[1]], schema=["a"])
        >>> df.select(negate(col("a").alias("result"))).show()
        ------------
        |"RESULT"  |
        ------------
        |-1        |
        ------------
        <BLANKLINE>
    """

    c = _to_col_if_str(e, "negate")
    return -c


def not_(e: ColumnOrName) -> Column:
    """Returns the inverse of a boolean expression.

    Example::

        >>> df = session.create_dataframe([[True]], schema=["a"])
        >>> df.select(not_(col("a").alias("result"))).show()
        ------------
        |"RESULT"  |
        ------------
        |False     |
        ------------
        <BLANKLINE>
    """

    c = _to_col_if_str(e, "not_")
    return ~c


def random(seed: Optional[int] = None) -> Column:
    """Each call returns a pseudo-random 64-bit integer.

    Example::
        >>> df = session.sql("select 1")
        >>> df = df.select(random(123).alias("result"))
    """
    # s = seed if seed is not None else randint(-(2**63), 2**63 - 1)
    return builtin("RAND")()


def uniform(
    min_: Union[ColumnOrName, int, float],
    max_: Union[ColumnOrName, int, float],
    gen: Union[ColumnOrName, int, float],
) -> Column:
    """
    Returns a uniformly random number.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[1]],
        ...     schema=["a"],
        ... )
        >>> df.select(uniform(1, 100, col("a")).alias("UNIFORM")).collect()
        [Row(UNIFORM=62)]
    """

    def convert_limit_to_col(limit):
        if isinstance(limit, int):
            return lit(limit)
        elif isinstance(limit, float):
            return lit(limit).cast(FloatType())
        return _to_col_if_str(limit, "uniform")

    min_col = convert_limit_to_col(min_)
    max_col = convert_limit_to_col(max_)
    gen_col = (
        lit(gen) if isinstance(gen, (int, float)) else _to_col_if_str(gen, "uniform")
    )
    return _call_function(
        "uniform", False, min_col, max_col, gen_col, is_data_generator=True
    )


def seq1(sign: int = 0) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 1 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 1 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq1` to
            generate sequences.

    Example::
        >>> df = session.generator(seq1(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ1(0)=0), Row(SEQ1(0)=1), Row(SEQ1(0)=2)]
    """
    return _call_function("seq1", False, Literal(sign), is_data_generator=True)


def seq2(sign: int = 0) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 2 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 2 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq2` to
            generate sequences.

    Example::
        >>> df = session.generator(seq2(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ2(0)=0), Row(SEQ2(0)=1), Row(SEQ2(0)=2)]
    """
    return _call_function("seq2", False, Literal(sign), is_data_generator=True)


def seq4(sign: int = 0) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 4 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 4 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq4` to
            generate sequences.

    Example::
        >>> df = session.generator(seq4(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ4(0)=0), Row(SEQ4(0)=1), Row(SEQ4(0)=2)]
    """
    return _call_function("seq4", False, Literal(sign), is_data_generator=True)


def seq8(sign: int = 0) -> Column:
    """Returns a sequence of monotonically increasing integers, with wrap-around
    which happens after largest representable integer of integer width 8 byte.

    Args:
        sign: When 0, the sequence continues at 0 after wrap-around. When 1, the sequence
            continues at smallest representable 8 byte integer. Defaults to 0.

    See Also:
        - :meth:`Session.generator`, which can be used to generate in tandem with `seq8` to
            generate sequences.

    Example::
        >>> df = session.generator(seq8(0), rowcount=3)
        >>> df.collect()
        [Row(SEQ8(0)=0), Row(SEQ8(0)=1), Row(SEQ8(0)=2)]
    """
    return _call_function("seq8", False, Literal(sign), is_data_generator=True)


def to_decimal(e: ColumnOrName, precision: int, scale: int) -> Column:
    """Converts an input expression to a decimal.

    Example::
        >>> df = session.create_dataframe(['12', '11.3', '-90.12345'], schema=['a'])
        >>> df.select(to_decimal(col('a'), 38, 0).as_('ans')).collect()
        [Row(ANS=12), Row(ANS=11), Row(ANS=-90)]

        >>> df.select(to_decimal(col('a'), 38, 2).as_('ans')).collect()
        [Row(ANS=Decimal('12.00')), Row(ANS=Decimal('11.30')), Row(ANS=Decimal('-90.12'))]
    """
    c = _to_col_if_str(e, "to_decimal")
    return builtin("to_decimal")(c, lit(precision), lit(scale))


def div0(
    dividend: Union[ColumnOrName, int, float], divisor: Union[ColumnOrName, int, float]
) -> Column:
    """
    Performs division like the division operator (/),
    but returns 0 when the divisor is 0 (rather than reporting an error).

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(div0(df["a"], 1).alias("divided_by_one"), div0(df["a"], 0).alias("divided_by_zero")).collect()
        [Row(DIVIDED_BY_ONE=Decimal('1.000000'), DIVIDED_BY_ZERO=Decimal('0.000000'))]
    """
    dividend_col = (
        lit(dividend)
        if isinstance(dividend, (int, float))
        else _to_col_if_str(dividend, "div0")
    )
    divisor_col = (
        lit(divisor)
        if isinstance(divisor, (int, float))
        else _to_col_if_str(divisor, "div0")
    )
    return builtin("div0")(dividend_col, divisor_col)


def sqrt(e: ColumnOrName) -> Column:
    """Returns the square-root of a non-negative numeric expression.

    Example::
        >>> df = session.create_dataframe(
        ...     [4, 9],
        ...     schema=["N"],
        ... ).select(sqrt(col("N")))
        >>> df.collect()
        [Row(SQRT("N")=2.0), Row(SQRT("N")=3.0)]
    """
    c = _to_col_if_str(e, "sqrt")
    return builtin("sqrt")(c)


def abs(e: ColumnOrName) -> Column:
    """Returns the absolute value of a numeric expression.

    Example::
        >>> df = session.create_dataframe([[-1]], schema=["a"])
        >>> df.select(abs(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "abs")
    return builtin("abs")(c)


def acos(e: ColumnOrName) -> Column:
    """Computes the inverse cosine (arc cosine) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[0.5]], schema=["deg"])
        >>> df.select(acos(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.047     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "acos")
    return builtin("acos")(c)


def asin(e: ColumnOrName) -> Column:
    """Computes the inverse sine (arc sine) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1]], schema=["deg"])
        >>> df.select(asin(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.571     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "asin")
    return builtin("asin")(c)


def atan(e: ColumnOrName) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1]], schema=["deg"])
        >>> df.select(atan(col("deg")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.785     |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "atan")
    return builtin("atan")(c)


def atan2(y: ColumnOrName, x: ColumnOrName) -> Column:
    """Computes the inverse tangent (arc tangent) of its input;
    the result is a number in the interval [-pi, pi].

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2]], schema=["x", "y"])
        >>> df.select(atan2(df.x, df.y).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.464     |
        ------------
        <BLANKLINE>
    """
    y_col = _to_col_if_str(y, "atan2")
    x_col = _to_col_if_str(x, "atan2")
    return builtin("atan2")(y_col, x_col)


def ceil(e: ColumnOrName) -> Column:
    """Returns values from the specified column rounded to the nearest equal or larger
    integer.

    Example::

        >>> df = session.create_dataframe([135.135, -975.975], schema=["a"])
        >>> df.select(ceil(df["a"]).alias("ceil")).collect()
        [Row(CEIL=136.0), Row(CEIL=-975.0)]
    """
    c = _to_col_if_str(e, "ceil")
    return builtin("ceil")(c)


def cos(e: ColumnOrName) -> Column:
    """Computes the cosine of its argument; the argument should be expressed in radians.

    Example:
        >>> from math import pi
        >>> df = session.create_dataframe([[pi]], schema=["deg"])
        >>> df.select(cos(col("deg")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |-1.0      |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "cos")
    return builtin("cos")(c)


def cosh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic cosine of its argument.

    Example:
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[0]], schema=["deg"])
        >>> df.select(cosh(col("deg")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "cosh")
    return builtin("cosh")(c)


def exp(e: ColumnOrName) -> Column:
    """
    Computes Euler's number e raised to a floating-point value.

    Example::

        >>> import math
        >>> from clickzetta.zettapark.types import IntegerType
        >>> df = session.create_dataframe([0.0, math.log(10)], schema=["a"])
        >>> df.select(exp(df["a"]).cast(IntegerType()).alias("exp")).collect()
        [Row(EXP=1), Row(EXP=10)]
    """
    c = _to_col_if_str(e, "exp")
    return builtin("exp")(c)


def factorial(e: ColumnOrName) -> Column:
    """
    Computes the factorial of its input. The input argument must be an integer
    expression in the range of 0 to 33.

    Example::

        >>> df = session.create_dataframe([0, 1, 5, 10], schema=["a"])
        >>> df.select(factorial(df["a"]).alias("factorial")).collect()
        [Row(FACTORIAL=1), Row(FACTORIAL=1), Row(FACTORIAL=120), Row(FACTORIAL=3628800)]
    """
    c = _to_col_if_str(e, "factorial")
    return builtin("factorial")(c)


def floor(e: ColumnOrName) -> Column:
    """
    Returns values from the specified column rounded to the nearest equal or
    smaller integer.

    Examples::

        >>> df = session.create_dataframe([135.135, -975.975], schema=["a"])
        >>> df.select(floor(df["a"]).alias("floor")).collect()
        [Row(FLOOR=135.0), Row(FLOOR=-976.0)]
    """
    c = _to_col_if_str(e, "floor")
    return builtin("floor")(c)


def format_number(col: ColumnOrName, d: Union[Column, int]):
    """Format numbers to a specific number of decimal places with HALF_TO_EVEN rounding.

    Note:
        1. The data type of the expression must be one of the `data types for a fixed-point number
        <https://doc.clickzetta.com/>`_.

        2. Data types for floating point numbers (e.g. FLOAT) are not supported with this argument.

        3. If the expression data type is not supported, the expression must be explicitly cast to decimal before calling.

    Example::
            >>> import decimal
            >>> from clickzetta.zettapark.functions import format_number
            >>> data = [(1, decimal.Decimal(3.14159)), (2, decimal.Decimal(2.71828)), (3, decimal.Decimal(1.41421))]
            >>> df = session.createDataFrame(data, ["id", "value"])
            >>> df.select("id",format_number("value",2).alias("value")).show()
            ------------------
            |"ID"  |"VALUE"  |
            ------------------
            |1     |3.14     |
            |2     |2.72     |
            |3     |1.41     |
            ------------------
            <BLANKLINE>
    """
    col = _to_col_if_str(col, "format_number")
    return bround(col, d).cast(StringType())


def sin(e: ColumnOrName) -> Column:
    """Computes the sine of its argument; the argument should be expressed in radians.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.generator(seq1(0), rowcount=3).select(sin(seq1(0)).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.8415')), Row(CAST (SIN(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.9093'))]
    """
    c = _to_col_if_str(e, "sin")
    return builtin("sin")(c)


def sinh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic sine of its argument.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.generator(seq1(0), rowcount=3).select(sinh(seq1(0)).cast(DecimalType(scale=4)))
        >>> df.collect()
        [Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('1.1752')), Row(CAST (SINH(SEQ1(0)) AS NUMBER(38, 4))=Decimal('3.6269'))]
    """
    c = _to_col_if_str(e, "sinh")
    return builtin("sinh")(c)


def tan(e: ColumnOrName) -> Column:
    """Computes the tangent of its argument; the argument should be expressed in radians.

    Example::
       >>> from clickzetta.zettapark.types import DecimalType
       >>> df = session.create_dataframe([0, 1], schema=["N"]).select(
       ...     tan(col("N")).cast(DecimalType(scale=4))
       ... )
       >>> df.collect()
       [Row(CAST (TAN("N") AS NUMBER(38, 4))=Decimal('0.0000')), Row(CAST (TAN("N") AS NUMBER(38, 4))=Decimal('1.5574'))]
    """
    c = _to_col_if_str(e, "tan")
    return builtin("tan")(c)


def tanh(e: ColumnOrName) -> Column:
    """Computes the hyperbolic tangent of its argument.

    Example::
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([0, 1], schema=["N"]).select(
        ...     tanh(col("N").cast(DecimalType(scale=4)))
        ... )
        >>> df.collect()
        [Row(TANH( CAST ("N" AS NUMBER(38, 4)))=0.0), Row(TANH( CAST ("N" AS NUMBER(38, 4)))=0.7615941559557649)]
    """
    c = _to_col_if_str(e, "tanh")
    return builtin("tanh")(c)


def degrees(e: ColumnOrName) -> Column:
    """
    Converts radians to degrees.

    Example::

        >>> import math
        >>> from clickzetta.zettapark.types import StructType, StructField, DoubleType, IntegerType
        >>> df = session.create_dataframe(
        ...     [math.pi / 3, math.pi, 3 * math.pi],
        ...     schema=StructType([StructField("a", DoubleType())]),
        ... )
        >>> df.select(degrees(col("a")).cast(IntegerType()).alias("DEGREES")).collect()
        [Row(DEGREES=60), Row(DEGREES=180), Row(DEGREES=540)]
    """
    c = _to_col_if_str(e, "degrees")
    return builtin("degrees")(c)


def radians(e: ColumnOrName) -> Column:
    """Converts degrees to radians.

    Examples::

        >>> df = session.create_dataframe([[1.111], [2.222], [3.333]], schema=["a"])
        >>> df.select(radians(col("a")).cast("number(38, 5)").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.01939   |
        |0.03878   |
        |0.05817   |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "radians")
    return builtin("radians")(c)


def md5(e: ColumnOrName) -> Column:
    """
    Returns a 32-character hex-encoded string containing the 128-bit MD5 message digest.

     Example::

        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(md5("col"))
        >>> df.collect()
        [Row(MD5("COL")='0cc175b9c0f1b6a831c399e269772661'), Row(MD5("COL")='92eb5ffee6ae2fec3ad71c777531578f')]
    """
    c = _to_col_if_str(e, "md5")
    return builtin("md5")(c)


def sha1(e: ColumnOrName) -> Column:
    """Returns a 40-character hex-encoded string containing the 160-bit SHA-1 message digest.

    Example::
        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(sha1("col"))
        >>> df.collect()
        [Row(SHA1("COL")='86f7e437faa5a7fce15d1ddcb9eaeaea377667b8'), Row(SHA1("COL")='e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98')]
    """
    c = _to_col_if_str(e, "sha1")
    return builtin("sha1")(c)


def sha2(e: ColumnOrName, num_bits: int) -> Column:
    """Returns a hex-encoded string containing the N-bit SHA-2 message digest,
    where N is the specified output digest size.

    Example::
        >>> df = session.create_dataframe(["a", "b"], schema=["col"]).select(sha2("col", 256))
        >>> df.collect()
        [Row(SHA2("COL", 256)='ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb'), Row(SHA2("COL", 256)='3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d')]
    """
    permitted_values = [0, 224, 256, 384, 512]
    if num_bits not in permitted_values:
        raise ValueError(
            f"num_bits {num_bits} is not in the permitted values {permitted_values}"
        )
    c = _to_col_if_str(e, "sha2")
    return builtin("sha2")(c, num_bits)


def hash(*cols: ColumnOrName) -> Column:
    """
    Returns a signed 64-bit hash value. Note that HASH never returns NULL, even for NULL inputs.

    Examples::

        >>> import decimal
        >>> df = session.create_dataframe([[10, "10", decimal.Decimal(10), 10.0]], schema=["a", "b", "c", "d"])
        >>> df.select(hash("a").alias("hash_a"), hash("b").alias("hash_b"), hash("c").alias("hash_c"), hash("d").alias("hash_d")).collect()
        [Row(HASH_A=1599627706822963068, HASH_B=3622494980440108984, HASH_C=1599627706822963068, HASH_D=1599627706822963068)]
        >>> df.select(hash(lit(None)).alias("one"), hash(lit(None), lit(None)).alias("two"), hash(lit(None), lit(None), lit(None)).alias("three")).collect()
        [Row(ONE=8817975702393619368, TWO=953963258351104160, THREE=2941948363845684412)]
    """
    columns = [_to_col_if_str(c, "hash") for c in cols]
    hashed_columns = [builtin("murmurhash3_64")(c) for c in columns]
    return builtin("hash_combine")(hashed_columns)


def ascii(e: ColumnOrName) -> Column:
    """Returns the ASCII code for the first character of a string. If the string is empty,
    a value of 0 is returned.

    Example::

        >>> df = session.create_dataframe(['!', 'A', 'a', '', 'bcd', None], schema=['a'])
        >>> df.select(df.a, ascii(df.a).as_('ascii')).collect()
        [Row(A='!', ASCII=33), Row(A='A', ASCII=65), Row(A='a', ASCII=97), Row(A='', ASCII=0), Row(A='bcd', ASCII=98), Row(A=None, ASCII=None)]
    """
    c = _to_col_if_str(e, "ascii")
    return builtin("ascii")(c)


def initcap(e: ColumnOrName, delimiters: ColumnOrName = None) -> Column:
    """
    Returns the input string with the first letter of each word in uppercase
    and the subsequent letters in lowercase.

    ``delimiters`` is an optional argument specifying a string of one or more
    characters that ``initcap`` uses as separators for words in the input expression.

    If ``delimiters`` is not specified, any of the following characters in the
    input expressions are treated as word separators:

        ``<whitespace> ! ? @ " ^ # $ & ~ _ , . : ; + - * % / | \\ [ ] ( ) { } < >``

    Examples::

        >>> df = session.create_dataframe(["the sky is blue", "WE CAN HANDLE THIS", "ÄäÖößÜü", None], schema=["a"])
        >>> df.select(initcap(df["a"]).alias("initcap")).collect()
        [Row(INITCAP='The Sky Is Blue'), Row(INITCAP='We Can Handle This'), Row(INITCAP='Ääöößüü'), Row(INITCAP=None)]
        >>> df.select(initcap(df["a"], lit('')).alias("initcap")).collect()
        [Row(INITCAP='The sky is blue'), Row(INITCAP='We can handle this'), Row(INITCAP='Ääöößüü'), Row(INITCAP=None)]
    """
    c = _to_col_if_str(e, "initcap")
    if delimiters is None:
        return builtin("initcap")(c)
    delimiter_col = _to_col_if_str(delimiters, "initcap")
    return builtin("initcap")(c, delimiter_col)


def length(e: ColumnOrName) -> Column:
    """
    Returns the length of an input string or binary value. For strings,
    the length is the number of characters, and UTF-8 characters are counted as a
    single character. For binary, the length is the number of bytes.

    Example::

        >>> df = session.create_dataframe(["the sky is blue", "WE CAN HANDLE THIS", "ÄäÖößÜü", None], schema=["a"])
        >>> df.select(length(df["a"]).alias("length")).collect()
        [Row(LENGTH=15), Row(LENGTH=18), Row(LENGTH=7), Row(LENGTH=None)]
    """
    c = _to_col_if_str(e, "length")
    return builtin("length")(c)


def lower(e: ColumnOrName) -> Column:
    """
    Returns the input string with all characters converted to lowercase.

    Example::

        >>> df = session.create_dataframe(['abc', 'Abc', 'aBC', 'Anführungszeichen', '14.95 €'], schema=["a"])
        >>> df.select(lower(col("a"))).collect()
        [Row(LOWER("A")='abc'), Row(LOWER("A")='abc'), Row(LOWER("A")='abc'), Row(LOWER("A")='anführungszeichen'), Row(LOWER("A")='14.95 €')]
    """
    c = _to_col_if_str(e, "lower")
    return builtin("lower")(c)


def lpad(e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName) -> Column:
    """
    Left-pads a string with characters from another string, or left-pads a
    binary value with bytes from another binary value.

    Example::

        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(lpad(col("a"), 3, lit("k")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |kka       |
        |kkb       |
        |kkc       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "lpad")
    p = _to_col_if_str(pad, "lpad")
    return builtin("lpad")(c, lit(len), p)


def ltrim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """
    Removes leading characters, including whitespace, from a string.

    Example::

        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([["asss"], ["bsss"], ["csss"]], schema=["a"])
        >>> df.select(rtrim(col("a"), trim_string=lit("sss")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |a         |
        |b         |
        |c         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "ltrim")
    t = _to_col_if_str(trim_string, "ltrim") if trim_string is not None else None
    return builtin("ltrim")(c, t) if t is not None else builtin("ltrim")(c)


def rpad(e: ColumnOrName, len: Union[Column, int], pad: ColumnOrName) -> Column:
    """Right-pads a string with characters from another string, or right-pads a
    binary value with bytes from another binary value. When called, `e` is padded to length `len`
    with characters/bytes from `pad`.

    Example::

        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(rpad(col("a"), 3, lit("k")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |akk       |
        |bkk       |
        |ckk       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "rpad")
    p = _to_col_if_str(pad, "rpad")
    return builtin("rpad")(c, lit(len), p)


def rtrim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """Removes trailing characters, including whitespace, from a string.

    Example::

        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([["asss"], ["bsss"], ["csss"]], schema=["a"])
        >>> df.select(rtrim(col("a"), trim_string=lit("sss")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |a         |
        |b         |
        |c         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "rtrim")
    t = _to_col_if_str(trim_string, "rtrim") if trim_string is not None else None
    return builtin("rtrim")(c, t) if t is not None else builtin("rtrim")(c)


def repeat(s: ColumnOrName, n: Union[Column, int]) -> Column:
    """Builds a string by repeating the input for the specified number of times.

    Example::

        >>> df = session.create_dataframe([["a"], ["b"], ["c"]], schema=["a"])
        >>> df.select(repeat(col("a"), 3).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |aaa       |
        |bbb       |
        |ccc       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(s, "repeat")
    return builtin("repeat")(c, lit(n))


def reverse(col: ColumnOrName) -> Column:
    """Reverses the order of characters in a string, or of bytes in a binary value.

    Example::

        >>> df = session.create_dataframe([["Hello"], ["abc"]], schema=["col1"])
        >>> df.select(reverse(col("col1"))).show()
        -----------------------
        |"REVERSE(""COL1"")"  |
        -----------------------
        |olleH                |
        |cba                  |
        -----------------------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "reverse")
    return builtin("reverse")(col)


def soundex(e: ColumnOrName) -> Column:
    """Returns a string that contains a phonetic representation of the input string.

    Example::
        >>> df = session.create_dataframe(["Marsha", "Marcia"], schema=["V"]).select(soundex(col("V")))
        >>> df.collect()
        [Row(SOUNDEX("V")='M620'), Row(SOUNDEX("V")='M620')]
    """
    c = _to_col_if_str(e, "soundex")
    return builtin("soundex")(c)


def trim(e: ColumnOrName, trim_string: Optional[ColumnOrName] = None) -> Column:
    """Removes leading and trailing characters from a string. Per default only whitespace ' ' characters are removed.

    Example::

        >>> df = session.create_dataframe(['hello', ' world', '   !   '], schema=["a"])
        >>> df.collect()
        [Row(A='hello'), Row(A=' world'), Row(A='   !   ')]
        >>> df.select(trim(col("a"))).collect()
        [Row(TRIM("A")='hello'), Row(TRIM("A")='world'), Row(TRIM("A")='!')]

    Example::

        >>> df = session.create_dataframe(['EUR 12.96', '7.89USD', '5.99E'], schema=["a"])
        >>> df.select(trim(col("a"), lit("EURUSD ")).as_("ans")).collect()
        [Row(ANS='12.96'), Row(ANS='7.89'), Row(ANS='5.99')]

    Example::

        >>> df = session.create_dataframe(['abc12 45a 79bc!'], schema=["a"])
        >>> df.select(trim(col("a"), lit("abc!")).as_("ans")).collect()
        [Row(ANS='12 45a 79')]

    """
    c = _to_col_if_str(e, "trim")
    t = _to_col_if_str(trim_string, "trim") if trim_string is not None else None
    return builtin("trim")(c, t) if t is not None else builtin("trim")(c)


def upper(e: ColumnOrName) -> Column:
    """Returns the input string with all characters converted to uppercase.
       Unicode characters are supported.

    Example::

        >>> df = session.create_dataframe(['abc', 'Abc', 'aBC', 'Anführungszeichen', '14.95 €'], schema=["a"])
        >>> df.select(upper(col("a"))).collect()
        [Row(UPPER("A")='ABC'), Row(UPPER("A")='ABC'), Row(UPPER("A")='ABC'), Row(UPPER("A")='ANFÜHRUNGSZEICHEN'), Row(UPPER("A")='14.95 €')]
    """
    c = _to_col_if_str(e, "upper")
    return builtin("upper")(c)


# no equivalent function in clickzetta lakehouse
#
# def strtok_to_array(
#     text: ColumnOrName, delimiter: Optional[ColumnOrName] = None
# ) -> Column:
#     """
#     Tokenizes the given string using the given set of delimiters and returns the tokens as an array.

#     If either parameter is a NULL, a NULL is returned. An empty array is returned if tokenization produces no tokens.

#     Example::
#         >>> df = session.create_dataframe(
#         ...     [["a.b.c", "."], ["1,2.3", ","]],
#         ...     schema=["text", "delimiter"],
#         ... )
#         >>> df.select(strtok_to_array("text", "delimiter").alias("TIME_FROM_PARTS")).collect()
#         [Row(TIME_FROM_PARTS='[\\n  "a",\\n  "b",\\n  "c"\\n]'), Row(TIME_FROM_PARTS='[\\n  "1",\\n  "2.3"\\n]')]
#     """
#     t = _to_col_if_str(text, "strtok_to_array")
#     d = (
#         _to_col_if_str(delimiter, "strtok_to_array")
#         if (delimiter is not None)
#         else None
#     )
#     return (
#         builtin("strtok_to_array")(t, d)
#         if (delimiter is not None)
#         else builtin("strtok_to_array")(t)
#     )


def struct(*cols: ColumnOrName) -> Column:
    """
    Returns an OBJECT constructed with the given columns.

    Example::
        >>> from clickzetta.zettapark.functions import struct
        >>> df = session.createDataFrame([("Bob", 80), ("Alice", None)], ["name", "age"])
        >>> res = df.select(struct("age", "name").alias("struct")).show()
        ---------------------
        |"STRUCT"           |
        ---------------------
        |{                  |
        |  "age": 80,       |
        |  "name": "Bob"    |
        |}                  |
        |{                  |
        |  "age": null,     |
        |  "name": "Alice"  |
        |}                  |
        ---------------------
        <BLANKLINE>
    """

    def flatten_col_list(obj):
        if isinstance(obj, str) or isinstance(obj, Column):
            return [obj]
        elif hasattr(obj, "__iter__"):
            acc = []
            for innerObj in obj:
                acc = acc + flatten_col_list(innerObj)
            return acc

    new_cols = []
    for c in flatten_col_list(cols):
        # first insert field_name
        if isinstance(c, str):
            new_cols.append(lit(c))
        else:
            name = c._expression.name
            name = name[1:] if name.startswith("`") else name
            name = name[:-1] if name.endswith("`") else name
            new_cols.append(lit(name))
        # next insert field value
        c = _to_col_if_str(c, "struct")
        if isinstance(c, Column) and isinstance(c._expression, Alias):
            new_cols.append(col(c._expression.children[0]))
        else:
            new_cols.append(c)
    return builtin("named_struct")(*new_cols)


def log(
    base: Union[ColumnOrName, int, float], x: Union[ColumnOrName, int, float]
) -> Column:
    """
    Returns the logarithm of a numeric expression.

    Example::

        >>> from clickzetta.zettapark.types import IntegerType
        >>> df = session.create_dataframe([1, 10], schema=["a"])
        >>> df.select(log(10, df["a"]).cast(IntegerType()).alias("log")).collect()
        [Row(LOG=0), Row(LOG=1)]
    """
    # b = lit(base) if isinstance(base, (int, float)) else _to_col_if_str(base, "log")
    arg = lit(x) if isinstance(x, (int, float)) else _to_col_if_str(x, "log")

    base_value = 0
    if isinstance(base, (int, float)):
        base_value = base
    elif isinstance(base, Column):
        if isinstance(base._expression, Literal):
            base_value = base._expression.value
    elif isinstance(base, str):
        base_value = int(base)
    if base_value == 1:
        return builtin("log1p")(arg)
    elif base_value == 2:
        return builtin("log2")(arg)
    elif base_value == 10:
        return builtin("log10")(arg)
    raise ValueError("log function only support base = 1 or 2 or 10")


def pow(
    left: Union[ColumnOrName, int, float], right: Union[ColumnOrName, int, float]
) -> Column:
    """Returns a number (left) raised to the specified power (right).

    Example::
        >>> df = session.create_dataframe([[2, 3], [3, 4]], schema=["x", "y"])
        >>> df.select(pow(col("x"), col("y")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |8.0       |
        |81.0      |
        ------------
        <BLANKLINE>
    """
    number = (
        lit(left) if isinstance(left, (int, float)) else _to_col_if_str(left, "pow")
    )
    power = (
        lit(right) if isinstance(right, (int, float)) else _to_col_if_str(right, "pow")
    )
    return builtin("pow")(number, power)


def round(e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0) -> Column:
    """Returns rounded values from the specified column.

    Example::

        >>> df = session.create_dataframe([[1.11], [2.22], [3.33]], schema=["a"])
        >>> df.select(round(col("a")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1.0       |
        |2.0       |
        |3.0       |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "round")
    scale_col = (
        lit(scale)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "round")
    )
    return builtin("round")(c, scale_col)


def sign(col: ColumnOrName) -> Column:
    """
    Returns the sign of its argument:

        - -1 if the argument is negative.
        - 1 if it is positive.
        - 0 if it is 0.

    Args:
        col: The column to evaluate its sign

    Example::
        >>> df = session.create_dataframe([(-2, 2, 0)], ["a", "b", "c"])
        >>> df.select(sign("a").alias("a_sign"), sign("b").alias("b_sign"), sign("c").alias("c_sign")).show()
        ----------------------------------
        |"A_SIGN"  |"B_SIGN"  |"C_SIGN"  |
        ----------------------------------
        |-1        |1         |0         |
        ----------------------------------
        <BLANKLINE>
    """
    return builtin("sign")(_to_col_if_str(col, "sign"))


def split(
    str: ColumnOrName,
    pattern: ColumnOrName,
) -> Column:
    """Splits a given string with a given separator and returns the result in an array
    of strings. To specify a string separator, use the :func:`lit()` function.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [["many-many-words", "-"], ["hello--hello", "--"]],
        ...     schema=["V", "D"],
        ... ).select(split(col("V"), col("D")))
        >>> df.show()
        -------------------------
        |"SPLIT(""V"", ""D"")"  |
        -------------------------
        |[                      |
        |  "many",              |
        |  "many",              |
        |  "words"              |
        |]                      |
        |[                      |
        |  "hello",             |
        |  "hello"              |
        |]                      |
        -------------------------
        <BLANKLINE>

    Example 2::

        >>> df = session.create_dataframe([["many-many-words"],["hello-hi-hello"]],schema=["V"],)
        >>> df.select(split(col("V"), lit("-"))).show()
        -----------------------
        |"SPLIT(""V"", '-')"  |
        -----------------------
        |[                    |
        |  "many",            |
        |  "many",            |
        |  "words"            |
        |]                    |
        |[                    |
        |  "hello",           |
        |  "hi",              |
        |  "hello"            |
        |]                    |
        -----------------------
        <BLANKLINE>
    """
    s = _to_col_if_str(str, "split")
    p = _to_col_if_str(pattern, "split")
    return builtin("split")(s, p)


def substring(
    str: ColumnOrName, pos: Union[Column, int], len: Union[Column, int]
) -> Column:
    """Returns the portion of the string or binary value str, starting from the
    character/byte specified by pos, with limited length. The length should be greater
    than or equal to zero. If the length is a negative number, the function returns an
    empty string.

    Note:
        For ``pos``, 1 is the first character of the string in ClickZetta database.

    :func:`substr` is an alias of :func:`substring`.

    Example::
        >>> df = session.create_dataframe(
        ...     ["abc", "def"],
        ...     schema=["S"],
        ... ).select(substring(col("S"), 1, 1))
        >>> df.collect()
        [Row(SUBSTRING("S", 1, 1)='a'), Row(SUBSTRING("S", 1, 1)='d')]
    """
    s = _to_col_if_str(str, "substring")
    p = pos if isinstance(pos, Column) else lit(pos)
    length = len if isinstance(len, Column) else lit(len)
    return builtin("substring")(s, p, length)


# no equivalent function in clickzetta lakehouse
#
# def substring_index(
#     text: ColumnOrName, delim: ColumnOrLiteralStr, count: int
# ) -> Column:
#     """
#     Returns the substring from string ``text`` before ``count`` occurrences of the delimiter ``delim``.
#     If ``count`` is positive, everything to the left of the final delimiter (counting from left) is
#     returned. If ``count`` is negative, everything to the right of the final delimiter (counting from the
#     right) is returned. If ``count`` is zero, returns empty string.

#     Example 1::
#         >>> df = session.create_dataframe(
#         ...     ["a.b.c.d"],
#         ...     schema=["S"],
#         ... ).select(substring_index(col("S"), ".", 2).alias("result"))
#         >>> df.show()
#         ------------
#         |"RESULT"  |
#         ------------
#         |a.b       |
#         ------------
#         <BLANKLINE>

#     Example 2::
#         >>> df = session.create_dataframe(
#         ...     [["a.b.c.d", "."]],
#         ...     schema=["S", "delimiter"],
#         ... ).select(substring_index(col("S"), col("delimiter"), 2).alias("result"))
#         >>> df.show()
#         ------------
#         |"RESULT"  |
#         ------------
#         |a.b       |
#         ------------
#         <BLANKLINE>
#     """
#     s = _to_col_if_str(text, "substring_index")
#     strtok_array = builtin("strtok_to_array")(s, delim)
#     return builtin("array_to_string")(
#         builtin("array_slice")(
#             strtok_array,
#             0 if count >= 0 else builtin("array_size")(strtok_array) + count,
#             count if count >= 0 else builtin("array_size")(strtok_array),
#         ),
#         delim,
#     )


def regexp_count(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    position: Union[Column, int] = 1,
    *parameters: ColumnOrLiteral,
) -> Column:
    """Returns the number of times that a pattern occurs in the subject.

    Example::

        >>> df = session.sql("select * from values('apple'),('banana'),('peach') as T(a)")
        >>> df.select(regexp_count(col("a"), "a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |3         |
        |1         |
        ------------
        <BLANKLINE>
    """
    sql_func_name = "regexp_count"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    pos = lit(position)

    params = [lit(p) for p in parameters]
    return builtin(sql_func_name)(sub, pat, pos, *params)


def regexp_extract(
    value: ColumnOrLiteralStr, regexp: ColumnOrLiteralStr, idx: int
) -> Column:
    r"""
    Extract a specific group matched by a regex, from the specified string column.
    If the regex did not match, or the specified group did not match,
    an empty string is returned.

    Example::

        >>> from clickzetta.zettapark.functions import regexp_extract
        >>> df = session.createDataFrame([["id_20_30", 10], ["id_40_50", 30]], ["id", "age"])
        >>> df.select(regexp_extract("id", r"(\d+)", 1).alias("RES")).show()
        ---------
        |"RES"  |
        ---------
        |20     |
        |40     |
        ---------
        <BLANKLINE>
    """
    value = _to_col_if_str(value, "regexp_extract")
    regexp = _to_col_if_lit(regexp, "regexp_extract")
    idx = _to_col_if_lit(idx, "regexp_extract")
    return builtin("regexp_extract")(value, regexp, idx)


def regexp_replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
    position: Union[Column, int] = 1,
    # occurrences: Union[Column, int] = 0,
    # *parameters: ColumnOrLiteral,
) -> Column:
    """Returns the subject with the specified pattern (or all occurrences of the pattern) either removed or replaced by a replacement string.
    If no matches are found, returns the original subject.

    Example::
        >>> df = session.create_dataframe(
        ...     [["It was the best of times, it was the worst of times"]], schema=["a"]
        ... )
        >>> df.select(regexp_replace(col("a"), lit("( ){1,}"), lit("")).alias("result")).show()
        --------------------------------------------
        |"RESULT"                                  |
        --------------------------------------------
        |Itwasthebestoftimes,itwastheworstoftimes  |
        --------------------------------------------
        <BLANKLINE>
    """
    sql_func_name = "regexp_replace"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    rep = lit(replacement)
    pos = lit(position)
    return builtin(sql_func_name)(sub, pat, rep, pos)


def replace(
    subject: ColumnOrName,
    pattern: ColumnOrLiteralStr,
    replacement: ColumnOrLiteralStr = "",
) -> Column:
    """
    Removes all occurrences of a specified subject and optionally replaces them with replacement.

    Example::

        >>> df = session.create_dataframe([["apple"], ["apple pie"], ["apple juice"]], schema=["a"])
        >>> df.select(replace(col("a"), "apple", "orange").alias("result")).show()
        ----------------
        |"RESULT"      |
        ----------------
        |orange        |
        |orange pie    |
        |orange juice  |
        ----------------
        <BLANKLINE>
    """
    sql_func_name = "replace"
    sub = _to_col_if_str(subject, sql_func_name)
    pat = lit(pattern)
    rep = lit(replacement)
    return builtin(sql_func_name)(sub, pat, rep)


def charindex(
    target_expr: ColumnOrName,
    source_expr: ColumnOrName,
    position: Optional[Union[Column, int]] = None,
) -> Column:
    """Searches for ``target_expr`` in ``source_expr`` and, if successful,
    returns the position (1-based) of the ``target_expr`` in ``source_expr``.

    Args:
        target_expr: A string or binary expression representing the value to look for.
        source_expr: A string or binary expression representing the value to search.
        position: A number indication the position (1-based) from where to start the search. Defaults to None.

    Examples::
        >>> df = session.create_dataframe(["banana"], schema=['a'])
        >>> df.select(charindex(lit("an"), df.a, 1).as_("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |2         |
        ------------
        <BLANKLINE>

        >>> df.select(charindex(lit("an"), df.a, 3).as_("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |4         |
        ------------
        <BLANKLINE>
    """
    t = _to_col_if_str(target_expr, "charindex")
    s = _to_col_if_str(source_expr, "charindex")
    return (
        builtin("charindex")(t, s, lit(position))
        if position is not None
        else builtin("charindex")(t, s)
    )


def concat(*cols: ColumnOrName) -> Column:
    """Concatenates one or more strings, or concatenates one or more binary values. If any of the values is null, the result is also null.

    Example::
        >>> df = session.create_dataframe([['Hello', 'World']], schema=['a', 'b'])
        >>> df.select(concat(df.a, df.b)).show()
        --------------------------
        |"CONCAT(""A"", ""B"")"  |
        --------------------------
        |HelloWorld              |
        --------------------------
        <BLANKLINE>
    """

    columns = [_to_col_if_str(c, "concat") for c in cols]
    return builtin("concat")(*columns)


def concat_ws(*cols: ColumnOrName) -> Column:
    """Concatenates two or more strings, or concatenates two or more binary values. If any of the values is null, the result is also null.
    The CONCAT_WS operator requires at least two arguments, and uses the first argument to separate all following arguments.

    Examples::
        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([['Hello', 'World']], schema=['a', 'b'])
        >>> df.select(concat_ws(lit(','), df.a, df.b)).show()
        ----------------------------------
        |"CONCAT_WS(',', ""A"", ""B"")"  |
        ----------------------------------
        |Hello,World                     |
        ----------------------------------
        <BLANKLINE>

        >>> df = session.create_dataframe([['Hello', 'World', ',']], schema=['a', 'b', 'sep'])
        >>> df.select(concat_ws('sep', df.a, df.b)).show()
        --------------------------------------
        |"CONCAT_WS(""SEP"", ""A"", ""B"")"  |
        --------------------------------------
        |Hello,World                         |
        --------------------------------------
        <BLANKLINE>
    """
    columns = [_to_col_if_str(c, "concat_ws") for c in cols]
    return builtin("concat_ws")(*columns)


def translate(
    src: ColumnOrName,
    source_alphabet: ColumnOrName,
    target_alphabet: ColumnOrName,
) -> Column:
    """Translates src from the characters in source_alphabet to the characters in
    target_alphabet. Each character matching a character at position i in the source_alphabet is replaced
    with the character at position i in the target_alphabet. If target_alphabet is shorter, and there is no corresponding
    character the character is omitted. target_alphabet can not be longer than source_alphabet.

    Example::

        >>> df = session.create_dataframe(["abcdef", "abba"], schema=["a"])
        >>> df.select(translate(col("a"), lit("abc"), lit("ABC")).as_("ans")).collect()
        [Row(ANS='ABCdef'), Row(ANS='ABBA')]

        >>> df = session.create_dataframe(["file with spaces.txt", "\\ttest"], schema=["a"])
        >>> df.select(translate(col("a"), lit(" \\t"), lit("_")).as_("ans")).collect()
        [Row(ANS='file_with_spaces.txt'), Row(ANS='test')]

    """
    source = _to_col_if_str(src, "translate")
    source_alphabet = _to_col_if_str(source_alphabet, "translate")
    target_alphabet = _to_col_if_str(target_alphabet, "translate")
    return builtin("translate")(source, source_alphabet, target_alphabet)


def contains(col: ColumnOrName, string: ColumnOrName) -> Column:
    """Returns if `col` contains `string` for each row. See `CONTAINS <https://doc.clickzetta.com/>`

    Example:
        >>> df = session.create_dataframe([[1,2], [3,4], [5,5] ], schema=["a","b"])
        >>> df.select(contains(col("a"), col("b")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |False     |
        |False     |
        |True      |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "contains")
    s = _to_col_if_str(string, "contains")
    return builtin("contains")(c, s)


def startswith(col: ColumnOrName, str: ColumnOrName) -> Column:
    """Returns true if col starts with str.

    Example::
        >>> df = session.create_dataframe(
        ...     [["abc", "a"], ["abc", "s"]],
        ...     schema=["S", "P"],
        ... ).select(startswith(col("S"), col("P")))
        >>> df.collect()
        [Row(STARTSWITH("S", "P")=True), Row(STARTSWITH("S", "P")=False)]
    """
    c = _to_col_if_str(col, "startswith")
    s = _to_col_if_str(str, "startswith")
    # use locate to mimic startswith
    return builtin("locate")(s, c) == lit(1)


def endswith(col: ColumnOrName, str: ColumnOrName) -> Column:
    """
    Returns true if col ends with str.

    Example::

        >>> df = session.create_dataframe(["apple", "banana", "peach"], schema=["a"])
        >>> df.select(endswith(df["a"], lit("ana")).alias("endswith")).collect()
        [Row(ENDSWITH=False), Row(ENDSWITH=True), Row(ENDSWITH=False)]
    """
    c = _to_col_if_str(col, "endswith")
    s = _to_col_if_str(str, "endswith")
    # use locate to mimic endswith
    return builtin("locate")(s, c, length(c) - length(s)) != lit(0)


def insert(
    base_expr: ColumnOrName,
    position: Union[Column, int],
    length: Union[Column, int],
    insert_expr: ColumnOrName,
) -> Column:
    """
    Replaces a substring of the specified length, starting at the specified position,
    with a new string or binary value.

    Examples::

        >>> df = session.create_dataframe(["abc"], schema=["a"])
        >>> df.select(insert(df["a"], 1, 2, lit("Z")).alias("insert")).collect()
        [Row(INSERT='Zc')]
    """
    b = _to_col_if_str(base_expr, "insert")
    i = _to_col_if_str(insert_expr, "insert")
    return builtin("insert")(b, lit(position), lit(length), i)


def left(str_expr: ColumnOrName, length: Union[Column, int]) -> Column:
    """Returns a left most substring of ``str_expr``.

    Example::

        >>> df = session.create_dataframe([["abc"], ["def"]], schema=["a"])
        >>> df.select(left(col("a"), 2).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |ab        |
        |de        |
        ------------
        <BLANKLINE>
    """
    s = _to_col_if_str(str_expr, "left")
    return builtin("left")(s, lit(length))


def right(str_expr: ColumnOrName, length: Union[Column, int]) -> Column:
    """Returns a right most substring of ``str_expr``.

    Example::

        >>> df = session.create_dataframe([["abc"], ["def"]], schema=["a"])
        >>> df.select(right(col("a"), 2).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |bc        |
        |ef        |
        ------------
        <BLANKLINE>
    """
    s = _to_col_if_str(str_expr, "right")
    return builtin("right")(s, lit(length))


def char(col: ColumnOrName) -> Column:
    """Converts a Unicode code point (including 7-bit ASCII) into the character that
    matches the input Unicode.

    Example::

        >>> df = session.create_dataframe([83, 33, 169, 8364, None], schema=['a'])
        >>> df.select(df.a, char(df.a).as_('char')).sort(df.a).show()
        -----------------
        |"A"   |"CHAR"  |
        -----------------
        |NULL  |NULL    |
        |33    |!       |
        |83    |S       |
        |169   |©       |
        |8364  |€       |
        -----------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "char")
    return builtin("char")(c)


# to_char = char
# def to_char(c: ColumnOrName, format: Optional[str] = None) -> Column:
#     """Converts a Unicode code point (including 7-bit ASCII) into the character that
#     matches the input Unicode.

#     Example::
#         >>> df = session.create_dataframe([1, 2, 3, 4], schema=['a'])
#         >>> df.select(to_char(col('a')).as_('ans')).collect()
#         [Row(ANS='1'), Row(ANS='2'), Row(ANS='3'), Row(ANS='4')]

#     Example::

#         >>> import datetime
#         >>> df = session.create_dataframe([datetime.datetime(2023, 4, 16), datetime.datetime(2017, 4, 3, 2, 59, 37, 153)], schema=['a'])
#         >>> df.select(to_char(col('a')).as_('ans')).collect()
#         [Row(ANS='2023-04-16 00:00:00.000'), Row(ANS='2017-04-03 02:59:37.000')]

#     """
#     c = try_cast(_to_col_if_str(c, "to_char"), StringType())
#     return builtin("")(c, lit(format)) if format is not None else builtin("")(c)


def date_format(e: ColumnOrName, fmt: str) -> Column:
    """Converts an input expression into the corresponding date in the specified date format.

    Example::
        >>> df = session.create_dataframe([("2023-10-10",), ("2022-05-15",), ("invalid",)], schema=['date'])
        >>> df.select(date_format('date', 'YYYY/MM/DD').as_('formatted_date')).show()
        --------------------
        |"FORMATTED_DATE"  |
        --------------------
        |2023/10/10        |
        |2022/05/15        |
        |NULL              |
        --------------------
        <BLANKLINE>

    Example::
        >>> df = session.create_dataframe([("2023-10-10 15:30:00",), ("2022-05-15 10:45:00",)], schema=['timestamp'])
        >>> df.select(date_format('timestamp', 'YYYY/MM/DD HH:mi:ss').as_('formatted_ts')).show()
        -----------------------
        |"FORMATTED_TS"       |
        -----------------------
        |2023/10/10 15:30:00  |
        |2022/05/15 10:45:00  |
        -----------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "date_format")
    return builtin("date_format")(c, lit(fmt))


def to_time(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding time.

    Example::

        >>> df = session.create_dataframe(['04:15:29.999'], schema=['a'])
        >>> df.select(to_time(col("a"))).collect()
        [Row(TO_TIME("A")=datetime.time(4, 15, 29, 999000))]
    """
    c = _to_col_if_str(e, "to_time")
    return builtin("to_time")(c, fmt) if fmt is not None else builtin("to_time")(c)


def to_timestamp(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into the corresponding timestamp.

    Per default fmt is set to auto, which makes ClickZetta detect the format automatically. With `to_timestamp` strings
    can be converted to timestamps. The format has to be specified according to the rules set forth in
    <https://doc.clickzetta.com/>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['a'])
        >>> df.select(to_timestamp(col("a")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 1, 2, 3, 4000))]
        >>> df = session.create_dataframe(["2020-05-01 13:11:20.000"], schema=['a'])
        >>> df.select(to_timestamp(col("a"), lit("YYYY-MM-DD HH24:MI:SS.FF3")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(2020, 5, 1, 13, 11, 20))]

    Another option is to convert dates into timestamps

    Example::
        >>> import datetime
        >>> df = session.createDataFrame([datetime.datetime(2022, 12, 25, 13, 59, 38, 467)], schema=["a"])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(2022, 12, 25, 13, 59, 38, 467))]
        >>> df = session.createDataFrame([datetime.date(2023, 3, 1)], schema=["a"])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(2023, 3, 1, 0, 0))]

    Integers can be converted into a timestamp as well, by providing optionally a scale as an integer as lined out in
    <https://doc.clickzetta.com/>. Currently Zettapark does support
    integers in the range of an 8-byte signed integer only.

    Example::
        >>> df = session.createDataFrame([20, 31536000000], schema=['a'])
        >>> df.select(to_timestamp(col("a"))).collect()
        [Row(TO_TIMESTAMP("A")=datetime.datetime(1970, 1, 1, 0, 0, 20)), Row(TO_TIMESTAMP("A")=datetime.datetime(2969, 5, 3, 0, 0))]
        >>> df.select(to_timestamp(col("a"), lit(9))).collect()
        [Row(TO_TIMESTAMP("A", 9)=datetime.datetime(1970, 1, 1, 0, 0)), Row(TO_TIMESTAMP("A", 9)=datetime.datetime(1970, 1, 1, 0, 0, 31, 536000))]

    Larger numbers stored in a string can be also converted via this approach

    Example::
        >>> df = session.createDataFrame(['20', '31536000000', '31536000000000', '31536000000000000'], schema=['a'])
        >>> df.select(to_timestamp(col("a")).as_("ans")).collect()
        [Row(ANS=datetime.datetime(1970, 1, 1, 0, 0, 20)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0)), Row(ANS=datetime.datetime(1971, 1, 1, 0, 0))]
    """
    c = _to_col_if_str(e, "to_timestamp")
    return (
        builtin("to_timestamp")(c, fmt)
        if fmt is not None
        else builtin("to_timestamp")(c)
    )


def from_utc_timestamp(e: ColumnOrName, tz: ColumnOrLiteral) -> Column:
    """Interprets an input expression as a UTC timestamp and converts it to the given time zone.

    Note:
        Time zone names are case-sensitive.
        ClickZetta does not support the majority of timezone abbreviations (e.g. PDT, EST, etc.). Instead you can
        specify a time zone name or a link name from release 2021a of the IANA Time Zone Database (e.g.
        America/Los_Angeles, Europe/London, UTC, Etc/GMT, etc.).
        See the following for more information:
        <https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab>
        <https://data.iana.org/time-zones/tzdb-2021a/backward>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['t'])
        >>> df.select(from_utc_timestamp(col("t"), "America/Los_Angeles").alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 30, 17, 2, 3, 4000))]

    Example::
        >>> df = session.create_dataframe([('2019-01-31 01:02:03.004', "America/Los_Angeles")], schema=['t', 'tz'])
        >>> df.select(from_utc_timestamp(col("t"), col("tz")).alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 30, 17, 2, 3, 4000))]
    """
    c = _to_col_if_str(e, "from_utc_timestamp")
    tz_c = _to_col_if_lit(tz, "from_utc_timestamp")
    return builtin("convert_timezone")("UTC", tz_c, c)


def to_utc_timestamp(e: ColumnOrName, tz: ColumnOrLiteral) -> Column:
    """Interprets an input expression as a timestamp and converts from given time zone to UTC.

    Note:
        Time zone names are case-sensitive.
        ClickZetta does not support the majority of timezone abbreviations (e.g. PDT, EST, etc.). Instead you can
        specify a time zone name or a link name from release 2021a of the IANA Time Zone Database (e.g.
        America/Los_Angeles, Europe/London, UTC, Etc/GMT, etc.).
        See the following for more information:
        <https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab>
        <https://data.iana.org/time-zones/tzdb-2021a/backward>

    Example::
        >>> df = session.create_dataframe(['2019-01-31 01:02:03.004'], schema=['t'])
        >>> df.select(to_utc_timestamp(col("t"), "America/Los_Angeles").alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 9, 2, 3, 4000))]

    Example::
        >>> df = session.create_dataframe([('2019-01-31 01:02:03.004', "America/Los_Angeles")], schema=['t', 'tz'])
        >>> df.select(to_utc_timestamp(col("t"), col("tz")).alias("ans")).collect()
        [Row(ANS=datetime.datetime(2019, 1, 31, 9, 2, 3, 4000))]
    """
    c = _to_col_if_str(e, "to_utc_timestamp")
    tz_c = _to_col_if_lit(tz, "to_utc_timestamp")
    return builtin("convert_timezone")(tz_c, "UTC", c)


def to_date(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """Converts an input expression into a date.

    Example::

        >>> df = session.create_dataframe(['2013-05-17', '2013-05-17'], schema=['a'])
        >>> df.select(to_date(col('a')).as_('ans')).collect()
        [Row(ANS=datetime.date(2013, 5, 17)), Row(ANS=datetime.date(2013, 5, 17))]

        >>> df = session.create_dataframe(['31536000000000', '71536004000000'], schema=['a'])
        >>> df.select(to_date(col('a')).as_('ans')).collect()
        [Row(ANS=datetime.date(1971, 1, 1)), Row(ANS=datetime.date(1972, 4, 7))]

    """
    c = _to_col_if_str(e, "to_date")
    if fmt is None:
        return builtin("date")(c)
    return builtin("to_date")(c, fmt)


def current_timestamp() -> Column:
    """Returns the current timestamp for the system.

    Example:
        >>> import datetime
        >>> result = session.create_dataframe([1]).select(current_timestamp()).collect()
        >>> assert isinstance(result[0]["CURRENT_TIMESTAMP()"], datetime.datetime)
    """

    return builtin("current_timestamp")()


def current_date() -> Column:
    """Returns the current date for the system.

    Example:
        >>> import datetime
        >>> result = session.create_dataframe([1]).select(current_date()).collect()
        >>> assert isinstance(result[0]["CURRENT_DATE()"], datetime.date)
    """
    return builtin("current_date")()


# def current_time() -> Column:
#     """Returns the current time for the system.

#     Example:
#         >>> import datetime
#         >>> result = session.create_dataframe([1]).select(current_time()).collect()
#         >>> assert isinstance(result[0]["CURRENT_TIME()"], datetime.time)
#     """
#     return builtin("current_time")()


def hour(e: ColumnOrName) -> Column:
    """
    Extracts the hour from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(hour("a")).collect()
        [Row(HOUR("A")=13), Row(HOUR("A")=1)]
    """
    c = _to_col_if_str(e, "hour")
    return builtin("hour")(c)


def last_day(expr: ColumnOrName, part: Optional[ColumnOrName] = None) -> Column:
    """
    Returns the last day of the specified date part for a date or timestamp.
    Commonly used to return the last day of the month for a date or timestamp.

    Args:
        expr: The array column
        part: The date part used to compute the last day of the given array column, default is "MONTH".
            Valid values are "YEAR", "MONTH", "QUARTER", "WEEK" or any of their supported variations.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(last_day("a")).collect()
        [Row(LAST_DAY("A")=datetime.date(2020, 5, 31)), Row(LAST_DAY("A")=datetime.date(2020, 8, 31))]
        >>> df.select(last_day("a", "YEAR")).collect()
        [Row(LAST_DAY("A", "YEAR")=datetime.date(2020, 12, 31)), Row(LAST_DAY("A", "YEAR")=datetime.date(2020, 12, 31))]
    """
    expr_col = _to_col_if_str(expr, "last_day")
    if part is None:
        # Ensure we do not change the column name
        return builtin("last_day")(expr_col)

    part_col = _to_col_if_str(part, "last_day")
    return builtin("last_day")(expr_col, part_col)


def minute(e: ColumnOrName) -> Column:
    """
    Extracts the minute from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(minute("a")).collect()
        [Row(MINUTE("A")=11), Row(MINUTE("A")=30)]
    """
    c = _to_col_if_str(e, "minute")
    return builtin("minute")(c)


def next_day(date: ColumnOrName, day_of_week: ColumnOrLiteral) -> Column:
    """
    Returns the date of the first specified DOW (day of week) that occurs after the input date.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     (datetime.date.fromisoformat("2020-08-01"), "mo"),
        ...     (datetime.date.fromisoformat("2020-12-01"), "we"),
        ... ], schema=["a", "b"])
        >>> df.select(next_day("a", col("b"))).collect()
        [Row(NEXT_DAY("A", "B")=datetime.date(2020, 8, 3)), Row(NEXT_DAY("A", "B")=datetime.date(2020, 12, 2))]
        >>> df.select(next_day("a", "fr")).collect()
        [Row(NEXT_DAY("A", 'FR')=datetime.date(2020, 8, 7)), Row(NEXT_DAY("A", 'FR')=datetime.date(2020, 12, 4))]
    """
    c = _to_col_if_str(date, "next_day")
    return builtin("next_day")(c, Column._to_expr(day_of_week))


def previous_day(date: ColumnOrName, day_of_week: ColumnOrLiteral) -> Column:
    """
    Returns the date of the first specified DOW (day of week) that occurs before the input date.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     (datetime.date.fromisoformat("2020-08-01"), "mo"),
        ...     (datetime.date.fromisoformat("2020-12-01"), "we"),
        ... ], schema=["a", "b"])
        >>> df.select(previous_day("a", col("b"))).collect()
        [Row(PREVIOUS_DAY("A", "B")=datetime.date(2020, 7, 27)), Row(PREVIOUS_DAY("A", "B")=datetime.date(2020, 11, 25))]
        >>> df.select(previous_day("a", "fr")).collect()
        [Row(PREVIOUS_DAY("A", 'FR')=datetime.date(2020, 7, 31)), Row(PREVIOUS_DAY("A", 'FR')=datetime.date(2020, 11, 27))]
    """
    c = _to_col_if_str(date, "previous_day")
    return builtin("previous_day")(c, Column._to_expr(day_of_week))


def second(e: ColumnOrName) -> Column:
    """
    Extracts the second from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(second("a")).collect()
        [Row(SECOND("A")=20), Row(SECOND("A")=5)]
    """
    c = _to_col_if_str(e, "second")
    return builtin("second")(c)


def month(e: ColumnOrName) -> Column:
    """
    Extracts the month from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(month("a")).collect()
        [Row(MONTH("A")=5), Row(MONTH("A")=8)]
    """
    c = _to_col_if_str(e, "month")
    return builtin("month")(c)


def monthname(e: ColumnOrName) -> Column:
    """
    Extracts the three-letter month name from the specified date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(monthname("a")).collect()
        [Row(MONTHNAME("A")='May'), Row(MONTHNAME("A")='Aug')]
    """
    c = _to_col_if_str(e, "monthname")
    return builtin("monthname")(c)


def quarter(e: ColumnOrName) -> Column:
    """
    Extracts the quarter from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(quarter("a")).collect()
        [Row(QUARTER("A")=2), Row(QUARTER("A")=3)]
    """
    c = _to_col_if_str(e, "quarter")
    return builtin("quarter")(c)


def year(e: ColumnOrName) -> Column:
    """
    Extracts the year from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ], schema=["a"])
        >>> df.select(year("a")).collect()
        [Row(YEAR("A")=2020), Row(YEAR("A")=2020)]
    """
    c = _to_col_if_str(e, "year")
    return builtin("year")(c)


def sysdate() -> Column:
    """
    Returns the current timestamp for the system, but in the UTC time zone.

    Example::

        >>> df = session.create_dataframe([1], schema=["a"])
        >>> df.select(sysdate()).collect() is not None
        True
    """
    return builtin("sysdate")()


def months_between(date1: ColumnOrName, date2: ColumnOrName) -> Column:
    """
    Returns the number of months between two DATE or TIMESTAMP values.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([[
        ...     datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f"),
        ...     datetime.datetime.strptime("2020-08-21 01:30:05.000", "%Y-%m-%d %H:%M:%S.%f")
        ... ]], schema=["a", "b"])
        >>> df.select(months_between("a", "b")).collect()
        [Row(MONTHS_BETWEEN("A", "B")=Decimal('-3.629452'))]
    """
    c1 = _to_col_if_str(date1, "months_between")
    c2 = _to_col_if_str(date2, "months_between")
    return builtin("timestampdiff")(Literal("month", identity=True), c1, c2)


def arrays_overlap(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Compares whether two ARRAYs have at least one element in common. Returns TRUE
    if there is at least one element in common; otherwise returns FALSE. The function
    is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row([1, 2], [1, 3]), Row([1, 2], [3, 4])], schema=["a", "b"])
        >>> df.select(arrays_overlap("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |True      |
        |False     |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "arrays_overlap")
    a2 = _to_col_if_str(array2, "arrays_overlap")
    return builtin("arrays_overlap")(a1, a2)


def array_distinct(col: ColumnOrName):
    """The function excludes any duplicate elements that are present in the input ARRAY.
    The function is not guaranteed to return the elements in the ARRAY in a specific order.
    The function is NULL safe, which means that it treats NULLs as known values when identifying duplicate elements.

    Args:
        col: The array column

    Returns:
        Returns a new ARRAY that contains only the distinct elements from the input ARRAY.

    Example::

        >>> from clickzetta.zettapark.functions import array_construct,array_distinct,lit
        >>> df = session.createDataFrame([["1"]], ["A"])
        >>> df = df.withColumn("array", array_construct(lit(1), lit(1), lit(1), lit(2), lit(3), lit(2), lit(2)))
        >>> df.withColumn("array_d", array_distinct("ARRAY")).show()
        -----------------------------
        |"A"  |"ARRAY"  |"ARRAY_D"  |
        -----------------------------
        |1    |[        |[          |
        |     |  1,     |  1,       |
        |     |  1,     |  2,       |
        |     |  1,     |  3        |
        |     |  2,     |]          |
        |     |  3,     |           |
        |     |  2,     |           |
        |     |  2      |           |
        |     |]        |           |
        -----------------------------
        <BLANKLINE>
    """
    col = _to_col_if_str(col, "array_distinct")
    return builtin("array_distinct")(col)


def array_intersection(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Returns an array that contains the matching elements in the two input arrays.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    Args:
        array1: An ARRAY that contains elements to be compared.
        array2: An ARRAY that contains elements to be compared.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row([1, 2], [1, 3])], schema=["a", "b"])
        >>> df.select(array_intersection("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "array_intersection")
    a2 = _to_col_if_str(array2, "array_intersection")
    return builtin("array_intersect")(a1, a2)


def array_except(
    source_array: ColumnOrName,
    array_of_elements_to_exclude: ColumnOrName,
    allow_duplicates=True,
) -> Column:
    """Returns a new ARRAY that contains the elements from one input ARRAY that are not in another input ARRAY.

    The function is NULL-safe, meaning it treats NULLs as known values for comparing equality.

    When allow_duplicates is set to True (default), this function is the same as the ClickZetta ARRAY_EXCEPT semantic:

    This function compares arrays by using multi-set semantics (sometimes called “bag semantics”). If source_array
    includes multiple copies of a value, the function only removes the number of copies of that value that are specified
    in array_of_elements_to_exclude.

    For example, if source_array contains 5 elements with the value 'A' and array_of_elements_to_exclude contains 2
    elements with the value 'A', the returned array contains 3 elements with the value 'A'.

    When allow_duplicates is set to False:

    This function compares arrays by using set semantics. Specifically, it will first do an element deduplication
    for both arrays, and then compute the array_except result.

    For example, if source_array contains 5 elements with the value 'A' and array_of_elements_to_exclude contains 2
    elements with the value 'A', the returned array is empty.

    Args:
        source_array: An array that contains elements to be included in the new ARRAY.
        array_of_elements_to_exclude: An array that contains elements to be excluded from the new ARRAY.
        allow_duplicates: If True, we use multi-set semantic. Otherwise use set semantic.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(["A", "B"], ["B", "C"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B", "B", "B", "C"], ["B"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  "B",    |
        |  "B",    |
        |  "C"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", None, None], ["B", None])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  null    |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row([{'a': 1, 'b': 2}, 1], [{'a': 1, 'b': 2}, 3])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B"], None)], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |NULL      |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B"], ["B", "C"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", "B", "B", "B", "C"], ["B"])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A",    |
        |  "C"     |
        |]         |
        ------------
        <BLANKLINE>
        >>> df = session.create_dataframe([Row(["A", None, None], ["B", None])], schema=["source_array", "array_of_elements_to_exclude"])
        >>> df.select(array_except("source_array", "array_of_elements_to_exclude", False).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  "A"     |
        |]         |
        ------------
        <BLANKLINE>
    """
    array1 = _to_col_if_str(source_array, "array_except")
    array2 = _to_col_if_str(array_of_elements_to_exclude, "array_except")
    if allow_duplicates:
        return builtin("array_except")(array1, array2)
    return builtin("array_except")(
        builtin("array_distinct")(array1), builtin("array_distinct")(array2)
    )


def array_min(array: ColumnOrName) -> Column:
    """Collection function: returns the minimum value of the array.

    Examples::
        >>> df = session.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
        >>> df.select(array_min(df.data).alias('min')).collect()
        [Row(min=1), Row(min=-1)]
    """
    array = _to_col_if_str(array, "array_min")
    return builtin("array_min")(array)


def array_max(array: ColumnOrName) -> Column:
    """Collection function: returns the maximum value of the array.

    Examples::
        >>> df = session.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
        >>> df.select(array_max(df.data).alias('max')).collect()
        [Row(max=3), Row(max=10)]
    """
    array = _to_col_if_str(array, "array_max")
    return builtin("array_max")(array)


def array_sort(array: ColumnOrName) -> Column:
    """Returns rows of array column in sorted order. Users can choose the sort order and decide where to keep null elements.

    Args:
        array: name of the column or column element which describes the column
        sort_ascending: Boolean that decides if array elements are sorted in ascending order.
            Defaults to True.
        nulls_first: Boolean that decides if SQL null elements will be placed in the beginning
            of the array. Note that this does not affect JSON null. Defaults to False.

    Examples::
        Behavior with SQL nulls:
            >>> df = session.sql("select array_construct(20, 0, null, 10) as A")
            >>> df.select(array_sort(df.a).as_("sorted_a")).show()
            ---------------
            |"SORTED_A"   |
            ---------------
            |[            |
            |  0,         |
            |  10,        |
            |  20,        |
            |  undefined  |
            |]            |
            ---------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False).as_("sorted_a")).show()
            ---------------
            |"SORTED_A"   |
            ---------------
            |[            |
            |  20,        |
            |  10,        |
            |  0,         |
            |  undefined  |
            |]            |
            ---------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False, True).as_("sorted_a")).show()
            ----------------
            |"SORTED_A"    |
            ----------------
            |[             |
            |  undefined,  |
            |  20,         |
            |  10,         |
            |  0           |
            |]             |
            ----------------
            <BLANKLINE>

        Behavior with JSON nulls:
            >>> df = session.create_dataframe([[[20, 0, None, 10]]], schema=["a"])
            >>> df.select(array_sort(df.a, False, False).as_("sorted_a")).show()
            --------------
            |"SORTED_A"  |
            --------------
            |[           |
            |  null,     |
            |  20,       |
            |  10,       |
            |  0         |
            |]           |
            --------------
            <BLANKLINE>
            >>> df.select(array_sort(df.a, False, True).as_("sorted_a")).show()
            --------------
            |"SORTED_A"  |
            --------------
            |[           |
            |  null,     |
            |  20,       |
            |  10,       |
            |  0         |
            |]           |
            --------------
            <BLANKLINE>

    See Also:
        - https://doc.clickzetta.com/
        - :func:`~clickzetta.zettapark.functions.sort_array` which is an alias of :meth:`~clickzetta.zettapark.functions.array_sort`.
    """
    array = _to_col_if_str(array, "array_sort")
    return builtin("array_sort")(array)


# def array_generate_range(
#     start: ColumnOrName, stop: ColumnOrName, step: Optional[ColumnOrName] = None
# ) -> Column:
#     """Generate a range of integers from `start` to `stop`, incrementing by `step`.
#     If `step` is not set, incrementing by 1.

#     Args:
#         start: the column that contains the integer to start with (inclusive).
#         stop: the column that contains the integer to stop (exclusive).
#         step: the column that contains the integer to increment.

#     Example::
#         >>> from clickzetta.zettapark import Row
#         >>> df1 = session.create_dataframe([(-2, 2)], ["a", "b"])
#         >>> df1.select(array_generate_range("a", "b").alias("result")).show()
#         ------------
#         |"RESULT"  |
#         ------------
#         |[         |
#         |  -2,     |
#         |  -1,     |
#         |  0,      |
#         |  1       |
#         |]         |
#         ------------
#         <BLANKLINE>
#         >>> df2 = session.create_dataframe([(4, -4, -2)], ["a", "b", "c"])
#         >>> df2.select(array_generate_range("a", "b", "c").alias("result")).show()
#         ------------
#         |"RESULT"  |
#         ------------
#         |[         |
#         |  4,      |
#         |  2,      |
#         |  0,      |
#         |  -2      |
#         |]         |
#         ------------
#         <BLANKLINE>
#     """
#     start_col = _to_col_if_str(start, "array_generate_range")
#     stop_col = _to_col_if_str(stop, "array_generate_range")
#     if step is None:
#         return builtin("array_generate_range")(start_col, stop_col)
#     step_col = _to_col_if_str(step, "array_generate_range")
#     return builtin("array_generate_range")(start_col, stop_col, step_col)


def sequence(
    start: ColumnOrName, stop: ColumnOrName, step: Optional[ColumnOrName] = None
) -> Column:
    """Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
    If `step` is not set, incrementing by 1 if start is less than or equal to stop, otherwise -1.

    Args:
        start: the column that contains the integer to start with (inclusive).
        stop: the column that contains the integer to stop (inclusive).
        step: the column that contains the integer to increment.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df1 = session.create_dataframe([(-2, 2)], ["a", "b"])
        >>> df1.select(sequence("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  -2,     |
        |  -1,     |
        |  0,      |
        |  1,      |
        |  2       |
        |]         |
        ------------
        <BLANKLINE>
        >>> df2 = session.create_dataframe([(4, -4, -2)], ["a", "b", "c"])
        >>> df2.select(sequence("a", "b", "c").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  4,      |
        |  2,      |
        |  0,      |
        |  -2,     |
        |  -4      |
        |]         |
        ------------
        <BLANKLINE>
    """
    start_col = _to_col_if_str(start, "sequence")
    stop_col = _to_col_if_str(stop, "sequence")
    if step is None:
        return builtin("sequence")(start_col, stop_col)
    step_col = _to_col_if_str(step, "sequence")
    return builtin("sequence")(start_col, stop_col, step_col)


def date_add(col: ColumnOrName, num_of_days: Union[ColumnOrName, int]):
    """
    Adds a number of days to a date column.

    Args:
        col: The column to add to.
        num_of_days: The number of days to add.

    Example::

        >>> from clickzetta.zettapark.functions import date_add, to_date
        >>> df = session.createDataFrame([("1976-01-06")], ["date"])
        >>> df = df.withColumn("date", to_date("date"))
        >>> res = df.withColumn("date", date_add("date", 4)).show()
        --------------
        |"DATE"      |
        --------------
        |1976-01-10  |
        --------------
        <BLANKLINE>
    """
    # Convert the input to a column if it is a string
    col = _to_col_if_str(col, "date_add")
    num_of_days = (
        lit(num_of_days)
        if isinstance(num_of_days, int)
        else _to_col_if_str(num_of_days, "date_add")
    )
    # Return the dateadd function with the column and number of days
    return builtin("add_days")(col, num_of_days)


def date_sub(col: ColumnOrName, num_of_days: Union[ColumnOrName, int]):
    """
    Subtracts a number of days from a date column.

    Args:
        col: The column to subtract from.
        num_of_days: The number of days to subtract.

    Example::

        >>> from clickzetta.zettapark.functions import date_sub, to_date
        >>> df = session.createDataFrame([("1976-01-06")], ["date"])
        >>> df = df.withColumn("date", to_date("date"))
        >>> df.withColumn("date", date_sub("date", 2)).show()
        --------------
        |"DATE"      |
        --------------
        |1976-01-04  |
        --------------
        <BLANKLINE>
    """
    # Convert the input parameters to the appropriate type
    col = _to_col_if_str(col, "date_sub")
    num_of_days = (
        lit(num_of_days)
        if isinstance(num_of_days, int)
        else _to_col_if_str(num_of_days, "date_sub")
    )
    # Return the date column with the number of days subtracted
    # return dateadd("day", -1 * num_of_days, col)
    return builtin("sub_days")(col, num_of_days)


@overload
def datediff(
    timeunit: str, col1: ColumnOrName, col2: ColumnOrName
) -> Column: ...  # pragma: no cover


@overload
def datediff(col1: ColumnOrName, col2: ColumnOrName) -> Column: ...  # pragma: no cover


def datediff(*args) -> Column:
    if len(args) == 2:
        col1 = _to_col_if_str(args[0], "datediff")
        col2 = _to_col_if_str(args[1], "datediff")
        return builtin("datediff")(col1, col2)
    elif len(args) == 3:
        timeunit = args[0]
        if not isinstance(timeunit, str):
            raise ValueError("timeunit must be a string")
        col1 = _to_col_if_str(args[1], "datediff")
        col2 = _to_col_if_str(args[2], "datediff")
        return builtin("datediff")(Literal(timeunit, identity=True), col1, col2)
    else:
        raise ValueError("datediff takes 2 or 3 arguments")


def trunc(e: ColumnOrName, scale: Union[ColumnOrName, int, float] = 0) -> Column:
    """Rounds the input expression down to the nearest (or equal) integer closer to zero,
    or to the nearest equal or smaller value with the specified number of
    places after the decimal point.

    Example::

        >>> df = session.createDataFrame([-1.0, -0.9, -.5, -.2, 0.0, 0.2, 0.5, 0.9, 1.1, 3.14159], schema=["a"])
        >>> df.select(trunc(col("a"))).collect()
        [Row(TRUNC("A", 0)=-1.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=0.0), Row(TRUNC("A", 0)=1.0), Row(TRUNC("A", 0)=3.0)]

        >>> df = session.createDataFrame([-1.323, 4.567, 0.0123], schema=["a"])
        >>> df.select(trunc(col("a"), lit(0))).collect()
        [Row(TRUNC("A", 0)=-1.0), Row(TRUNC("A", 0)=4.0), Row(TRUNC("A", 0)=0.0)]
        >>> df.select(trunc(col("a"), lit(1))).collect()
        [Row(TRUNC("A", 1)=-1.3), Row(TRUNC("A", 1)=4.5), Row(TRUNC("A", 1)=0.0)]
        >>> df.select(trunc(col("a"), lit(2))).collect()
        [Row(TRUNC("A", 2)=-1.32), Row(TRUNC("A", 2)=4.56), Row(TRUNC("A", 2)=0.01)]

    Note that the function ``trunc`` is overloaded with ``date_trunc`` for datetime types. It allows to round datetime
    objects.

    Example::

        >>> import datetime
        >>> df = session.createDataFrame([datetime.date(2022, 12, 25), datetime.date(2022, 1, 10), datetime.date(2022, 7, 7)], schema=["a"])
        >>> df.select(trunc(col("a"), lit("QUARTER"))).collect()
        [Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 10, 1)), Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 1, 1)), Row(TRUNC("A", 'QUARTER')=datetime.date(2022, 7, 1))]

        >>> df = session.createDataFrame([datetime.datetime(2022, 12, 25, 13, 59, 38, 467)], schema=["a"])
        >>> df.collect()
        [Row(A=datetime.datetime(2022, 12, 25, 13, 59, 38, 467))]
        >>> df.select(trunc(col("a"), lit("MINUTE"))).collect()
        [Row(TRUNC("A", 'MINUTE')=datetime.datetime(2022, 12, 25, 13, 59))]

    """
    c = _to_col_if_str(e, "trunc")
    scale_col = (
        lit(scale)
        if isinstance(scale, (int, float))
        else _to_col_if_str(scale, "trunc")
    )
    return builtin("trunc")(c, scale_col)


def dateadd(part: str, col1: ColumnOrName, col2: ColumnOrName) -> Column:
    """Adds the specified value for the specified date or time part to date or time expr.

    `Supported date and time parts <https://doc.clickzetta.com/>`_

    Example::

        >>> # add one year on dates
        >>> import datetime
        >>> date_df = session.create_dataframe([[datetime.date(2020, 1, 1)]], schema=["date_col"])
        >>> date_df.select(dateadd("year", lit(1), col("date_col")).alias("year_added")).show()
        ----------------
        |"YEAR_ADDED"  |
        ----------------
        |2021-01-01    |
        ----------------
        <BLANKLINE>
        >>> date_df.select(dateadd("month", lit(1), col("date_col")).alias("month_added")).show()
        -----------------
        |"MONTH_ADDED"  |
        -----------------
        |2020-02-01     |
        -----------------
        <BLANKLINE>
        >>> date_df.select(dateadd("day", lit(1), col("date_col")).alias("day_added")).show()
        ---------------
        |"DAY_ADDED"  |
        ---------------
        |2020-01-02   |
        ---------------
        <BLANKLINE>

    Args:
        part: The time part to use for the addition
        col1: The first timestamp column or addend in the dateadd
        col2: The second timestamp column or the addend in the dateadd
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string or year、month、day")
    c1 = _to_col_if_str(col1, "dateadd")
    c2 = _to_col_if_str(col2, "dateadd")

    if isinstance(c2, Column) and isinstance(c2._expression, Literal):
        c1, c2 = c2, c1
    return builtin("dateadd")(Literal(part, identity=True), c1, c2)


def date_part(part: str, e: ColumnOrName) -> Column:
    """
    Extracts the specified date or time part from a date, time, or timestamp. See
    `DATE_PART <https://doc.clickzetta.com/>`_ for details.

    Args:
        part: The time part to use for the addition.
        e: The column expression of a date, time, or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([[datetime.datetime(2023, 1, 1, 1, 1, 1)]], schema=["ts_col"])
        >>> df.select(date_part("year", col("ts_col")).alias("year"), date_part("epoch_second", col("ts_col")).alias("epoch_second")).show()
        ---------------------------
        |"YEAR"  |"EPOCH_SECOND"  |
        ---------------------------
        |2023    |1672534861      |
        ---------------------------
        <BLANKLINE>
    """
    if not isinstance(part, str):
        raise ValueError("part must be a string")
    c = _to_col_if_str(e, "date_part")
    return builtin("date_part")(part, c)


def date_from_parts(
    y: Union[ColumnOrName, int],
    m: Union[ColumnOrName, int],
    d: Union[ColumnOrName, int],
) -> Column:
    """
    Creates a date from individual numeric components that represent the year, month, and day of the month.

    Example::
        >>> df = session.create_dataframe([[2022, 4, 1]], schema=["year", "month", "day"])
        >>> df.select(date_from_parts("year", "month", "day")).collect()
        [Row(DATE_FROM_PARTS("YEAR", "MONTH", "DAY")=datetime.date(2022, 4, 1))]
        >>> session.table("dual").select(date_from_parts(2022, 4, 1)).collect()
        [Row(DATE_FROM_PARTS(2022, 4, 1)=datetime.date(2022, 4, 1))]
    """
    y_col = _to_col_if_str_or_int(y, "date_from_parts")
    m_col = _to_col_if_str_or_int(m, "date_from_parts")
    d_col = _to_col_if_str_or_int(d, "date_from_parts")
    return builtin("date_from_parts")(y_col, m_col, d_col)


def date_trunc(part: ColumnOrName, expr: ColumnOrName) -> Column:
    """
    Truncates a DATE, TIME, or TIMESTAMP to the specified precision.

    Note that truncation is not the same as extraction. For example:
    - Truncating a timestamp down to the quarter returns the timestamp corresponding to midnight of the first day of the
    quarter for the input timestamp.
    - Extracting the quarter date part from a timestamp returns the quarter number of the year in the timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(date_trunc("YEAR", "a"), date_trunc("MONTH", "a"), date_trunc("DAY", "a")).collect()
        [Row(DATE_TRUNC("YEAR", "A")=datetime.datetime(2020, 1, 1, 0, 0), DATE_TRUNC("MONTH", "A")=datetime.datetime(2020, 5, 1, 0, 0), DATE_TRUNC("DAY", "A")=datetime.datetime(2020, 5, 1, 0, 0))]
        >>> df.select(date_trunc("HOUR", "a"), date_trunc("MINUTE", "a"), date_trunc("SECOND", "a")).collect()
        [Row(DATE_TRUNC("HOUR", "A")=datetime.datetime(2020, 5, 1, 13, 0), DATE_TRUNC("MINUTE", "A")=datetime.datetime(2020, 5, 1, 13, 11), DATE_TRUNC("SECOND", "A")=datetime.datetime(2020, 5, 1, 13, 11, 20))]
        >>> df.select(date_trunc("QUARTER", "a")).collect()
        [Row(DATE_TRUNC("QUARTER", "A")=datetime.datetime(2020, 4, 1, 0, 0))]
    """
    part_col = _to_col_if_str(part, "date_trunc")
    expr_col = _to_col_if_str(expr, "date_trunc")
    return builtin("date_trunc")(part_col, expr_col)


def dayname(e: ColumnOrName) -> Column:
    """
    Extracts the three-letter day-of-week name from the specified date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayname("a")).collect()
        [Row(DAYNAME("A")='Fri')]
    """
    c = _to_col_if_str(e, "dayname")
    return builtin("dayname")(c)


def dayofmonth(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the month from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofmonth("a")).collect()
        [Row(DAYOFMONTH("A")=1)]
    """
    c = _to_col_if_str(e, "dayofmonth")
    return builtin("dayofmonth")(c)


def dayofweek(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the week from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofweek("a")).collect()
        [Row(DAYOFWEEK("A")=5)]
    """
    c = _to_col_if_str(e, "dayofweek")
    return builtin("dayofweek")(c)


def dayofyear(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding day (number) of the year from a date or timestamp.

    Example::
        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(dayofyear("a")).collect()
        [Row(DAYOFYEAR("A")=122)]
    """
    c = _to_col_if_str(e, "dayofyear")
    return builtin("dayofyear")(c)


def _columns_from_timestamp_parts(
    func_name: str, *args: Union[ColumnOrName, int]
) -> Tuple[Column, ...]:
    if len(args) == 3:
        year_ = _to_col_if_str_or_int(args[0], func_name)
        month = _to_col_if_str_or_int(args[1], func_name)
        day = _to_col_if_str_or_int(args[2], func_name)
        return year_, month, day
    elif len(args) == 6:
        year_ = _to_col_if_str_or_int(args[0], func_name)
        month = _to_col_if_str_or_int(args[1], func_name)
        day = _to_col_if_str_or_int(args[2], func_name)
        hour = _to_col_if_str_or_int(args[3], func_name)
        minute = _to_col_if_str_or_int(args[4], func_name)
        second = _to_col_if_str_or_int(args[5], func_name)
        return year_, month, day, hour, minute, second
    else:
        # Should never happen since we only use this internally
        raise ValueError(f"Incorrect number of args passed to {func_name}")


def _timestamp_from_parts_internal(
    func_name: str, *args: Union[ColumnOrName, int], **kwargs: Union[ColumnOrName, int]
) -> Tuple[Column, ...]:
    num_args = len(args)
    if num_args == 2:
        # expression mode
        date_expr = _to_col_if_str(args[0], func_name)
        time_expr = _to_col_if_str(args[1], func_name)
        return date_expr, time_expr
    elif 6 <= num_args <= 8:
        # parts mode
        y, m, d, h, min_, s = _columns_from_timestamp_parts(func_name, *args[:6])
        ns_arg = args[6] if num_args >= 7 else kwargs.get("nanoseconds")
        # Timezone is only accepted in timestamp_from_parts function
        tz_arg = args[7] if num_args == 8 else kwargs.get("timezone")
        if tz_arg is not None and func_name != "timestamp_from_parts":
            raise ValueError(f"{func_name} does not accept timezone as an argument")
        ns = None if ns_arg is None else _to_col_if_str_or_int(ns_arg, func_name)
        tz = None if tz_arg is None else _to_col_if_sql_expr(tz_arg, func_name)
        if ns is not None and tz is not None:
            return y, m, d, h, min_, s, ns, tz
        elif ns is not None:
            return y, m, d, h, min_, s, ns
        elif tz is not None:
            # We need to fill in nanoseconds as 0 to make the sql function work
            return y, m, d, h, min_, s, lit(0), tz
        else:
            return y, m, d, h, min_, s
    else:
        raise ValueError(
            f"{func_name} expected 2 or 6 required arguments, got {num_args}"
        )


def time_from_parts(
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
) -> Column:
    """
    Creates a time from individual numeric components.

    TIME_FROM_PARTS is typically used to handle values in "normal" ranges (e.g. hours 0-23, minutes 0-59),
    but it also handles values from outside these ranges. This allows, for example, choosing the N-th minute
    in a day, which can be used to simplify some computations.

    Example::

        >>> df = session.create_dataframe(
        ...     [[11, 11, 0, 987654321], [10, 10, 0, 987654321]],
        ...     schema=["hour", "minute", "second", "nanoseconds"],
        ... )
        >>> df.select(time_from_parts(
        ...     "hour", "minute", "second", nanoseconds="nanoseconds"
        ... ).alias("TIME_FROM_PARTS")).collect()
        [Row(TIME_FROM_PARTS=datetime.time(11, 11, 0, 987654)), Row(TIME_FROM_PARTS=datetime.time(10, 10, 0, 987654))]
    """
    h, m, s = _columns_from_timestamp_parts("time_from_parts", hour, minute, second)
    if nanoseconds:
        return builtin("time_from_parts")(
            h, m, s, _to_col_if_str_or_int(nanoseconds, "time_from_parts")
        )
    else:
        return builtin("time_from_parts")(h, m, s)


@overload
def timestamp_from_parts(
    date_expr: ColumnOrName, time_expr: ColumnOrName
) -> Column: ...  # pragma: no cover


@overload
def timestamp_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
) -> Column: ...  # pragma: no cover


def timestamp_from_parts(*args, **kwargs) -> Column:
    """
    Creates a timestamp from individual numeric components. If no time zone is in effect,
    the function can be used to create a timestamp from a date expression and a time expression.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_FROM_PARTS")).collect()
        [Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]

    Example 2::

        >>> df = session.create_dataframe(
        ...     [['2022-04-01', '11:11:00'], ['2022-03-31', '11:11:00']],
        ...     schema=["date", "time"]
        ... )
        >>> df.select(
        ...     timestamp_from_parts(to_date("date"), to_time("time")
        ... ).alias("TIMESTAMP_FROM_PARTS")).collect()
        [Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]
    """
    return builtin("timestamp_from_parts")(
        *_timestamp_from_parts_internal("timestamp_from_parts", *args, **kwargs)
    )


def timestamp_ltz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
) -> Column:
    """
    Creates a timestamp from individual numeric components.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_ltz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_LTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_LTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11, tzinfo=<DstTzInfo 'America/Los_Angeles' PDT-1 day, 17:00:00 DST>)), Row(TIMESTAMP_LTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11, tzinfo=<DstTzInfo 'America/Los_Angeles' PDT-1 day, 17:00:00 DST>))]
    """
    func_name = "timestamp_ltz_from_parts"
    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    return (
        builtin(func_name)(y, m, d, h, min_, s)
        if ns is None
        else builtin(func_name)(y, m, d, h, min_, s, ns)
    )


@overload
def timestamp_ntz_from_parts(
    date_expr: ColumnOrName, time_expr: ColumnOrName
) -> Column: ...  # pragma: no cover


@overload
def timestamp_ntz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanosecond: Optional[Union[ColumnOrName, int]] = None,
) -> Column: ...  # pragma: no cover


def timestamp_ntz_from_parts(*args, **kwargs) -> Column:
    """
    Creates a timestamp from individual numeric components. The function can be used to
    create a timestamp from a date expression and a time expression.

    Example 1::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0], [2022, 3, 31, 11, 11, 0]],
        ...     schema=["year", "month", "day", "hour", "minute", "second"],
        ... )
        >>> df.select(timestamp_ntz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second"
        ... ).alias("TIMESTAMP_NTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]

    Example 2::

        >>> df = session.create_dataframe(
        ...     [['2022-04-01', '11:11:00'], ['2022-03-31', '11:11:00']],
        ...     schema=["date", "time"]
        ... )
        >>> df.select(
        ...     timestamp_ntz_from_parts(to_date("date"), to_time("time")
        ... ).alias("TIMESTAMP_NTZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11)), Row(TIMESTAMP_NTZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11))]
    """
    return builtin("timestamp_ntz_from_parts")(
        *_timestamp_from_parts_internal("timestamp_ntz_from_parts", *args, **kwargs)
    )


def timestamp_tz_from_parts(
    year: Union[ColumnOrName, int],
    month: Union[ColumnOrName, int],
    day: Union[ColumnOrName, int],
    hour: Union[ColumnOrName, int],
    minute: Union[ColumnOrName, int],
    second: Union[ColumnOrName, int],
    nanoseconds: Optional[Union[ColumnOrName, int]] = None,
    timezone: Optional[ColumnOrLiteralStr] = None,
) -> Column:
    """
    Creates a timestamp from individual numeric components and a string timezone.

    Example::

        >>> df = session.create_dataframe(
        ...     [[2022, 4, 1, 11, 11, 0, 'America/Los_Angeles'], [2022, 3, 31, 11, 11, 0, 'America/Los_Angeles']],
        ...     schema=["year", "month", "day", "hour", "minute", "second", "timezone"],
        ... )
        >>> df.select(timestamp_tz_from_parts(
        ...     "year", "month", "day", "hour", "minute", "second", timezone="timezone"
        ... ).alias("TIMESTAMP_TZ_FROM_PARTS")).collect()
        [Row(TIMESTAMP_TZ_FROM_PARTS=datetime.datetime(2022, 4, 1, 11, 11, tzinfo=pytz.FixedOffset(-420))), Row(TIMESTAMP_TZ_FROM_PARTS=datetime.datetime(2022, 3, 31, 11, 11, tzinfo=pytz.FixedOffset(-420)))]
    """
    func_name = "timestamp_tz_from_parts"
    y, m, d, h, min_, s = _columns_from_timestamp_parts(
        func_name, year, month, day, hour, minute, second
    )
    ns = None if nanoseconds is None else _to_col_if_str_or_int(nanoseconds, func_name)
    tz = None if timezone is None else _to_col_if_sql_expr(timezone, func_name)
    if nanoseconds is not None and timezone is not None:
        return builtin(func_name)(y, m, d, h, min_, s, ns, tz)
    elif nanoseconds is not None:
        return builtin(func_name)(y, m, d, h, min_, s, ns)
    elif timezone is not None:
        return builtin(func_name)(y, m, d, h, min_, s, lit(0), tz)
    else:
        return builtin(func_name)(y, m, d, h, min_, s)


def weekofyear(e: ColumnOrName) -> Column:
    """
    Extracts the corresponding week (number) of the year from a date or timestamp.

    Example::

        >>> import datetime
        >>> df = session.create_dataframe(
        ...     [[datetime.datetime.strptime("2020-05-01 13:11:20.000", "%Y-%m-%d %H:%M:%S.%f")]],
        ...     schema=["a"],
        ... )
        >>> df.select(weekofyear("a")).collect()
        [Row(WEEKOFYEAR("A")=18)]
    """
    c = _to_col_if_str(e, "weekofyear")
    return builtin("weekofyear")(c)


def typeof(col: ColumnOrName) -> Column:
    """Return DDL-formatted type string for the data type of the input.

    Example::

        >>> df = session.create_dataframe([(1,)], ["a"])
        >>> df.select(typeof(df.a).alias('r')).collect()
        [Row(r='bigint')]

    """
    c = _to_col_if_str(col, "typeof")
    return builtin("typeof")(c)


def check_json(col: ColumnOrName) -> Column:
    """Checks the validity of a JSON document.
    If the input string is a valid JSON document or a NULL (i.e. no error would occur when
    parsing the input string), the function returns NULL.
    In case of a JSON parsing error, the function returns a string that contains the error
    message.

    Example::

        >>> df = session.create_dataframe(["{'ValidKey1': 'ValidValue1'}", "{'Malformed -- missing val':}", None], schema=['a'])
        >>> df.select(check_json(df.a)).show()
        -----------------------
        |"CHECK_JSON(""A"")"  |
        -----------------------
        |NULL                 |
        |misplaced }, pos 29  |
        |NULL                 |
        -----------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "check_json")
    return builtin("check_json")(c)


def parse_json(e: ColumnOrName) -> Column:
    """Parse the value of the specified column as a JSON string and returns the
    resulting JSON document.

    Example::

        >>> df = session.create_dataframe([['{"key": "1"}']], schema=["a"])
        >>> df.select(parse_json(df["a"]).alias("result")).show()
        ----------------
        |"RESULT"      |
        ----------------
        |{             |
        |  "key": "1"  |
        |}             |
        ----------------
        <BLANKLINE>
    """
    c = _to_col_if_str(e, "parse_json")
    return builtin("json_parse")(cast(c, StringType()))


def array_agg(col: ColumnOrName, is_distinct: bool = False) -> Column:
    """Returns the input values, pivoted into an ARRAY. If the input is empty, an empty
    ARRAY is returned.

    Example::
        >>> df = session.create_dataframe([[1], [2], [3], [1]], schema=["a"])
        >>> df.select(array_agg("a", True).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "array_agg")
    return _call_function("array_agg", is_distinct, c)


def array_append(array: ColumnOrName, element: ColumnOrName) -> Column:
    """Returns an ARRAY containing all elements from the source ARRAY as well as the new element.
    The new element is located at end of the ARRAY.

    Args:
        array: The column containing the source ARRAY.
        element: The column containing the element to be appended. The element may be of almost
            any data type. The data type does not need to match the data type(s) of the
            existing elements in the ARRAY.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3])])
        >>> df.select(array_append("a", lit(4)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_append")
    e = _to_col_if_str(element, "array_append")
    return builtin("array_append")(a, e)


def array_cat(array1: ColumnOrName, array2: ColumnOrName) -> Column:
    """Returns the concatenation of two ARRAYs.

    Args:
        array1: Column containing the source ARRAY.
        array2: Column containing the ARRAY to be appended to array1.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3], b=[4, 5])])
        >>> df.select(array_cat("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2,      |
        |  3,      |
        |  4,      |
        |  5       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a1 = _to_col_if_str(array1, "array_cat")
    a2 = _to_col_if_str(array2, "array_cat")
    return builtin("array_cat")(a1, a2)


def array_compact(array: ColumnOrName) -> Column:
    """Returns a compacted ARRAY with missing and null values removed,
    effectively converting sparse arrays into dense arrays.

    Args:
        array: Column containing the source ARRAY to be compacted

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, None, 3])])
        >>> df.select("a", array_compact("a").alias("compacted")).show()
        -------------------------
        |"A"      |"COMPACTED"  |
        -------------------------
        |[        |[            |
        |  1,     |  1,         |
        |  null,  |  3          |
        |  3      |]            |
        |]        |             |
        -------------------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_compact")
    return builtin("array_compact")(a)


def array_construct(*cols: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type.

    Example::
        >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        >>> df.select(array_construct("a", "b").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    cs = [_to_col_if_str(c, "array") for c in cols]
    return builtin("array")(*cs)


def array_construct_compact(*cols: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from zero, one, or more inputs.
    The constructed ARRAY omits any NULL input values.

    Args:
        cols: Columns containing the values (or expressions that evaluate to values). The
            values do not all need to be of the same data type.

    Example::
        >>> df = session.create_dataframe([[1, None, 2], [3, None, 4]], schema=["a", "b", "c"])
        >>> df.select(array_construct_compact("a", "b", "c").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  3,      |
        |  4       |
        |]         |
        ------------
        <BLANKLINE>
    """
    cs = [_to_col_if_str(c, "array_construct_compact") for c in cols]
    return builtin("array_construct_compact")(*cs)


def array_contains(col: ColumnOrName, value: Any) -> Column:
    """Collection function: returns null if the array is null, true if the array contains the given value, and false otherwise.

    Example::
        >>> df = session.create_dataframe([(["a", "b", "c"],), ([],)], ['data'])
        >>> df.select(array_contains(df.data, "a")).collect()
        [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
        >>> df.select(array_contains(df.data, lit("a"))).collect()
        [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    """
    c = _to_col_if_str(col, "array_contains")
    return builtin("array_contains")(c, value)


def array_insert(
    array: ColumnOrName, pos: ColumnOrName, element: ColumnOrName
) -> Column:
    """Returns an ARRAY containing all elements from the source ARRAY as well as the new element.

    Args:
        array: Column containing the source ARRAY.
        pos: Column containing a (zero-based) position in the source ARRAY.
            The new element is inserted at this position. The original element from this
            position (if any) and all subsequent elements (if any) are shifted by one position
            to the right in the resulting array (i.e. inserting at position 0 has the same
            effect as using array_prepend).
            A negative position is interpreted as an index from the back of the array (e.g.
            -1 results in insertion before the last element in the array).
        element: Column containing the element to be inserted. The new element is located at
            position pos. The relative order of the other elements from the source
            array is preserved.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row([1, 2]), Row([1, 3])], schema=["a"])
        >>> df.select(array_insert("a", lit(0), lit(10)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  10,     |
        |  1,      |
        |  2       |
        |]         |
        |[         |
        |  10,     |
        |  1,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_insert")
    p = _to_col_if_str(pos, "array_insert")
    e = _to_col_if_str(element, "array_insert")
    return builtin("array_insert")(a, p, e)


def array_position(col: ColumnOrName, value: Any) -> Column:
    """Collection function: Locates the position of the first occurrence of the given value in the given array.
    Returns null if either of the arguments are null.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row([2, 1]), Row([1, 3])], schema=["a"])
        >>> df.select(array_position(lit(1), "a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |0         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(col, "array_position")
    return builtin("array_position")(a, value)


def array_prepend(array: ColumnOrName, element: ColumnOrName) -> Column:
    """Returns an ARRAY containing the new element as well as all elements from the source ARRAY.
    The new element is positioned at the beginning of the ARRAY.

    Args:
        array Column containing the source ARRAY.
        element Column containing the element to be prepended.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3])])
        >>> df.select(array_prepend("a", lit(4)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  4,      |
        |  1,      |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_prepend")
    e = _to_col_if_str(element, "array_prepend")
    return builtin("array_prepend")(a, e)


def array_size(col: ColumnOrName) -> Column:
    """Returns the total number of elements in the array.
    The function returns null for null input.

    Example::
        >>> df = session.createDataFrame([([2, 1, 3],), (None,)], ['data'])
        >>> df.select(array_size(df.data).alias('r')).collect()
        [Row(r=3), Row(r=None)]
    """
    a = _to_col_if_str(col, "array_size")
    return builtin("array_size")(a)


def array_slice(array: ColumnOrName, from_: ColumnOrName, to: ColumnOrName) -> Column:
    """Returns an ARRAY constructed from a specified subset of elements of the input ARRAY.

    Args:
        array: Column containing the source ARRAY.
        from_: Column containing a position in the source ARRAY. The position of the first
            element is 0. Elements from positions less than this parameter are
            not included in the resulting ARRAY.
        to: Column containing a position in the source ARRAY. Elements from positions equal to
            or greater than this parameter are not included in the resulting array.

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3, 4, 5])])
        >>> df.select(array_slice("a", lit(1), lit(3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  2,      |
        |  3       |
        |]         |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_slice")
    f = _to_col_if_str(from_, "array_slice")
    t = _to_col_if_str(to, "array_slice")
    return builtin("slice")(a, f, t)


def array_to_string(array: ColumnOrName, separator: ColumnOrName) -> Column:
    """Returns an input ARRAY converted to a string by casting all values to strings (using
    TO_VARCHAR) and concatenating them (using the string from the second argument to separate
    the elements).

    Args:
        array: Column containing the ARRAY of elements to convert to a string.
        separator: Column containing the string to put between each element (e.g. a space,
            comma, or other human-readable separator).

    Example::
        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, True, "s"])])
        >>> df.select(array_to_string("a", lit(",")).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1,true,s  |
        ------------
        <BLANKLINE>
    """
    a = _to_col_if_str(array, "array_to_string")
    s = _to_col_if_str(separator, "array_to_string")
    return builtin("string")(a, s)


def array_unique_agg(col: ColumnOrName) -> Column:
    """Returns a Column containing the distinct values in the specified column col.
    The values in the Column are in no particular order, and the order is not deterministic.
    The function ignores NULL values in col.
    If col contains only NULL values or col is empty, the function returns an empty Column.

    Args:
        col: A :class:`Column` object or column name that determines the values.

    Example::
        >>> df = session.create_dataframe([[5], [2], [1], [2], [1]], schema=["a"])
        >>> df.select(array_unique_agg("a").alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |[         |
        |  5,      |
        |  2,      |
        |  1       |
        |]         |
        ------------
        <BLANKLINE>
    """
    c = _to_col_if_str(col, "array_unique_agg")
    return _call_function("collect_set_on_array", False, to_array(c))


# The following three vector functions have doctests that are disabled (">>" instead of ">>>")
# since vectors are not yet rolled out.


def vector_cosine_distance(v1: ColumnOrName, v2: ColumnOrName) -> Column:
    """Returns the cosine distance between two vectors of equal dimension and element type.

    Example::
        >> from clickzetta.zettapark.functions import vector_cosine_distance
        >> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >> df.select(vector_cosine_distance(df.a, df.b).as_("dist")).show()
        ----------------------
        |"DIST"              |
        ----------------------
        |0.9925833339709303  |
        ----------------------
    """
    v1 = _to_col_if_str(v1, "vector_cosine_distance")
    v2 = _to_col_if_str(v2, "vector_cosine_distance")
    return builtin("vector_cosine_distance")(v1, v2)


def vector_l2_distance(v1: ColumnOrName, v2: ColumnOrName) -> Column:
    """Returns the cosine distance between two vectors of equal dimension and element type.

    Example::
        >> from clickzetta.zettapark.functions import vector_l2_distance
        >> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >> df.select(vector_l2_distance(df.a, df.b).as_("dist")).show()
        ---------------------
        |"DIST"              |
        ----------------------
        |1.7320508075688772  |
        ----------------------
    """
    v1 = _to_col_if_str(v1, "vector_l2_distance")
    v2 = _to_col_if_str(v2, "vector_l2_distance")
    return builtin("vector_l2_distance")(v1, v2)


def vector_inner_product(v1: ColumnOrName, v2: ColumnOrName) -> Column:
    """Returns the inner product between two vectors of equal dimension and element type.

    Example::
        >> from clickzetta.zettapark.functions import vector_inner_product
        >> df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")
        >> df.select(vector_inner_product(df.a, df.b).as_("dist")).show()
        ----------
        |"DIST"  |
        ----------
        |20.0    |
        ----------
    """
    v1 = _to_col_if_str(v1, "vector_inner_product")
    v2 = _to_col_if_str(v2, "vector_inner_product")
    return builtin("vector_inner_product")(v1, v2)


def asc(c: ColumnOrName) -> Column:
    """Returns a Column expression with values sorted in ascending order.

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=1), Row(A=2), Row(A=3)]
    """
    c = _to_col_if_str(c, "asc")
    return c.asc()


def asc_nulls_first(c: ColumnOrName) -> Column:
    """Returns a Column expression with values sorted in ascending order
    (null values sorted before non-null values).

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc_nulls_first(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=1), Row(A=2), Row(A=3)]
    """
    c = _to_col_if_str(c, "asc_nulls_first")
    return c.asc_nulls_first()


def asc_nulls_last(c: ColumnOrName) -> Column:
    """Returns a Column expression with values sorted in ascending order
    (null values sorted after non-null values).

    Example::

        >>> df = session.create_dataframe([None, 3, 2, 1, None], schema=["a"])
        >>> df.sort(asc_nulls_last(df["a"])).collect()
        [Row(A=1), Row(A=2), Row(A=3), Row(A=None), Row(A=None)]
    """
    c = _to_col_if_str(c, "asc_nulls_last")
    return c.asc_nulls_last()


def desc(c: ColumnOrName) -> Column:
    """
    Returns a Column expression with values sorted in descending order.

    Example::

        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc(df["a"])).collect()
        [Row(A=3), Row(A=2), Row(A=1), Row(A=None), Row(A=None)]
    """
    c = _to_col_if_str(c, "desc")
    return c.desc()


def desc_nulls_first(c: ColumnOrName) -> Column:
    """
    Returns a Column expression with values sorted in descending order
    (null values sorted before non-null values).

    Example::

        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc_nulls_first(df["a"])).collect()
        [Row(A=None), Row(A=None), Row(A=3), Row(A=2), Row(A=1)]
    """
    c = _to_col_if_str(c, "desc_nulls_first")
    return c.desc_nulls_first()


def desc_nulls_last(c: ColumnOrName) -> Column:
    """
    Returns a Column expression with values sorted in descending order
    (null values sorted after non-null values).

    Example::
        >>> df = session.create_dataframe([1, 2, 3, None, None], schema=["a"])
        >>> df.sort(desc_nulls_last(df["a"])).collect()
        [Row(A=3), Row(A=2), Row(A=1), Row(A=None), Row(A=None)]
    """
    c = _to_col_if_str(c, "desc_nulls_last")
    return c.desc_nulls_last()


def cast(column: ColumnOrName, to: Union[str, DataType]) -> Column:
    """Converts a value of one data type into another data type.
    The semantics of CAST are the same as the semantics of the corresponding to datatype conversion functions.
    If the cast is not possible, a ``ZettaparkSQLException`` exception is thrown.

    Example::

        >>> from clickzetta.zettapark.types import DecimalType, IntegerType
        >>> df = session.create_dataframe([[1.5432]], schema=["d"])
        >>> df.select(cast(df.d, DecimalType(15, 2)).as_("DECIMAL"), cast(df.d, IntegerType()).as_("INT")).show()
        ---------------------
        |"DECIMAL"  |"INT"  |
        ---------------------
        |1.54       |2      |
        ---------------------
        <BLANKLINE>
    """
    c = _to_col_if_str(column, "cast")
    return c.cast(to)


def try_cast(column: ColumnOrName, to: Union[str, DataType]) -> Column:
    """A special version of CAST for a subset of data type conversions.
    It performs the same operation (i.e. converts a value of one data type into another data type),
    but returns a NULL value instead of raising an error when the conversion can not be performed.

    The ``column`` argument must be a string column in ClickZetta.

    Example::

        >>> from clickzetta.zettapark.types import IntegerType, FloatType
        >>> df = session.create_dataframe(['0', '-12', '22', '1001'], schema=["a"])
        >>> df.select(try_cast(col("a"), IntegerType()).as_('ans')).collect()
        [Row(ANS=0), Row(ANS=-12), Row(ANS=22), Row(ANS=1001)]

    Example::

        >>> df = session.create_dataframe(['0.12', 'USD 27.90', '13.97 USD', '€97.0', '17,-'], schema=["a"])
        >>> df.select(try_cast(col("a"), FloatType()).as_('ans')).collect()
        [Row(ANS=0.12), Row(ANS=None), Row(ANS=None), Row(ANS=None), Row(ANS=None)]

    """
    c = _to_col_if_str(column, "cast")
    return c.try_cast(to)


def to_binary(e: ColumnOrName, fmt: Optional[str] = None) -> Column:
    """Converts the input expression to a binary value. For NULL input, the output is NULL.

    Example::

        >>> df = session.create_dataframe(['00', '67', '0312'], schema=['a'])
        >>> df.select(to_binary(col('a')).as_('ans')).collect()
        [Row(ANS=bytearray(b'\\x00')), Row(ANS=bytearray(b'g')), Row(ANS=bytearray(b'\\x03\\x12'))]

        >>> df = session.create_dataframe(['aGVsbG8=', 'd29ybGQ=', 'IQ=='], schema=['a'])
        >>> df.select(to_binary(col('a'), 'BASE64').as_('ans')).collect()
        [Row(ANS=bytearray(b'hello')), Row(ANS=bytearray(b'world')), Row(ANS=bytearray(b'!'))]

        >>> df.select(to_binary(col('a'), 'UTF-8').as_('ans')).collect()
        [Row(ANS=bytearray(b'aGVsbG8=')), Row(ANS=bytearray(b'd29ybGQ=')), Row(ANS=bytearray(b'IQ=='))]
    """
    assert fmt is None or fmt.lower() == "utf-8"
    c = _to_col_if_str(e, "to_binary")
    # return builtin("to_binary")(c, fmt) if fmt else builtin("to_binary")(c)
    return builtin("binary")(c)


def to_array(e: ColumnOrName) -> Column:
    """Converts any value to an ARRAY value or NULL (if input is NULL).

    Example::

        >>> df = session.create_dataframe([1, 2, 3, 4], schema=['a'])
        >>> df.select(to_array(col('a')).as_('ans')).collect()
        [Row(ANS='[\\n  1\\n]'), Row(ANS='[\\n  2\\n]'), Row(ANS='[\\n  3\\n]'), Row(ANS='[\\n  4\\n]')]


        >>> from clickzetta.zettapark import Row
        >>> df = session.create_dataframe([Row(a=[1, 2, 3]), Row(a=None)])
        >>> df.select(to_array(col('a')).as_('ans')).collect()
        [Row(ANS='[\\n  1,\\n  2,\\n  3\\n]'), Row(ANS=None)]
    """
    c = _to_col_if_str(e, "to_array")
    return builtin("array")(c)


def array(*e: ColumnOrName) -> Column:
    c = [_to_col_if_str(i, "array") for i in e]
    return builtin("array")(*c)


def to_json(e: ColumnOrName) -> Column:
    """Converts a column containing a StructType, ArrayType or a MapType into a JSON string.
    Throws an exception, in the case of an unsupported type.

    Example::
        >>> from clickzetta.zettapark.types import *
        >>> from clickzetta.zettapark import Row
        >>> data = [(1, Row(age=2, name='Alice'))]
        >>> df = session.create_dataframe(data, ("key", "value"))
        >>> df.select(to_json(df.value).alias("json")).collect()
        [Row(json='{"age":2,"name":"Alice"}')]
        >>> data = [(1, [Row(age=2, name='Alice'), Row(age=3, name='Bob')])]
        >>> df = session.create_dataframe(data, ("key", "value"))
        >>> df.select(to_json(df.value).alias("json")).collect()
        [Row(json='[{"age":2,"name":"Alice"},{"age":3,"name":"Bob"}]')]
        >>> data = [(1, {"name": "Alice"})]
        >>> df = session.create_dataframe(data, ("key", "value"))
        >>> df.select(to_json(df.value).alias("json")).collect()
        [Row(json='{"name":"Alice"}')]
        >>> data = [(1, [{"name": "Alice"}, {"name": "Bob"}])]
        >>> df = session.create_dataframe(data, ("key", "value"))
        >>> df.select(to_json(df.value).alias("json")).collect()
        [Row(json='[{"name":"Alice"},{"name":"Bob"}]')]
        >>> data = [(1, ["Alice", "Bob"])]
        >>> df = session.create_dataframe(data, ("key", "value"))
        >>> df.select(to_json(df.value).alias("json")).collect()
        [Row(json='["Alice","Bob"]')]
    """
    c = _to_col_if_str(e, "to_json")
    return builtin("to_json")(c)


def from_json(e: ColumnOrName, schema: Union[ArrayType, StructType, str]) -> Column:
    """Parses a column containing a JSON string into a MapType
    with StringType as keys type, StructType or ArrayType with the specified schema.
    Returns null, in the case of an unparseable string."""
    c = _to_col_if_str(e, "from_json")
    if isinstance(schema, (ArrayType, StructType)):
        s = lit(convert_data_type_to_name(schema))
    elif isinstance(schema, str):
        s = lit(schema)
    else:
        raise ValueError(f"Invalid schema: {schema.__class__.__name__}")
    return builtin("from_json")(c, s)


def get_json_object(e: ColumnOrName, path: str) -> Column:
    """Extracts json object from a json string based on json path specified,
    and returns json string of the extracted json object.
    It will return null if the input json string is invalid.
    """
    c = _to_col_if_str(e, "get_json_object")
    return builtin("get_json_object")(c, lit(path))


def get_ignore_case(obj: ColumnOrName, field: ColumnOrName) -> Column:
    """
    Extracts a field value from an object. Returns NULL if either of the arguments is NULL.
    This function is similar to :meth:`get` but applies case-insensitive matching to field names.

    Examples::

        >>> df = session.create_dataframe([{"a": {"aa": 1, "bb": 2, "cc": 3}}])
        >>> df.select(get_ignore_case(df["a"], lit("AA")).alias("get_ignore_case")).collect()
        [Row(GET_IGNORE_CASE='1')]
    """
    c1 = _to_col_if_str(obj, "get_ignore_case")
    c2 = _to_col_if_str(field, "get_ignore_case")
    return builtin("get_ignore_case")(c1, c2)


def object_keys(obj: ColumnOrName) -> Column:
    """Returns an array containing the list of keys in the input object.


    Example::
        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.sql(
        ...     "select object_construct(a,b,c,d,e,f) as obj, k, v from "
        ...     "values('age', 21, 'zip', 21021, 'name', 'Joe', 'age', 0),"
        ...     "('age', 26, 'zip', 94021, 'name', 'Jay', 'age', 0) as T(a,b,c,d,e,f,k,v)"
        ... )
        >>> df.select(object_keys(col("obj")).alias("result")).show()
        -------------
        |"RESULT"   |
        -------------
        |[          |
        |  "age",   |
        |  "name",  |
        |  "zip"    |
        |]          |
        |[          |
        |  "age",   |
        |  "name",  |
        |  "zip"    |
        |]          |
        -------------
        <BLANKLINE>
    """
    c = _to_col_if_str(obj, "object_keys")
    return builtin("object_keys")(c)


def map_keys(obj: ColumnOrName) -> Column:
    """Returns an array containing the list of keys in the input map.


    Example::
        >>> from clickzetta.zettapark.functions import map_keys
        >>> df = spark.sql("SELECT map(1, 'a', 2, 'b') as data")
        >>> df.select(map_keys("data").alias("keys")).show()
        +------+
        |  keys|
        +------+
        |[1, 2]|
        +------+
    """
    c = _to_col_if_str(obj, "map_keys")
    return builtin("map_keys")(c)


def get_path(col: ColumnOrName, path: ColumnOrName) -> Column:
    """
    Extracts a value from semi-structured data using a path name.

    Examples::

        >>> df = session.create_dataframe([{"a": {"aa": {"dd": 4}, "bb": 2, "cc": 3}}])
        >>> df.select(get_path(df["a"], lit("aa.dd")).alias("get_path")).collect()
        [Row(GET_PATH='4')]
    """
    c1 = _to_col_if_str(col, "get_path")
    c2 = _to_col_if_str(path, "get_path")
    return builtin("get_path")(c1, c2)


def get(col1: Union[ColumnOrName, int], col2: Union[ColumnOrName, int]) -> Column:
    """Extracts a value from an object or array; returns NULL if either of the arguments is NULL.

    Example::

        >>> from clickzetta.zettapark.functions import lit
        >>> df = session.create_dataframe([({"a": 1.0, "b": 2.0}, [1, 2, 3],), ({}, [],)], ["map", "list"])
        >>> df.select(get(df.list, 1).as_("idx1")).sort(col("idx1")).show()
        ----------
        |"IDX1"  |
        ----------
        |NULL    |
        |2       |
        ----------
        <BLANKLINE>

        >>> df.select(get(df.map, lit("a")).as_("get_a")).sort(col("get_a")).show()
        -----------
        |"GET_A"  |
        -----------
        |NULL     |
        |1        |
        -----------
        <BLANKLINE>"""
    c1 = _to_col_if_str_or_int(col1, "get")
    c2 = _to_col_if_str_or_int(col2, "get")
    return builtin("get")(c1, c2)


element_at = get


def when(condition: ColumnOrSqlExpr, value: ColumnOrLiteral) -> CaseExpr:
    """Works like a cascading if-then-else statement.
    A series of conditions are evaluated in sequence.
    When a condition evaluates to TRUE, the evaluation stops and the associated
    result (after THEN) is returned. If none of the conditions evaluate to TRUE,
    then the result after the optional OTHERWISE is returned, if present;
    otherwise NULL is returned.

    Args:
        condition: A :class:`Column` expression or SQL text representing the specified condition.
        value: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is true.

    Example::

        >>> df = session.create_dataframe([1, None, 2, 3, None, 5, 6], schema=["a"])
        >>> df.collect()
        [Row(A=1), Row(A=None), Row(A=2), Row(A=3), Row(A=None), Row(A=5), Row(A=6)]
        >>> df.select(when(col("a") % 2 == 0, lit("even")).as_("ans")).collect()
        [Row(ANS=None), Row(ANS=None), Row(ANS='even'), Row(ANS=None), Row(ANS=None), Row(ANS=None), Row(ANS='even')]

    Multiple when statements can be changed and `otherwise`/`else_` used to create expressions similar to ``CASE WHEN ... ELSE ... END`` in SQL.

    Example::

        >>> df.select(when(col("a") % 2 == 0, lit("even")).when(col("a") % 2 == 1, lit("odd")).as_("ans")).collect()
        [Row(ANS='odd'), Row(ANS=None), Row(ANS='even'), Row(ANS='odd'), Row(ANS=None), Row(ANS='odd'), Row(ANS='even')]
        >>> df.select(when(col("a") % 2 == 0, lit("even")).when(col("a") % 2 == 1, lit("odd")).otherwise(lit("unknown")).as_("ans")).collect()
        [Row(ANS='odd'), Row(ANS='unknown'), Row(ANS='even'), Row(ANS='odd'), Row(ANS='unknown'), Row(ANS='odd'), Row(ANS='even')]
    """
    return CaseExpr(
        CaseWhen(
            [
                (
                    _to_col_if_sql_expr(condition, "when")._expression,
                    Column._to_expr(value),
                )
            ]
        )
    )


def iff(
    condition: ColumnOrSqlExpr,
    expr1: ColumnOrLiteral,
    expr2: ColumnOrLiteral,
) -> Column:
    """
    Returns one of two specified expressions, depending on a condition.
    This is equivalent to an ``if-then-else`` expression.

    Args:
        condition: A :class:`Column` expression or SQL text representing the specified condition.
        expr1: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is true.
        expr2: A :class:`Column` expression or a literal value, which will be returned
            if ``condition`` is false.

    Examples::

        >>> df = session.create_dataframe([True, False, None], schema=["a"])
        >>> df.select(iff(df["a"], lit("true"), lit("false")).alias("iff")).collect()
        [Row(IFF='true'), Row(IFF='false'), Row(IFF='false')]
    """
    return builtin("if")(_to_col_if_sql_expr(condition, "if"), expr1, expr2)


def in_(
    cols: List[ColumnOrName],
    *vals: Union["clickzetta.zettapark.DataFrame", LiteralType, Iterable[LiteralType]],
) -> Column:
    return _in_expr(cols, *vals, positive=True)


def not_in(
    cols: List[ColumnOrName],
    *vals: Union["clickzetta.zettapark.DataFrame", LiteralType, Iterable[LiteralType]],
) -> Column:
    return _in_expr(cols, *vals, positive=False)


def _in_expr(
    cols: List[ColumnOrName],
    *vals: Union["clickzetta.zettapark.DataFrame", LiteralType, Iterable[LiteralType]],
    positive: bool,
) -> Column:
    """Returns a conditional expression that you can pass to the filter or where methods to
    perform the equivalent of a WHERE ... IN query that matches rows containing a sequence of
    values.

    The expression evaluates to true if the values in a row matches the values in one of
    the specified sequences.

    The following code returns a DataFrame that contains the rows in which
    the columns `c1` and `c2` contain the values:
    - `1` and `"a"`, or
    - `2` and `"b"`
    This is equivalent to ``SELECT * FROM table WHERE (c1, c2) IN ((1, 'a'), (2, 'b'))``.

    Example::

        >>> df = session.create_dataframe([[1, "a"], [2, "b"], [3, "c"]], schema=["col1", "col2"])
        >>> df.filter(in_([col("col1"), col("col2")], [[1, "a"], [2, "b"]])).show()
        -------------------
        |"COL1"  |"COL2"  |
        -------------------
        |1       |a       |
        |2       |b       |
        -------------------
        <BLANKLINE>

    The following code returns a DataFrame that contains the rows where
    the values of the columns `c1` and `c2` in `df2` match the values of the columns
    `a` and `b` in `df1`. This is equivalent to
    ``SELECT * FROM table2 WHERE (c1, c2) IN (SELECT a, b FROM table1)``.

    Example::

        >>> df1 = session.sql("select 1, 'a'")
        >>> df.filter(in_([col("col1"), col("col2")], df1)).show()
        -------------------
        |"COL1"  |"COL2"  |
        -------------------
        |1       |a       |
        -------------------
        <BLANKLINE>

    Args::
        cols: A list of the columns to compare for the IN operation.
        vals: A list containing the values to compare for the IN operation.
    """
    vals = parse_positional_args_to_list(*vals)
    columns = [_to_col_if_str(c, "in_") for c in cols]
    return (
        Column(MultipleExpression([c._expression for c in columns])).in_(vals)
        if positive
        else Column(MultipleExpression([c._expression for c in columns])).not_in(vals)
    )


def cume_dist() -> Column:
    """
    Finds the cumulative distribution of a value with regard to other values
    within the same window partition.

    Example:

        >>> from clickzetta.zettapark.window import Window
        >>> from clickzetta.zettapark.types import DecimalType
        >>> df = session.create_dataframe([[1, 2], [1, 2], [1,3], [4, 5], [2, 3], [3, 4], [4, 7], [3,7], [4,5]], schema=["a", "b"])
        >>> df.select(cume_dist().over(Window.order_by("a")).cast(DecimalType(scale=3)).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.333     |
        |0.333     |
        |0.333     |
        |0.444     |
        |0.667     |
        |0.667     |
        |1.000     |
        |1.000     |
        |1.000     |
        ------------
        <BLANKLINE>
    """
    return builtin("cume_dist")()


def rank() -> Column:
    """
    Returns the rank of a value within an ordered group of values.
    The rank value starts at 1 and continues up.

    Example::
        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(rank().over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |2         |
        |2         |
        |1         |
        |1         |
        ------------
        <BLANKLINE>
    """
    return builtin("rank")()


def percent_rank() -> Column:
    """
    Returns the relative rank of a value within a group of values, specified as a percentage
    ranging from 0.0 to 1.0.

    Example::
        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(percent_rank().over(Window.partition_by("x").order_by(col("y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |0.0       |
        |0.5       |
        |0.5       |
        |0.0       |
        |0.0       |
        ------------
        <BLANKLINE>
    """
    return builtin("percent_rank")()


def dense_rank() -> Column:
    """
    Returns the rank of a value within a group of values, without gaps in the ranks.
    The rank value starts at 1 and continues up sequentially.
    If two values are the same, they will have the same rank.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> window = Window.order_by("key")
        >>> df = session.create_dataframe([(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"])
        >>> df.select(dense_rank().over(window).as_("dense_rank")).collect()
        [Row(DENSE_RANK=1), Row(DENSE_RANK=1), Row(DENSE_RANK=2), Row(DENSE_RANK=2)]
    """
    return builtin("dense_rank")()


def row_number() -> Column:
    """
    Returns a unique row number for each row within a window partition.
    The row number starts at 1 and continues up sequentially.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(row_number().over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).show()
        ------------
        |"RESULT"  |
        ------------
        |1         |
        |2         |
        |3         |
        |1         |
        |2         |
        ------------
        <BLANKLINE>
    """
    return builtin("row_number")()


def lag(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[ColumnOrLiteral] = None,
) -> Column:
    """
    Accesses data in a previous row in the same result set without having to
    join the table to itself.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(lag("Z").over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).collect()
        [Row(RESULT=None), Row(RESULT=10), Row(RESULT=1), Row(RESULT=None), Row(RESULT=1)]
    """
    c = _to_col_if_str(e, "lag")
    return Column(Lag(c._expression, offset, Column._to_expr(default_value)))


def lead(
    e: ColumnOrName,
    offset: int = 1,
    default_value: Optional[Union[Column, LiteralType]] = None,
) -> Column:
    """
    Accesses data in a subsequent row in the same result set without having to
    join the table to itself.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [
        ...         [1, 2, 1],
        ...         [1, 2, 3],
        ...         [2, 1, 10],
        ...         [2, 2, 1],
        ...         [2, 2, 3],
        ...     ],
        ...     schema=["x", "y", "z"]
        ... )
        >>> df.select(lead("Z").over(Window.partition_by(col("X")).order_by(col("Y"))).alias("result")).collect()
        [Row(RESULT=1), Row(RESULT=3), Row(RESULT=None), Row(RESULT=3), Row(RESULT=None)]
    """
    c = _to_col_if_str(e, "lead")
    return Column(Lead(c._expression, offset, Column._to_expr(default_value)))


def last_value(
    e: ColumnOrName,
    ignore_nulls: bool = False,
) -> Column:
    """
    Returns the last value within an ordered group of values.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> window = Window.partition_by("column1").order_by("column2")
        >>> df = session.create_dataframe([[1, 10], [1, 11], [2, 20], [2, 21]], schema=["column1", "column2"])
        >>> df.select(df["column1"], df["column2"], last_value(df["column2"]).over(window).as_("column2_last")).collect()
        [Row(COLUMN1=1, COLUMN2=10, COLUMN2_LAST=11), Row(COLUMN1=1, COLUMN2=11, COLUMN2_LAST=11), Row(COLUMN1=2, COLUMN2=20, COLUMN2_LAST=21), Row(COLUMN1=2, COLUMN2=21, COLUMN2_LAST=21)]
    """
    c = _to_col_if_str(e, "last_value")
    return Column(LastValue(c._expression, None, None, ignore_nulls))


def first_value(
    e: ColumnOrName,
    ignore_nulls: bool = False,
) -> Column:
    """
    Returns the first value within an ordered group of values.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> window = Window.partition_by("column1").order_by("column2")
        >>> df = session.create_dataframe([[1, 10], [1, 11], [2, 20], [2, 21]], schema=["column1", "column2"])
        >>> df.select(df["column1"], df["column2"], first_value(df["column2"]).over(window).as_("column2_first")).collect()
        [Row(COLUMN1=1, COLUMN2=10, COLUMN2_FIRST=10), Row(COLUMN1=1, COLUMN2=11, COLUMN2_FIRST=10), Row(COLUMN1=2, COLUMN2=20, COLUMN2_FIRST=20), Row(COLUMN1=2, COLUMN2=21, COLUMN2_FIRST=20)]
    """
    c = _to_col_if_str(e, "last_value")
    return Column(FirstValue(c._expression, None, None, ignore_nulls))


def ntile(e: Union[int, ColumnOrName]) -> Column:
    """
    Divides an ordered data set equally into the number of buckets specified by n.
    Buckets are sequentially numbered 1 through n.

    Args:
        e: The desired number of buckets; must be a positive integer value.

    Example::

        >>> from clickzetta.zettapark.window import Window
        >>> df = session.create_dataframe(
        ...     [["C", "SPY", 3], ["C", "AAPL", 10], ["N", "SPY", 5], ["N", "AAPL", 7], ["Q", "MSFT", 3]],
        ...     schema=["exchange", "symbol", "shares"]
        ... )
        >>> df.select(col("exchange"), col("symbol"), ntile(3).over(Window.partition_by("exchange").order_by("shares")).alias("ntile_3")).show()
        -------------------------------------
        |"EXCHANGE"  |"SYMBOL"  |"NTILE_3"  |
        -------------------------------------
        |Q           |MSFT      |1          |
        |N           |SPY       |1          |
        |N           |AAPL      |2          |
        |C           |SPY       |1          |
        |C           |AAPL      |2          |
        -------------------------------------
        <BLANKLINE>
    """
    c = _to_col_if_str_or_int(e, "ntile")
    return builtin("ntile")(c)


def percentile_cont(percentile: float) -> Column:
    """
    Return a percentile value based on a continuous distribution of the
    input column. If no input row lies exactly at the desired percentile,
    the result is calculated using linear interpolation of the two nearest
    input values. NULL values are ignored in the calculation.

    Args:
        percentile: the percentile of the value that you want to find.
            The percentile must be a constant between 0.0 and 1.0. For example,
            if you want to find the value at the 90th percentile, specify 0.9.

    Example:

        >>> df = session.create_dataframe([
        ...     (0, 0), (0, 10), (0, 20), (0, 30), (0, 40),
        ...     (1, 10), (1, 20), (2, 10), (2, 20), (2, 25),
        ...     (2, 30), (3, 60), (4, None)
        ... ], schema=["k", "v"])
        >>> df.group_by("k").agg(percentile_cont(0.25).within_group("v").as_("percentile")).sort("k").collect()
        [Row(K=0, PERCENTILE=Decimal('10.000')), \
Row(K=1, PERCENTILE=Decimal('12.500')), \
Row(K=2, PERCENTILE=Decimal('17.500')), \
Row(K=3, PERCENTILE=Decimal('60.000')), \
Row(K=4, PERCENTILE=None)]
    """
    return builtin("percentile_cont")(percentile)


def greatest(*columns: ColumnOrName) -> Column:
    """
    Returns the largest value from a list of expressions.
    If any of the argument values is NULL, the result is NULL.

    Examples::

        >>> df = session.create_dataframe([[1, 2, 3], [2, 4, -1], [3, 6, None]], schema=["a", "b", "c"])
        >>> df.select(greatest(df["a"], df["b"], df["c"]).alias("greatest")).collect()
        [Row(GREATEST=3), Row(GREATEST=4), Row(GREATEST=None)]
    """
    c = [_to_col_if_str(ex, "greatest") for ex in columns]
    return builtin("greatest")(*c)


def least(*columns: ColumnOrName) -> Column:
    """
    Returns the smallest value from a list of expressions.
    If any of the argument values is NULL, the result is NULL.

    Example::

        >>> df = session.create_dataframe([[1, 2, 3], [2, 4, -1], [3, 6, None]], schema=["a", "b", "c"])
        >>> df.select(least(df["a"], df["b"], df["c"]).alias("least")).collect()
        [Row(LEAST=1), Row(LEAST=-1), Row(LEAST=None)]
    """
    c = [_to_col_if_str(ex, "least") for ex in columns]
    return builtin("least")(*c)


def listagg(e: ColumnOrName, delimiter: str = "", is_distinct: bool = False) -> Column:
    """
    Returns the concatenated input values, separated by `delimiter` string.
    See `LISTAGG <https://doc.clickzetta.com/>`_ for details.

    Args:
        e: A :class:`Column` object or column name that determines the values
            to be put into the list.
        delimiter: A string delimiter.
        is_distinct: Whether the input expression is distinct.

    Examples::

        >>> df = session.create_dataframe([1, 2, 3, 2, 4, 5], schema=["col"])
        >>> df.select(listagg("col", ",").within_group(df["col"].asc()).as_("result")).collect()
        [Row(RESULT='1,2,2,3,4,5')]
    """
    c = _to_col_if_str(e, "listagg")
    return Column(ListAgg(c._expression, delimiter, is_distinct))


def when_matched(
    condition: Optional[Column] = None,
) -> "clickzetta.zettapark.table.WhenMatchedClause":
    """
    Specifies a matched clause for the :meth:`Table.merge <clickzetta.zettapark.Table.merge>` action.
    See :class:`~clickzetta.zettapark.table.WhenMatchedClause` for details.

    Convenience function to create a new WhenMatchedClause instance which is required together with an action when merging
    a Zettapark table with a Zettapark DataFrame (see clickzetta.zettapark.Table.merge for more details).

    Example::

        >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
        >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
        >>> target = session.table("my_table")
        >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
        >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
        ...              [when_matched().update({"value": source["value"]})])
        MergeResult(rows_inserted=0, rows_updated=1, rows_deleted=0)
        >>> target.collect()
        [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='new'), Row(KEY=11, VALUE='old')]

    """
    return clickzetta.zettapark.table.WhenMatchedClause(condition)


def when_not_matched(
    condition: Optional[Column] = None,
) -> "clickzetta.zettapark.table.WhenNotMatchedClause":
    """
    Specifies a not-matched clause for the :meth:`Table.merge <clickzetta.zettapark.Table.merge>` action.
    See :class:`~clickzetta.zettapark.table.WhenNotMatchedClause` for details.

    Convenience function to create a new WhenNotMatchedClause instance which is required together with an action when merging
    a Zettapark table with a Zettapark DataFrame (see clickzetta.zettapark.Table.merge for more details).

    Example::

        >>> from clickzetta.zettapark.types import IntegerType, StringType, StructField, StructType
        >>> schema = StructType([StructField("key", IntegerType()), StructField("value", StringType())])
        >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=schema)
        >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
        >>> target = session.table("my_table")
        >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=schema)
        >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
        ...              [when_not_matched().insert({"key": source["key"]})])
        MergeResult(rows_inserted=2, rows_updated=0, rows_deleted=0)
        >>> target.sort(col("key"), col("value")).collect()
        [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE=None), Row(KEY=13, VALUE=None)]
    """
    return clickzetta.zettapark.table.WhenNotMatchedClause(condition)


def call_udf(
    udf_name: str,
    *args: ColumnOrLiteral,
) -> Column:
    """Calls a user-defined function (UDF) by name.

    Args:
        udf_name: The name of UDF in ClickZetta.
        args: Arguments can be in two types:

            - :class:`~clickzetta.zettapark.Column`, or
            - Basic Python types, which are converted to Zettapark literals.

    Example::
        >>> from clickzetta.zettapark.types import IntegerType
        >>> udf_def = session.udf.register(lambda x, y: x + y, name="add_columns", input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), replace=True)
        >>> df = session.create_dataframe([[1, 2]], schema=["a", "b"])
        >>> df.select(call_udf("add_columns", col("a"), col("b"))).show()
        -------------------------------
        |"ADD_COLUMNS(""A"", ""B"")"  |
        -------------------------------
        |3                            |
        -------------------------------
        <BLANKLINE>
    """

    validate_object_name(udf_name)
    udf_registration = clickzetta.zettapark.session._get_active_session().udf
    identifer = udf_registration._to_identifier(udf_name)
    if identifer is None:
        raise ValueError(f"UDF '{udf_name}' is not registered")
    return _call_function(identifer, False, *args, api_call_source="functions.call_udf")


def call_table_function(
    function_name: str, *args: ColumnOrLiteral, **kwargs: ColumnOrLiteral
) -> "clickzetta.zettapark.table_function.TableFunctionCall":
    """Invokes a ClickZetta table function, including system-defined table functions and user-defined table functions.

    It returns a :meth:`~clickzetta.zettapark.table_function.TableFunctionCall` so you can specify the partition clause.

    Args:
        function_name: The name of the table function.
        args: The positional arguments of the table function.
        **kwargs: The named arguments of the table function. Some table functions (e.g., ``flatten``) have named arguments instead of positional ones.

    Example::
        >>> from clickzetta.zettapark.functions import lit
        >>> session.table_function(call_table_function("split_to_table", lit("split words to table"), lit(" ")).over()).collect()
        [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    return clickzetta.zettapark.table_function.TableFunctionCall(
        function_name, *args, **kwargs
    )


def table_function(function_name: str) -> Callable:
    """Create a function object to invoke a ClickZetta table function.

    Args:
        function_name: The name of the table function.

    Example::
        >>> from clickzetta.zettapark.functions import lit
        >>> split_to_table = table_function("split_to_table")
        >>> session.table_function(split_to_table(lit("split words to table"), lit(" ")).over()).collect()
        [Row(SEQ=1, INDEX=1, VALUE='split'), Row(SEQ=1, INDEX=2, VALUE='words'), Row(SEQ=1, INDEX=3, VALUE='to'), Row(SEQ=1, INDEX=4, VALUE='table')]
    """
    return lambda *args, **kwargs: call_table_function(function_name, *args, **kwargs)


def call_function(function_name: str, *args: ColumnOrLiteral) -> Column:
    """Invokes a ClickZetta `system-defined function <https://doc.clickzetta.com/>`_ (built-in function) with the specified name
    and arguments.

    Args:
        function_name: The name of built-in function in Clickzetta
        args: Arguments can be in two types:

            - :class:`~clickzetta.zettapark.Column`, or
            - Basic Python types, which are converted to Zettapark literals.

    Example::
        >>> df = session.create_dataframe([1, 2, 3, 4], schema=["a"])  # a single column with 4 rows
        >>> df.select(call_function("avg", col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>

    """

    return _call_function(function_name, False, *args)


def function(
    function_name: str,
) -> Callable:
    """
    Function object to invoke a ClickZetta `system-defined function <https://doc.clickzetta.com/>`_ (built-in function). Use this to invoke
    any built-in functions not explicitly listed in this object.

    Args:
        function_name: The name of built-in function in ClickZetta.

    Returns:
        A :class:`Callable` object for calling a ClickZetta system-defined function.

    Example::
        >>> df = session.create_dataframe([1, 2, 3, 4], schema=["a"])  # a single column with 4 rows
        >>> df.select(call_function("avg", col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>
        >>> my_avg = function('avg')
        >>> df.select(my_avg(col("a"))).show()
        ----------------
        |"AVG(""A"")"  |
        ----------------
        |2.500000      |
        ----------------
        <BLANKLINE>
    """
    return lambda *args: call_function(function_name, *args)


def _call_function(
    name: str,
    is_distinct: bool = False,
    *args: ColumnOrLiteral,
    api_call_source: Optional[str] = None,
    is_data_generator: bool = False,
) -> Column:
    expressions = [Column._to_expr(arg) for arg in parse_positional_args_to_list(*args)]
    return Column(
        FunctionExpression(
            name,
            expressions,
            is_distinct=is_distinct,
            api_call_source=api_call_source,
            is_data_generator=is_data_generator,
        )
    )


# Add these alias for user code migration
call_builtin = call_function
collect_set = array_unique_agg
builtin = function
countDistinct = count_distinct
substr = substring
# to_varchar = to_char
expr = sql_expr
monotonically_increasing_id = seq8
from_unixtime = to_timestamp
sort_array = array_sort
signum = sign


def unix_timestamp(e: ColumnOrName, fmt: Optional["Column"] = None) -> Column:
    """
    Converts a timestamp or a timestamp string to Unix time stamp (in seconds).

    Example::

        >>> import datetime
        >>> df = session.create_dataframe([["2013-05-08T23:39:20.123-07:00"]], schema=["ts_col"])
        >>> df.select(unix_timestamp(col("ts_col")).alias("unix_time")).show()
        ---------------
        |"UNIX_TIME"  |
        ---------------
        |1368056360   |
        ---------------
        <BLANKLINE>
    """
    return builtin("UNIX_TIMESTAMP")(to_timestamp(e, fmt))
