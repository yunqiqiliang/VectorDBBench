#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from clickzetta.zettapark._internal.analyzer.binary_plan_node import (
    AsOf,
    Except,
    Intersect,
    JoinType,
    LeftAnti,
    LeftSemi,
    NaturalJoin,
    UsingJoin,
)
from clickzetta.zettapark._internal.analyzer.datatype_mapper import (
    schema_expression,
    to_sql,
)
from clickzetta.zettapark._internal.analyzer.expression import Attribute
from clickzetta.zettapark._internal.type_utils import convert_data_type_to_name
from clickzetta.zettapark._internal.utils import (
    ALREADY_QUOTED,
    BACKTICK,
    EMPTY_STRING,
    TempObjectType,
    escape_quotes,
    is_single_quoted,
    is_sql_select_statement,
    is_sql_show_statement,
    quote_if_needed,
    quote_name,
    random_name_for_temp_object,
    unquote_if_safe,
)
from clickzetta.zettapark._internal.volume_path import VolumeKind, VolumePath
from clickzetta.zettapark.row import Row
from clickzetta.zettapark.types import DataType, StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

LEFT_PARENTHESIS = "("
RIGHT_PARENTHESIS = ")"
LEFT_BRACKET = "["
RIGHT_BRACKET = "]"
AS = " AS "
AND = " AND "
OR = " OR "
NOT = " NOT "
STAR = " * "
SPACE = " "
SINGLE_QUOTE = "'"
COMMA = ", "
MINUS = " - "
PLUS = " + "
DISTINCT = " DISTINCT "
LIKE = " LIKE "
CAST = " CAST "
TRY_CAST = " CAST "
IN = " IN "
GROUP_BY = " GROUP BY "
PARTITION_BY = " PARTITION BY "
ORDER_BY = " ORDER BY "
CLUSTER_BY = " CLUSTERED BY "
OVER = " OVER "
SELECT = " SELECT "
FROM = " FROM "
WHERE = " WHERE "
LIMIT = " LIMIT "
OFFSET = " OFFSET "
PIVOT = " PIVOT "
UNPIVOT = " UNPIVOT "
FOR = " FOR "
ON = " ON "
USING = " USING "
JOIN = " JOIN "
NATURAL = " NATURAL "
ASOF = " ASOF "
MATCH_CONDITION = " MATCH_CONDITION "
EXISTS = " EXISTS "
CREATE = " CREATE "
TABLE = " TABLE "
REPLACE = " REPLACE "
VIEW = " VIEW "
DYNAMIC = " DYNAMIC "
LAG = " LAG "
WAREHOUSE = " WAREHOUSE "
TEMPORARY = " TEMPORARY "
IF = " If "
INSERT = " INSERT "
INTO = " INTO "
OVERWRITE = " OVERWRITE "
VALUES = " VALUES "
SEQ8 = " SEQ8() "
ROW_NUMBER = " ROW_NUMBER() "
ONE = " 1 "
GENERATOR = "GENERATOR"
ROW_COUNT = "ROWCOUNT"
RIGHT_ARROW = " => "
NUMBER = " NUMBER "
STRING = " STRING "
UNSAT_FILTER = " 1 = 0 "
BETWEEN = " BETWEEN "
FOLLOWING = " FOLLOWING "
PRECEDING = " PRECEDING "
DOLLAR = "$"
DOUBLE_COLON = "::"
DROP = " DROP "
TRUNCATE = " truncate "
FILE = " FILE "
FILES = " FILES "
FORMAT = " FORMAT "
TYPE = " TYPE "
EQUALS = " = "
LOCATION = " LOCATION "
FILE_FORMAT = " FILE_FORMAT "
FORMAT_NAME = " FORMAT_NAME "
COPY = " COPY "
REG_EXP = " RLIKE "
COLLATE = " COLLATE "
RESULT_SCAN = " RESULT_SCAN"
INFER_SCHEMA = " INFER_SCHEMA "
SAMPLE = " SAMPLE "
ROWS = " ROWS "
CASE = " CASE "
WHEN = " WHEN "
THEN = " THEN "
ELSE = " ELSE "
END = " END "
FLATTEN = " FLATTEN "
INPUT = " INPUT "
PATH = " PATH "
OUTER = " OUTER "
RECURSIVE = " RECURSIVE "
MODE = " MODE "
LATERAL = " LATERAL "
PUT = " PUT "
GET = " GET "
GROUPING_SETS = " GROUPING SETS "
QUESTION_MARK = "?"
PATTERN = " PATTERN "
WITHIN_GROUP = " WITHIN GROUP "
VALIDATION_MODE = " VALIDATION_MODE "
UPDATE = " UPDATE "
DELETE = " DELETE "
SET = " SET "
MERGE = " MERGE "
MATCHED = " MATCHED "
LISTAGG = " LISTAGG "
HEADER = " HEADER "
IGNORE_NULLS = " IGNORE NULLS "
UNION = " UNION "
UNION_ALL = " UNION ALL "
RENAME = " RENAME "
INTERSECT = f" {Intersect.sql} "
EXCEPT = f" {Except.sql} "
NOT_NULL = " NOT NULL "
WITH = "WITH "
WITH_STMT_SUFFIX = (
    " /* 221d83563c966dac7ab3a208a5be8b8fc9c9f7692a0568512cfb7ac974fa27aa */ "
)
TO = " TO "
VOLUME = " VOLUME "
USER = " USER "
SUBDIRECTORY = " SUBDIRECTORY "
LIST = " LIST "
REGEXP = " REGEXP "
EXTERNAL = " EXTERNAL "
FUNCTION = " FUNCTION "
PROPERTIES = " PROPERTIES "

TEMPORARY_STRING_SET = frozenset(["temporary", "temp"])


def result_scan_statement(uuid_place_holder: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + TABLE
        + LEFT_PARENTHESIS
        + RESULT_SCAN
        + LEFT_PARENTHESIS
        + SINGLE_QUOTE
        + uuid_place_holder
        + SINGLE_QUOTE
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def function_expression(
    name: str,
    children: List[str],
    is_distinct: bool,
    parse_local_name: bool = False,
    generate_udf_alias: bool = False,
) -> str:
    if parse_local_name and not generate_udf_alias:
        name = quote_if_needed(name)
    return (
        name
        + LEFT_PARENTHESIS
        + f"{'DISTINCT ' if is_distinct else EMPTY_STRING}"
        + COMMA.join(unquote_if_safe(c) if parse_local_name else c for c in children)
        + RIGHT_PARENTHESIS
    )


def named_arguments_function(name: str, args: Dict[str, str]) -> str:
    return (
        name
        + LEFT_PARENTHESIS
        + COMMA.join([key + RIGHT_ARROW + value for key, value in args.items()])
        + RIGHT_PARENTHESIS
    )


def partition_spec(col_exprs: List[str]) -> str:
    return f"PARTITION BY {COMMA.join(col_exprs)}" if col_exprs else EMPTY_STRING


def order_by_spec(col_exprs: List[str]) -> str:
    return f"ORDER BY {COMMA.join(col_exprs)}" if col_exprs else EMPTY_STRING


def table_function_partition_spec(
    over: bool, partition_exprs: List[str], order_exprs: List[str]
) -> str:
    return (
        f"{OVER}{LEFT_PARENTHESIS}{partition_spec(partition_exprs)}{SPACE}{order_by_spec(order_exprs)}{RIGHT_PARENTHESIS}"
        if over
        else EMPTY_STRING
    )


def subquery_expression(child: str) -> str:
    return LEFT_PARENTHESIS + child + RIGHT_PARENTHESIS


def binary_arithmetic_expression(
    op: str, left: str, right: str, parse_local_name: bool = False
) -> str:
    if parse_local_name:
        return unquote_if_safe(left) + SPACE + op + SPACE + unquote_if_safe(right)
    return LEFT_PARENTHESIS + left + SPACE + op + SPACE + right + RIGHT_PARENTHESIS


def alias_expression(origin: str, alias: str) -> str:
    return origin + AS + alias


def within_group_expression(column: str, order_by_cols: List[str]) -> str:
    return (
        column
        + WITHIN_GROUP
        + LEFT_PARENTHESIS
        + ORDER_BY
        + COMMA.join(order_by_cols)
        + RIGHT_PARENTHESIS
    )


def grouping_set_expression(args: List[List[str]]) -> str:
    flat_args = [LEFT_PARENTHESIS + COMMA.join(arg) + RIGHT_PARENTHESIS for arg in args]
    return GROUPING_SETS + LEFT_PARENTHESIS + COMMA.join(flat_args) + RIGHT_PARENTHESIS


def like_expression(expr: str, pattern: str) -> str:
    return expr + LIKE + pattern


def block_expression(expressions: List[str]) -> str:
    if (
        len(expressions) == 1
        and expressions[0].startswith("(")
        and expressions[0].endswith(")")
    ):
        return expressions[0]
    return LEFT_PARENTHESIS + COMMA.join(expressions) + RIGHT_PARENTHESIS


def in_expression(
    column: str, values: List[str], positive: bool, parse_local_name: bool = False
) -> str:
    return (
        (unquote_if_safe(column) if parse_local_name else column)
        + (" IN " if positive else " NOT IN ")
        + block_expression(values)
    )


def regexp_expression(expr: str, pattern: str) -> str:
    return expr + REG_EXP + pattern


def collate_expression(expr: str, collation_spec: str) -> str:
    return expr + COLLATE + single_quote(collation_spec)


def subfield_expression(expr: str, field: Union[str, int]) -> str:
    return (
        expr
        + LEFT_BRACKET
        + (
            SINGLE_QUOTE + field + SINGLE_QUOTE
            if isinstance(field, str)
            else str(field)
        )
        + RIGHT_BRACKET
    )


def flatten_expression(
    input_: str, path: Optional[str], outer: bool, recursive: bool, mode: str
) -> str:
    return (
        FLATTEN
        + LEFT_PARENTHESIS
        + INPUT
        + RIGHT_ARROW
        + input_
        + COMMA
        + PATH
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + (path or EMPTY_STRING)
        + SINGLE_QUOTE
        + COMMA
        + OUTER
        + RIGHT_ARROW
        + str(outer).upper()
        + COMMA
        + RECURSIVE
        + RIGHT_ARROW
        + str(recursive).upper()
        + COMMA
        + MODE
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + mode
        + SINGLE_QUOTE
        + RIGHT_PARENTHESIS
    )


def lateral_statement(lateral_expression: str, child: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + COMMA
        + LATERAL
        + lateral_expression
    )


def join_table_function_statement(
    func: str,
    child: str,
    left_cols: List[str],
    right_cols: List[str],
    use_constant_subquery_alias: bool,
) -> str:
    LEFT_ALIAS = (
        "T_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    RIGHT_ALIAS = (
        "T_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    left_cols = [f"{LEFT_ALIAS}.{col}" for col in left_cols]
    right_cols = [f"{RIGHT_ALIAS}.{col}" for col in right_cols]
    select_cols = COMMA.join(left_cols + right_cols)

    return (
        SELECT
        + select_cols
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + AS
        + LEFT_ALIAS
        + JOIN
        + LEFT_PARENTHESIS
        + SELECT
        + func
        + RIGHT_PARENTHESIS
        + AS
        + RIGHT_ALIAS
    )


def table_function_statement(func: str, operators: Optional[List[str]] = None) -> str:
    if operators is None:
        return project_statement([], func)
    return project_statement(operators, func)


def case_when_expression(branches: List[Tuple[str, str]], else_value: str) -> str:
    return (
        " CASE"
        + EMPTY_STRING.join(
            [WHEN + condition + THEN + value for condition, value in branches]
        )
        + ELSE
        + else_value
        + END
    )


def project_statement(project: List[str], child: str, is_distinct: bool = False) -> str:
    if " " not in child:
        return (
            SELECT
            + f"{DISTINCT if is_distinct else EMPTY_STRING}"
            + f"{STAR if not project else COMMA.join(project)}"
            + FROM
            + child
        )
    else:
        return (
            SELECT
            + f"{DISTINCT if is_distinct else EMPTY_STRING}"
            + f"{STAR if not project else COMMA.join(project)}"
            + FROM
            + LEFT_PARENTHESIS
            + child
            + RIGHT_PARENTHESIS
        )


def filter_statement(condition: str, child: str) -> str:
    return project_statement([], child) + WHERE + condition


def sample_statement(
    child: str,
    probability_fraction: Optional[float] = None,
    row_count: Optional[int] = None,
):
    """Generates the sql text for the sample part of the plan being executed"""
    if probability_fraction is not None:
        return (
            project_statement([], child)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(probability_fraction * 100)
            + RIGHT_PARENTHESIS
        )
    elif row_count is not None:
        return (
            project_statement([], child)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(row_count)
            + ROWS
            + RIGHT_PARENTHESIS
        )
    # this shouldn't happen because upstream code will validate either probability_fraction or row_count will have a value.
    else:  # pragma: no cover
        raise ValueError(
            "Either 'probability_fraction' or 'row_count' must not be None."
        )


def aggregate_statement(
    grouping_exprs: List[str], aggregate_exprs: List[str], child: str
) -> str:
    # add limit 1 because aggregate may be on non-aggregate function in a scalar aggregation
    # for example, df.agg(lit(1))
    return project_statement(aggregate_exprs, child) + (
        (LIMIT + str(1))
        if not grouping_exprs
        else (GROUP_BY + COMMA.join(grouping_exprs))
    )


def sort_statement(order: List[str], child: str) -> str:
    return project_statement([], child) + ORDER_BY + COMMA.join(order)


def range_statement(start: int, end: int, step: int, column_name: str) -> str:
    query = (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + (
            SELECT
            + "EXPLODE"
            + LEFT_PARENTHESIS
            + "SEQUENCE"
            + LEFT_PARENTHESIS
            + f"{start}, {end}, {step}"
            + RIGHT_PARENTHESIS
            + RIGHT_PARENTHESIS
            + AS
            + column_name
        )
        + RIGHT_PARENTHESIS
        + WHERE
        + LEFT_PARENTHESIS
        + f"{column_name} != {end}"
        + RIGHT_PARENTHESIS
    )
    return query


def schema_query_for_values_statement(output: List[Attribute]) -> str:
    cells = [schema_expression(attr.datatype, attr.nullable) for attr in output]

    query = (
        SELECT
        + COMMA.join([f"col{i + 1}{AS}{attr.name}" for i, attr in enumerate(output)])
        + FROM
        + VALUES
        + LEFT_PARENTHESIS
        + COMMA.join(cells)
        + RIGHT_PARENTHESIS
    )
    return query


def values_statement(output: List[Attribute], data: List[Row]) -> str:
    data_types = [attr.datatype for attr in output]
    names = [quote_name(attr.name) for attr in output]
    rows = []
    for row in data:
        cells = [
            to_sql(value, data_type, from_values_statement=True)
            for value, data_type in zip(row, data_types)
        ]
        rows.append(LEFT_PARENTHESIS + COMMA.join(cells) + RIGHT_PARENTHESIS)

    query = (
        SELECT
        + COMMA.join([f"col{i + 1}{AS}{c}" for i, c in enumerate(names)])
        + FROM
        + VALUES
        + COMMA.join(rows)
    )
    return query


def placeholder_values_statement(
    output: List[Attribute], data: List[List[Any]]
) -> List[str]:
    data_types = [attr.datatype for attr in output]
    rows = []
    for row in data:
        cells = [
            to_sql(value, data_type, from_values_statement=True)
            for value, data_type in zip(row, data_types)
        ]
        rows.append(LEFT_PARENTHESIS + COMMA.join(cells) + RIGHT_PARENTHESIS)
    return rows


def empty_values_statement(output: List[Attribute]) -> str:
    data = [Row(*[None] * len(output))]
    return filter_statement(UNSAT_FILTER, values_statement(output, data))


def wrap_select_if_sql_show_statement(query: str) -> str:
    return (
        (SELECT + STAR + FROM + LEFT_PARENTHESIS + query + RIGHT_PARENTHESIS)
        if is_sql_show_statement(query)
        else query
    )


def set_operator_statement(left: str, right: str, operator: str) -> str:
    return (
        LEFT_PARENTHESIS
        + wrap_select_if_sql_show_statement(left)
        + RIGHT_PARENTHESIS
        + SPACE
        + operator
        + SPACE
        + LEFT_PARENTHESIS
        + wrap_select_if_sql_show_statement(right)
        + RIGHT_PARENTHESIS
    )


def left_semi_or_anti_join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    condition: str,
    use_constant_subquery_alias: bool,
) -> str:
    left_alias = (
        "ZETTAPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "ZETTAPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    if isinstance(join_type, LeftSemi):
        where_condition = " SEMI JOIN "
    else:
        where_condition = " ANTI JOIN "

    # this generates sql like "Where a = b"
    join_condition = ON + condition

    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + where_condition
        + LEFT_PARENTHESIS
        + SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + f"{join_condition if join_condition else EMPTY_STRING}"
    )


def asof_join_statement(
    left: str,
    right: str,
    join_condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
):
    left_alias = (
        "ZETTAPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "ZETTAPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    on_sql = ON + join_condition if join_condition else EMPTY_STRING

    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + ASOF
        + JOIN
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + MATCH_CONDITION
        + LEFT_PARENTHESIS
        + match_condition
        + RIGHT_PARENTHESIS
        + on_sql
    )


def _supported_join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
) -> str:
    left_alias = (
        "ZETTAPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "ZETTAPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    if isinstance(join_type, UsingJoin):
        join_sql = join_type.tpe.sql
    elif isinstance(join_type, NaturalJoin):
        join_sql = NATURAL + join_type.tpe.sql
    else:
        join_sql = join_type.sql

    # This generates sql like "USING(a, b)"
    using_condition = None
    if isinstance(join_type, UsingJoin):
        if len(join_type.using_columns) != 0:
            using_condition = (
                USING
                + LEFT_PARENTHESIS
                + COMMA.join(join_type.using_columns)
                + RIGHT_PARENTHESIS
            )

    # This generates sql like "ON a = b"
    join_condition = None
    if condition:
        join_condition = ON + condition

    if using_condition and join_condition:
        raise ValueError("A join should either have using clause or a join condition")

    match_condition = (
        (MATCH_CONDITION + match_condition) if match_condition else EMPTY_STRING
    )

    source = (
        LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + SPACE
        + join_sql
        + JOIN
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + f"{match_condition if match_condition else EMPTY_STRING}"
        + f"{using_condition if using_condition else EMPTY_STRING}"
        + f"{join_condition if join_condition else EMPTY_STRING}"
    )

    return project_statement([], source)


def join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    join_condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
) -> str:
    if isinstance(join_type, (LeftSemi, LeftAnti)):
        return left_semi_or_anti_join_statement(
            left, right, join_type, join_condition, use_constant_subquery_alias
        )
    if isinstance(join_type, AsOf):
        return asof_join_statement(
            left, right, join_condition, match_condition, use_constant_subquery_alias
        )
    if isinstance(join_type, UsingJoin) and isinstance(
        join_type.tpe, (LeftSemi, LeftAnti)
    ):
        raise ValueError(f"Unexpected using clause in {join_type.tpe} join")
    return _supported_join_statement(
        left,
        right,
        join_type,
        join_condition,
        match_condition,
        use_constant_subquery_alias,
    )


def create_table_statement(
    table_name: str,
    schema: str,
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
    clustering_key: Optional[Iterable[str]] = None,
    *,
    use_scoped_temp_objects: bool = False,
    is_generated: bool = False,
) -> str:
    cluster_by_clause = (
        (CLUSTER_BY + LEFT_PARENTHESIS + COMMA.join(clustering_key) + RIGHT_PARENTHESIS)
        if clustering_key
        else EMPTY_STRING
    )
    return (
        f"{CREATE}"
        f"{TABLE}"
        f" {(IF + NOT + EXISTS) if not replace and not error else EMPTY_STRING} "
        f"{table_name}"
        f"{LEFT_PARENTHESIS}{schema}{RIGHT_PARENTHESIS}"
        f"{cluster_by_clause}"
    )


def insert_into_statement(
    table_name: str, child: str, column_names: Optional[Iterable[str]] = None
) -> str:
    table_columns = f"({COMMA.join(column_names)})" if column_names else EMPTY_STRING
    if is_sql_select_statement(child):
        if WITH_STMT_SUFFIX in child:
            cte_stmt, rest = child.split(WITH_STMT_SUFFIX)
            return f"{cte_stmt}{WITH_STMT_SUFFIX}{INSERT}{INTO}{table_name}{table_columns}{SPACE}{rest}"
        return f"{INSERT}{INTO}{table_name}{table_columns}{SPACE}{child}"
    return f"{INSERT}{INTO}{table_name}{table_columns}{project_statement([], child)}"


def insert_overwrite_statement(
    table_name: str, child: str, column_names: Optional[Iterable[str]] = None
) -> str:
    table_columns = f"({COMMA.join(column_names)})" if column_names else EMPTY_STRING
    if is_sql_select_statement(child):
        if WITH_STMT_SUFFIX in child:
            cte_stmt, rest = child.split(WITH_STMT_SUFFIX)
            return f"{cte_stmt}{WITH_STMT_SUFFIX}{INSERT}{OVERWRITE}{table_name}{table_columns}{SPACE}{rest}"
        return f"{INSERT}{OVERWRITE}{table_name}{table_columns}{SPACE}{child}"
    return (
        f"{INSERT}{OVERWRITE}{table_name}{table_columns}{project_statement([], child)}"
    )


def batch_insert_into_statement(table_name: str, column_names: List[str]) -> str:
    return (
        f"{INSERT}{INTO}{table_name}"
        f"{LEFT_PARENTHESIS}{COMMA.join(column_names)}{RIGHT_PARENTHESIS}"
        f"{VALUES}{LEFT_PARENTHESIS}"
        f"{COMMA.join([QUESTION_MARK] * len(column_names))}{RIGHT_PARENTHESIS}"
    )


def create_table_as_select_statement(
    table_name: str,
    child: str,
    column_definition: str,
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
    clustering_key: Optional[Iterable[str]] = None,
) -> str:
    cluster_by_clause = (
        (CLUSTER_BY + LEFT_PARENTHESIS + COMMA.join(clustering_key) + RIGHT_PARENTHESIS)
        if clustering_key
        else EMPTY_STRING
    )
    return (
        f"{CREATE}{TABLE}"
        f"{IF + NOT + EXISTS if not replace and not error else EMPTY_STRING}"
        f" {table_name}"
        f"{cluster_by_clause} {AS}{project_statement([], child)}"
    )


def limit_statement(
    row_count: str, offset: str, child: str, on_top_of_order_by: bool
) -> str:
    return (
        f"{child if on_top_of_order_by else project_statement([], child)}"
        + LIMIT
        + row_count
        + OFFSET
        + offset
    )


def schema_cast_seq(schema: List[Attribute]) -> List[str]:
    res = []
    for index, attr in enumerate(schema):
        name = (
            DOLLAR
            + str(index + 1)
            + DOUBLE_COLON
            + convert_data_type_to_name(attr.datatype)
        )
        res.append(name + AS + quote_name(attr.name))
    return res


def schema_cast_named(schema: List[Tuple[str, str]]) -> List[str]:
    return [s[0] + AS + quote_name_without_upper_casing(s[1]) for s in schema]


def infer_schema_statement(path: str, file_format_name: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + TABLE
        + LEFT_PARENTHESIS
        + INFER_SCHEMA
        + LEFT_PARENTHESIS
        + LOCATION
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + path
        + SINGLE_QUOTE
        + COMMA
        + FILE_FORMAT
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + file_format_name
        + SINGLE_QUOTE
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def _quote_string_literal(s: str) -> str:
    return f"'{s}'"


def _volume_identifier_clause(volume_path: VolumePath) -> str:
    if volume_path.kind == VolumeKind.EXTERNAL:
        return VOLUME + ".".join(f"`{x}`" for x in volume_path.volume_name)
    elif volume_path.kind == VolumeKind.TABLE:
        return TABLE + VOLUME + ".".join(f"`{x}`" for x in volume_path.volume_name)
    elif volume_path.kind == VolumeKind.USER:
        return USER + VOLUME
    else:
        raise ValueError(f"Illegal volume kind: {volume_path.kind}")


def _volume_resource_clause(volume_path: VolumePath) -> str:
    if volume_path.path.endswith("/"):
        return SUBDIRECTORY + _quote_string_literal(volume_path.path)
    else:
        return FILE + _quote_string_literal(volume_path.path)


class FileOperationStatements:
    @staticmethod
    def get(volume_path: VolumePath, local_path: Path, options: Dict[str, str]) -> str:
        return (
            GET
            + _volume_identifier_clause(volume_path)
            + SPACE
            + _volume_resource_clause(volume_path)
            + SPACE
            + TO
            + _quote_string_literal(local_path)
            + SPACE
            + to_options_clause(options)
        )

    @staticmethod
    def put(local_path: Path, volume_path: VolumePath, options: Dict[str, str]) -> str:
        return (
            PUT
            + _quote_string_literal(local_path)
            + SPACE
            + TO
            + _volume_identifier_clause(volume_path)
            + SPACE
            + _volume_resource_clause(volume_path)
            + SPACE
            + to_options_clause(options)
        )

    @staticmethod
    def list_(volume_path: VolumePath, options: Dict[str, str]) -> str:
        if not volume_path.path.endswith("/"):
            raise ValueError("volume path must be a directory")
        return (
            LIST
            + _volume_identifier_clause(volume_path)
            + SPACE
            + _volume_resource_clause(volume_path)
            + SPACE
            + to_options_clause(options)
        )

    @staticmethod
    def delete(volume_path: VolumePath) -> str:
        return (
            DELETE
            + _volume_identifier_clause(volume_path)
            + SPACE
            + _volume_resource_clause(volume_path)
        )

    @staticmethod
    def copy_files(dest: VolumePath, src: VolumePath, options: Dict[str, str]) -> str:
        if not dest.path.endswith("/"):
            raise ValueError("destination path must be a directory")
        if src.path.endswith("/"):
            src_res = SUBDIRECTORY + _quote_string_literal(src.path)
        else:
            src_res = (
                FILES
                + LEFT_PARENTHESIS
                + _quote_string_literal(src.path)
                + RIGHT_PARENTHESIS
            )
        return (
            COPY
            + FILES
            + INTO
            + _volume_identifier_clause(dest)
            + SPACE
            + _volume_resource_clause(dest)
            + SPACE
            + FROM
            + _volume_identifier_clause(src)
            + SPACE
            + src_res
            + SPACE
            + to_options_clause(options)
        )


def convert_value_to_sql_option(value: Optional[Union[str, bool, int, float]]) -> str:
    if isinstance(value, str):
        if len(value) > 1 and is_single_quoted(value):
            return value
        else:
            value = value.replace(
                "'", "''"
            )  # escape single quotes before adding a pair of quotes
            return f"'{value}'"
    else:
        return str(value)


def get_options_statement(options: Dict[str, Any]) -> str:
    return (
        SPACE
        + SPACE.join(
            f"{k}{EQUALS}{convert_value_to_sql_option(v)}"
            for k, v in options.items()
            if v is not None
        )
        + SPACE
    )


def drop_file_format_if_exists_statement(format_name: str) -> str:
    return DROP + FILE + FORMAT + IF + EXISTS + format_name


def to_table_options_clause(options: Optional[Dict[str, Any]]) -> str:
    # TODO escape special characters in options
    return (
        "OPTIONS (" + ", ".join(f"'{k}' = '{str(v)}'" for k, v in options.items()) + ")"
        if options
        else ""
    )


def _volume_source_schema_clause(schema: Optional[List[Attribute]]) -> str:
    if not schema:
        return EMPTY_STRING
    return LEFT_PARENTHESIS + attribute_to_schema_string(schema) + RIGHT_PARENTHESIS


def _volume_source_filter_clause(
    volume_path: VolumePath, pattern: Optional[str]
) -> str:
    if volume_path.path.endswith("/"):
        resource_clause = SUBDIRECTORY + _quote_string_literal(volume_path.path)
        if pattern:
            resource_clause += REGEXP + _quote_string_literal(pattern)
    else:
        resource_clause = (
            FILES
            + LEFT_PARENTHESIS
            + _quote_string_literal(volume_path.path)
            + RIGHT_PARENTHESIS
        )
    return resource_clause


def select_from_volume_statement(
    path: str,
    file_format: str,
    pattern: Optional[str],
    schema: Optional[List[Attribute]],
    file_format_options: Dict[str, Any],
) -> str:
    volume_path = VolumePath(path)
    return (
        SELECT
        + STAR
        + FROM
        + _volume_identifier_clause(volume_path)
        + SPACE
        + _volume_source_schema_clause(schema)
        + USING
        + file_format
        + SPACE
        + to_table_options_clause(file_format_options)
        + SPACE
        + _volume_source_filter_clause(volume_path, pattern)
    )


def unary_expression(
    child: str, sql_operator: str, operator_first: bool, parse_local_name: bool = False
) -> str:
    return (
        (sql_operator + SPACE + child)
        if operator_first
        else (child + SPACE + sql_operator)
    )


def math_unary_expression(
    child: str, sql_operator: str, operator_first: bool, parse_local_name: bool = False
) -> str:
    return sql_operator + LEFT_PARENTHESIS + child + RIGHT_PARENTHESIS


def window_expression(window_function: str, window_spec: str, parse_local_name) -> str:
    return (
        (unquote_if_safe(window_function) if parse_local_name else window_function)
        + OVER
        + LEFT_PARENTHESIS
        + window_spec
        + RIGHT_PARENTHESIS
    )


def window_spec_expression(
    partition_exprs: List[str], order_exprs: List[str], frame_spec: str
) -> str:
    return f"{partition_spec(partition_exprs)}{SPACE}{order_by_spec(order_exprs)}{SPACE}{frame_spec}".strip()


def specified_window_frame_expression(frame_type: str, lower: str, upper: str) -> str:
    return SPACE + frame_type + BETWEEN + lower + AND + upper + SPACE


def window_frame_boundary_expression(offset: str, is_following: bool) -> str:
    return offset + (FOLLOWING if is_following else PRECEDING)


def rank_related_function_expression(
    func_name: str,
    expr: str,
    offset: int,
    default: Optional[str],
    ignore_nulls: Optional[bool],
    parse_local_name: bool = False,
) -> str:
    return (
        (unquote_if_safe(func_name) if parse_local_name else func_name)
        + LEFT_PARENTHESIS
        + (unquote_if_safe(expr) if parse_local_name else expr)
        + (COMMA + str(offset) if offset else EMPTY_STRING)
        + (COMMA + default if default else EMPTY_STRING)
        + (COMMA + str(ignore_nulls) if ignore_nulls else EMPTY_STRING)
        + RIGHT_PARENTHESIS
    )


def cast_expression(child: str, datatype: DataType, try_: bool = False) -> str:
    return (
        # (TRY_CAST if try_ else CAST)
        "CAST"
        + LEFT_PARENTHESIS
        + child
        + AS
        + convert_data_type_to_name(datatype)
        + RIGHT_PARENTHESIS
    )


def order_expression(name: str, direction: str, null_ordering: str) -> str:
    return name + SPACE + direction + SPACE + null_ordering


def create_or_replace_view_statement(name: str, child: str, is_temp: bool) -> str:
    return (
        CREATE
        + OR
        + REPLACE
        # + f"{TEMPORARY if is_temp else EMPTY_STRING}"
        + VIEW
        + name
        + AS
        + project_statement([], child)
    )


def create_or_replace_dynamic_table_statement(
    name: str, warehouse: str, lag: str, child: str
) -> str:
    return (
        CREATE
        + OR
        + REPLACE
        + DYNAMIC
        + TABLE
        + name
        # TODO(XXX) we dont support lag and warehouse yet
        # + f"{LAG + EQUALS + convert_value_to_sql_option(lag)}"
        # + f"{WAREHOUSE + EQUALS + warehouse}"
        + AS
        + project_statement([], child)
    )


def pivot_statement(
    pivot_column: str, pivot_values: List[str], aggregate: str, child: str
) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + PIVOT
        + LEFT_PARENTHESIS
        + aggregate
        + FOR
        + pivot_column
        + IN
        + LEFT_PARENTHESIS
        + COMMA.join(pivot_values)
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def unpivot_statement(
    value_column: str, name_column: str, column_list: List[str], child: str
) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + UNPIVOT
        + LEFT_PARENTHESIS
        + value_column
        + FOR
        + name_column
        + IN
        + LEFT_PARENTHESIS
        + COMMA.join(column_list)
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def rename_statement(column_map: Dict[str, str], child: str) -> str:
    return (
        SELECT
        + STAR
        + RENAME
        + LEFT_PARENTHESIS
        + COMMA.join([f"{before}{AS}{after}" for before, after in column_map.items()])
        + RIGHT_PARENTHESIS
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
    )


def to_options_clause(options: Optional[Dict[str, Any]]) -> str:
    def _value(v: Any) -> str:
        if isinstance(v, list):
            return "(" + ", ".join(_value(e) for e in v) + ")"
        return v

    return " ".join(f"{k} = {_value(v)}" for k, v in options.items()) if options else ""


def copy_into_table(
    table_name: str,
    file_path: str,
    file_format_type: str,
    format_type_options: Dict[str, Any],
    *,
    copy_options: Dict[str, Any],
    pattern: Optional[str],
    files: Optional[List[str]] = None,
    column_names: Optional[List[str]] = None,
    transformations: Optional[List[str]] = None,
    user_schema: Optional[StructType] = None,
) -> str:
    volume_path = VolumePath(file_path)
    source = (
        _volume_identifier_clause(volume_path)
        + SPACE
        + _volume_source_schema_clause(user_schema._to_attributes())
        + SPACE
        + USING
        + file_format_type
        + SPACE
        + to_table_options_clause(format_type_options)
        + SPACE
        + _volume_source_filter_clause(volume_path, pattern)
    )
    if transformations:
        source = (
            LEFT_PARENTHESIS
            + SELECT
            + COMMA.join(transformations)
            + FROM
            + source
            + RIGHT_PARENTHESIS
        )
    return (
        COPY
        + INTO
        + table_name
        + (
            (LEFT_PARENTHESIS + COMMA.join(column_names) + RIGHT_PARENTHESIS)
            if column_names
            else EMPTY_STRING
        )
        + FROM
        + source
        + to_options_clause(copy_options)
    )


def copy_into_volume_statement(
    query: str,
    volume_path: VolumePath,
    partition_by: Optional[str] = None,
    file_format_type: Optional[str] = None,
    format_type_options: Optional[Dict[str, Any]] = None,
    **copy_options: Any,
) -> str:
    if partition_by is not None:
        raise ValueError("partition_by is not supported for copy into volume")
    if not volume_path.path.endswith("/"):
        raise ValueError("volume path does not end with '/'")
    return (
        COPY
        + INTO
        + _volume_identifier_clause(volume_path)
        + SUBDIRECTORY
        + _quote_string_literal(volume_path.path)
        + FROM
        + LEFT_PARENTHESIS
        + query
        + RIGHT_PARENTHESIS
        + FILE_FORMAT
        + EQUALS
        + LEFT_PARENTHESIS
        + TYPE
        + EQUALS
        + (file_format_type or "CSV")
        + SPACE
        + to_options_clause(format_type_options)
        + RIGHT_PARENTHESIS
        + SPACE
        + to_options_clause(copy_options)
    )


def update_statement(
    table_name: str,
    assignments: Dict[str, str],
    condition: Optional[str],
    source_data: Optional[str],
) -> str:
    return (
        UPDATE
        + table_name
        + SET
        + COMMA.join([k + EQUALS + v for k, v in assignments.items()])
        + (
            (FROM + LEFT_PARENTHESIS + source_data + RIGHT_PARENTHESIS)
            if source_data
            else EMPTY_STRING
        )
        + ((WHERE + condition) if condition else EMPTY_STRING)
    )


def delete_statement(
    table_name: str, condition: Optional[str], source_data: Optional[str]
) -> str:
    return (
        DELETE
        + FROM
        + table_name
        + (
            (USING + LEFT_PARENTHESIS + source_data + RIGHT_PARENTHESIS)
            if source_data
            else EMPTY_STRING
        )
        + ((WHERE + condition) if condition else EMPTY_STRING)
    )


def insert_merge_statement(
    condition: Optional[str], keys: List[str], values: List[str]
) -> str:
    return (
        WHEN
        + NOT
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + INSERT
        + (
            (LEFT_PARENTHESIS + COMMA.join(keys) + RIGHT_PARENTHESIS)
            if keys
            else EMPTY_STRING
        )
        + VALUES
        + LEFT_PARENTHESIS
        + COMMA.join(values)
        + RIGHT_PARENTHESIS
    )


def update_merge_statement(condition: Optional[str], assignment: Dict[str, str]) -> str:
    return (
        WHEN
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + UPDATE
        + SET
        + COMMA.join([k + EQUALS + v for k, v in assignment.items()])
    )


def delete_merge_statement(condition: Optional[str]) -> str:
    return (
        WHEN
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + DELETE
    )


def merge_statement(
    table_name: str, source: str, join_expr: str, clauses: List[str]
) -> str:
    return (
        MERGE
        + INTO
        + table_name
        + USING
        + LEFT_PARENTHESIS
        + source
        + RIGHT_PARENTHESIS
        + ON
        + join_expr
        + EMPTY_STRING.join(clauses)
    )


def truncate_table_if_exists_statement(table_name: str) -> str:
    return TRUNCATE + TABLE + table_name


def drop_table_if_exists_statement(table_name: str) -> str:
    return DROP + TABLE + IF + EXISTS + table_name


def attribute_to_schema_string(attributes: List[Attribute]) -> str:
    return COMMA.join(
        attr.name
        + SPACE
        + convert_data_type_to_name(attr.datatype)
        + (NOT_NULL if not attr.nullable else EMPTY_STRING)
        for attr in attributes
    )


def schema_value_statement(output: List[Attribute]) -> str:
    return SELECT + COMMA.join(
        [
            schema_expression(attr.datatype, attr.nullable) + AS + quote_name(attr.name)
            for attr in output
        ]
    )


def list_agg(col: str, delimiter: str, is_distinct: bool) -> str:
    return (
        LISTAGG
        + LEFT_PARENTHESIS
        + f"{DISTINCT if is_distinct else EMPTY_STRING}"
        + col
        + COMMA
        + delimiter
        + RIGHT_PARENTHESIS
    )


def generator(row_count: int) -> str:
    return (
        GENERATOR
        + LEFT_PARENTHESIS
        + ROW_COUNT
        + RIGHT_ARROW
        + str(row_count)
        + RIGHT_PARENTHESIS
    )


def table(content: str) -> str:
    return TABLE + LEFT_PARENTHESIS + content + RIGHT_PARENTHESIS


def single_quote(value: str) -> str:
    if value.startswith(SINGLE_QUOTE) and value.endswith(SINGLE_QUOTE):
        return value
    else:
        return SINGLE_QUOTE + value + SINGLE_QUOTE


def quote_name_without_upper_casing(name: str) -> str:
    return BACKTICK + escape_quotes(name) + BACKTICK


def unquote_if_quoted(string):
    return string[1:-1].replace('""', '"') if ALREADY_QUOTED.match(string) else string


# Most integer types map to number(38,0)
# https://doc.clickzetta.com/
# data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
def number(precision: int = 38, scale: int = 0) -> str:
    return (
        NUMBER
        + LEFT_PARENTHESIS
        + str(precision)
        + COMMA
        + str(scale)
        + RIGHT_PARENTHESIS
    )


def get_file_format_spec(
    file_format_type: str, format_type_options: Dict[str, Any]
) -> str:
    file_format_name = format_type_options.get("FORMAT_NAME")
    file_format_str = FILE_FORMAT + EQUALS + LEFT_PARENTHESIS
    if file_format_name is None:
        file_format_str += TYPE + EQUALS + file_format_type
        if format_type_options:
            file_format_str += (
                SPACE + get_options_statement(format_type_options) + SPACE
            )
    else:
        file_format_str += FORMAT_NAME + EQUALS + file_format_name
    file_format_str += RIGHT_PARENTHESIS
    return file_format_str


def cte_statement(queries: List[str], table_names: List[str]) -> str:
    result = COMMA.join(
        f"{table_name}{AS}{LEFT_PARENTHESIS}{query}{RIGHT_PARENTHESIS}"
        for query, table_name in zip(queries, table_names)
    )
    return f"{WITH}{result}{WITH_STMT_SUFFIX}"


def create_udf_statement(
    schema_name: str, handler_module: str, handler_class: str, handler_file: str
) -> str:
    return (
        CREATE
        + EXTERNAL
        + FUNCTION
        + f"{schema_name}.{handler_module}"
        + AS
        + single_quote(f"{handler_module}.{handler_class}")
        + USING
        + FILE
        + single_quote(handler_file)
        + SPACE
        + WITH
        + PROPERTIES
        + LEFT_PARENTHESIS
        + single_quote("remote.udf.protocol")
        + EQUALS
        + single_quote("ipc.arrow.v0")
        + COMMA
        + single_quote("remote.udf.api")
        + EQUALS
        + single_quote("python3.mc.v0")
        + RIGHT_PARENTHESIS
    )
