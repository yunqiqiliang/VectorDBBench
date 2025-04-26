#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import math
from array import array
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, List, Optional

from clickzetta.zettapark._internal.type_utils import convert_data_type_to_name
from clickzetta.zettapark._internal.utils import unquote_name
from clickzetta.zettapark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    MapType,
    NullType,
    StringType,
    StructType,
    TimestampTimeZone,
    TimestampType,
    VectorType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

MILLIS_PER_DAY = 24 * 3600 * 1000
MICROS_PER_MILLIS = 1000


def str_to_sql(value: str) -> str:
    sql_str = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    return f"'{sql_str}'"


def _to_vector_literal(value: Iterable, datatype: VectorType) -> str:
    if datatype.element_type == "tinyint":
        return "vector(" + ", ".join(str(int(i)) + "Y" for i in value) + ")"
    elif datatype.element_type == "int":
        return "vector(" + ", ".join(str(int(i)) for i in value) + ")"
    elif datatype.element_type == "float":
        return "vector(" + ", ".join(str(float(i)) + "F" for i in value) + ")"
    raise TypeError(f"Unsupported VectorType: {datatype}")


def to_sql(value: Any, datatype: DataType, from_values_statement: bool = False) -> str:
    """Convert a value with DataType to a compatible sql"""

    if isinstance(datatype, NullType) and value is None:
        return "NULL"

    if value is None:
        return f"CAST(NULL AS {convert_data_type_to_name(datatype)})"

    # Not nulls
    if isinstance(value, str) and isinstance(datatype, StringType):
        # If this is used in a values statement (e.g., create_dataframe),
        # the sql value has to be casted to make sure the varchar length
        # will not be limited.
        return (
            f"CAST({str_to_sql(value)} AS {convert_data_type_to_name(datatype)})"
            if from_values_statement
            else str_to_sql(value)
        )
    if isinstance(datatype, StringType):
        return f"CAST({to_sql_without_cast(value, None)} AS {convert_data_type_to_name(datatype)})"

    if isinstance(datatype, _IntegralType):
        return f"CAST({value} AS {convert_data_type_to_name(datatype)})"

    if isinstance(datatype, BooleanType):
        return f"CAST({value} AS {convert_data_type_to_name(datatype)})"

    if isinstance(value, float) and isinstance(datatype, _FractionalType):
        if math.isnan(value):
            cast_value = "'NAN'"
        elif math.isinf(value) and value > 0:
            cast_value = "'INF'"
        elif math.isinf(value) and value < 0:
            cast_value = "'-INF'"
        else:
            cast_value = f"'{value}'"
        return f"CAST({cast_value} AS {convert_data_type_to_name(datatype)})"

    if isinstance(value, Decimal) and isinstance(datatype, DecimalType):
        format_ = f"{{:.{datatype.scale}f}}"
        value = format_.format(value, datatype=datatype)
        return f"CAST({value} AS {convert_data_type_to_name(datatype)})"

    if isinstance(datatype, DateType):
        if isinstance(value, int):
            # add value as number of days to 1970-01-01
            target_date = date(1970, 1, 1) + timedelta(days=value)
            return f"DATE '{target_date.isoformat()}'"
        elif isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        elif isinstance(value, str):
            return f"DATE '{value}'"

    if isinstance(datatype, TimestampType):
        if isinstance(value, (int, str, datetime)):
            if isinstance(value, int):
                # add value as microseconds to 1970-01-01 00:00:00.00.
                value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                    microseconds=value
                )
            if datatype.tz == TimestampTimeZone.NTZ:
                return f"TIMESTAMP_NTZ '{value}'"
            elif datatype.tz == TimestampTimeZone.LTZ:
                return f"TIMESTAMP_LTZ '{value}'"
            # NOTE: we dont support TIMESTAMP_TZ yet.
            # elif datatype.tz == TimestampTimeZone.TZ:
            #     return f"CAST('{value}' AS TIMESTAMP_TZ)"
            else:
                return f"TIMESTAMP '{value}'"

    if isinstance(value, (list, bytes, bytearray)) and isinstance(datatype, BinaryType):
        return f"unhex('{bytes(value).hex()}')"

    if isinstance(value, (list, tuple, array)) and isinstance(datatype, ArrayType):
        params = [to_sql(element, datatype.element_type) for element in value]
        return (
            f"CAST(ARRAY({','.join(params)}) AS {convert_data_type_to_name(datatype)})"
        )

    if isinstance(value, dict) and isinstance(datatype, MapType):
        params = []
        for k, v in value.items():
            params.append(to_sql(k, datatype.key_type))
            params.append(to_sql(v, datatype.value_type))
        return f"CAST(MAP({','.join(params)}) AS {convert_data_type_to_name(datatype)})"

    if isinstance(value, (tuple, dict)) and isinstance(datatype, StructType):
        if isinstance(value, dict):
            value_dict = value
        elif hasattr(value, "_asdict"):
            value_dict = value._asdict()
        else:
            value_dict = {
                unquote_name(field.name): v for field, v in zip(datatype.fields, value)
            }
        params = [
            to_sql(value_dict.get(unquote_name(field.name), None), field.datatype)
            for field in datatype.fields
        ]
        return (
            f"CAST(STRUCT({','.join(params)}) AS {convert_data_type_to_name(datatype)})"
        )

    if isinstance(datatype, VectorType) and isinstance(value, Iterable):
        return _to_vector_literal(value, datatype)

    raise TypeError(f"Unsupported datatype {datatype}, value {value} by to_sql()")


def schema_expression(data_type: DataType, is_nullable: bool) -> str:
    if is_nullable:
        return "CAST(NULL AS " + convert_data_type_to_name(data_type) + ")"

    if isinstance(data_type, _NumericType):
        return f"CAST(0 AS {convert_data_type_to_name(data_type)})"
    if isinstance(data_type, StringType):
        return f"CAST('a' AS {convert_data_type_to_name(data_type)})"
    if isinstance(data_type, BinaryType):
        return f"CAST('01' AS {convert_data_type_to_name(data_type)})"
    if isinstance(data_type, DateType):
        return "DATE '2020-9-16'"
    if isinstance(data_type, BooleanType):
        return "true"
    if isinstance(data_type, TimestampType):
        if data_type.tz == TimestampTimeZone.NTZ:
            return "TIMESTAMP_NTZ '2020-09-16 06:30:00'"
        elif data_type.tz == TimestampTimeZone.LTZ:
            return "TIMESTAMP_LTZ '2020-09-16 06:30:00'"
        # elif data_type.tz == TimestampTimeZone.TZ: # not supported
        #     return "to_timestamp_tz('2020-09-16 06:30:00')"
        else:
            return "TIMESTAMP '2020-09-16 06:30:00'"
    if isinstance(data_type, ArrayType):
        return f"ARRAY({schema_expression(data_type.element_type, False)})"
    if isinstance(data_type, MapType):
        key_expr = schema_expression(data_type.key_type, False)
        value_expr = schema_expression(data_type.value_type, False)
        return f"MAP({key_expr}, {value_expr})"
    if isinstance(data_type, StructType):
        params: List[str] = []
        for f in data_type.fields:
            params.append(f"'{unquote_name(f.name)}'")
            params.append(schema_expression(f.datatype, f.nullable))
        return "NAMED_STRUCT(" + ", ".join(params) + ")"
    if isinstance(data_type, VectorType):

        def zero_gen(count: int, suffix: str):
            for _ in range(count):
                yield str(0) + suffix

        if data_type.element_type == "tinyint":
            return "vector(" + ", ".join(zero_gen(data_type.dimension, "Y")) + ")"
        if data_type.element_type == "int":
            return "vector(" + ", ".join(zero_gen(data_type.dimension, "")) + ")"
        elif data_type.element_type == "float":
            return "vector(" + ", ".join(zero_gen(data_type.dimension, "F")) + ")"
        else:
            raise TypeError(f"Invalid vector element type: {data_type.element_type}")

    raise Exception(f"Unsupported data type: {data_type.__class__.__name__}")


def to_sql_without_cast(value: Any, datatype: Optional[DataType]) -> str:
    if value is None:
        return "NULL"
    if isinstance(datatype, StringType):
        return f"'{value}'"
    if isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    return str(value)
