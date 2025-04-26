#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

from clickzetta.connector.v0 import sql_types
from clickzetta.zettapark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    VectorType,
)


class _SqlTypeToDataTypeVisitor(sql_types.SqlTypeVisitor):
    def visit_any(self, t: sql_types.SqlType):
        raise NotImplementedError(f"Unsupported type: {t.__class__.__name__}")

    def visit_void(self, t: sql_types.VoidType):
        return NullType()

    def visit_tinyint(self, t: sql_types.TinyintType):
        return ByteType()

    def visit_smallint(self, t: sql_types.SmallintType):
        return ShortType()

    def visit_int(self, t: sql_types.IntType):
        return IntegerType()

    def visit_bigint(self, t: sql_types.BigintType):
        return LongType()

    def visit_float(self, t: sql_types.FloatType):
        return FloatType()

    def visit_double(self, t: sql_types.DoubleType):
        return DoubleType()

    def visit_decimal(self, t: sql_types.DecimalType):
        return DecimalType(t.precision, t.scale)

    def visit_boolean(self, t: sql_types.BooleanType):
        return BooleanType()

    def visit_char(self, t: sql_types.CharType):
        return StringType(t.length)

    def visit_varchar(self, t: sql_types.VarcharType):
        return StringType(t.length)

    def visit_string(self, t: sql_types.StringType):
        return StringType()

    def visit_binary(self, t: sql_types.BinaryType):
        return BinaryType()

    def visit_date(self, t: sql_types.DateType):
        return DateType()

    def visit_timestamp_ltz(self, t: sql_types.TimestampLtzType):
        return TimestampType(TimestampTimeZone.LTZ)

    def visit_timestamp_ntz(self, t: sql_types.TimestampNtzType):
        return TimestampType(TimestampTimeZone.NTZ)

    def visit_array(self, t: sql_types.ArrayType):
        e = t.element_type.accept(self)
        return ArrayType(e)

    def visit_map(self, t: sql_types.MapType):
        k = t.key_type.accept(self)
        v = t.value_type.accept(self)
        return MapType(k, v)

    def visit_struct(self, t: sql_types.StructType):
        fields = []
        for f in t.fields:
            ft: sql_types.SqlType = f.type_
            fields.append(StructField(f.name, ft.accept(self), ft.nullable))
        return StructType(fields)

    def visit_vector(self, t: sql_types.VectorType):
        return VectorType(t.number_type.name, t.dimension)

    def visit_json(self, t: sql_types.JsonType):
        return StringType()


SQL_TYPE_TO_DATA_TYPE_VISITOR = _SqlTypeToDataTypeVisitor()


def sql_type_to_data_type(t: sql_types.SqlType) -> DataType:
    return t.accept(SQL_TYPE_TO_DATA_TYPE_VISITOR)
