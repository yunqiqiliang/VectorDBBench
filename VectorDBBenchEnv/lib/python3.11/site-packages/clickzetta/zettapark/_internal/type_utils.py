#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
import ast
import ctypes
import datetime
import decimal
import re
import sys
import typing  # noqa: F401
from array import array
from typing import (  # noqa: F401
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    NewType,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import clickzetta.zettapark.types  # type: ignore
from clickzetta.zettapark._connector import ResultMetadata, installed_pandas
from clickzetta.zettapark._internal.type_mapping import sql_type_to_data_type
from clickzetta.zettapark._internal.type_parser import ParsingError, parse_data_type
from clickzetta.zettapark.row import Row
from clickzetta.zettapark.types import (
    LTZ,
    NTZ,
    TZ,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DataTypeVisitor,
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
    Timestamp,
    TimestampTimeZone,
    TimestampType,
    VectorType,
    _AtomicType,
    _NumericType,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
try:
    from typing import Iterable  # noqa: F401
except ImportError:
    from collections.abc import Iterable  # noqa: F401

if installed_pandas:
    from clickzetta.zettapark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeries,
        PandasSeriesType,
    )


def convert_metadata_to_data_type(metadata: ResultMetadata) -> DataType:
    return sql_type_to_data_type(metadata.sql_type)


def convert_data_type_to_name(datatype: DataType) -> str:
    if isinstance(datatype, DecimalType):
        return f"decimal({datatype.precision},{datatype.scale})"
    if isinstance(datatype, IntegerType):
        return "int"
    if isinstance(datatype, ShortType):
        return "smallint"
    if isinstance(datatype, ByteType):
        return "tinyint"
    if isinstance(datatype, LongType):
        return "bigint"
    if isinstance(datatype, FloatType):
        return "float"
    if isinstance(datatype, DoubleType):
        return "double"
    if isinstance(datatype, StringType):
        return f"varchar({datatype.length})" if datatype.length else "string"
    if isinstance(datatype, NullType):
        return "void"
    if isinstance(datatype, BooleanType):
        return "boolean"
    if isinstance(datatype, DateType):
        return "date"
    if isinstance(datatype, TimestampType):
        if datatype.tz == TimestampTimeZone.NTZ:
            return "timestamp_ntz"
        elif datatype.tz == TimestampTimeZone.LTZ:
            return "timestamp_ltz"
    if isinstance(datatype, BinaryType):
        return "binary"
    if isinstance(datatype, ArrayType):
        element_type = convert_data_type_to_name(datatype.element_type)
        return f"array<{element_type}>"
    if isinstance(datatype, MapType):
        key_type = convert_data_type_to_name(datatype.key_type)
        value_type = convert_data_type_to_name(datatype.value_type)
        return f"map<{key_type},{value_type}>"
    if isinstance(datatype, VectorType):
        return f"vector({datatype.element_type},{datatype.dimension})"
    if isinstance(datatype, StructType):
        fields = ",".join(
            (
                f"{f.name}:{convert_data_type_to_name(f.datatype)}"
                f"{' not null' if not f.nullable else ''}"
            )
            for f in datatype.fields
        )
        return f"struct<{fields}>"
    raise TypeError(f"Unsupported data type: {datatype.__class__.__name__}")


# Mapping Python types to DataType
NoneType = type(None)
PYTHON_TO_DATA_TYPE_MAPPINGS = {
    NoneType: NullType,
    bool: BooleanType,
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    bytes: BinaryType,
}


VALID_PYTHON_TYPES_FOR_LITERAL_VALUE = (
    *PYTHON_TO_DATA_TYPE_MAPPINGS.keys(),
    list,
    tuple,
    dict,
)
VALID_ZETTAPARK_TYPES_FOR_LITERAL_VALUE = (
    *PYTHON_TO_DATA_TYPE_MAPPINGS.values(),
    _NumericType,
    ArrayType,
    MapType,
)

# Mapping Python array types to DataType
ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS = {
    "b": ctypes.c_byte,
    "h": ctypes.c_short,
    "i": ctypes.c_int,
    "l": ctypes.c_long,
    "q": ctypes.c_longlong,
}

ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS = {
    "B": ctypes.c_ubyte,
    "H": ctypes.c_ushort,
    "I": ctypes.c_uint,
    "L": ctypes.c_ulong,
    "Q": ctypes.c_ulonglong,
}


def int_size_to_type(size: int) -> Type[DataType]:
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    if size <= 16:
        return ShortType
    if size <= 32:
        return IntegerType
    if size <= 64:
        return LongType


# The list of all supported array typecodes, is stored here
ARRAY_TYPE_MAPPINGS = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    "f": FloatType,
    "d": DoubleType,
}

# compute array typecode mappings for signed integer types
for _typecode in ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS.keys():
    size = ctypes.sizeof(ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS[_typecode]) * 8
    dt = int_size_to_type(size)
    if dt is not None:
        ARRAY_TYPE_MAPPINGS[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS[_typecode]) * 8 + 1
    dt = int_size_to_type(size)
    if dt is not None:
        ARRAY_TYPE_MAPPINGS[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    ARRAY_TYPE_MAPPINGS["u"] = StringType


def infer_type(obj: Any) -> DataType:
    """Infer the DataType from obj"""
    if obj is None:
        return NullType()

    datatype = PYTHON_TO_DATA_TYPE_MAPPINGS.get(type(obj))
    if datatype is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    elif datatype is not None:
        return datatype()

    if isinstance(obj, dict):
        key_type, value_type = NullType(), NullType()
        for key, value in obj.items():
            if key is not None:
                key_type = merge_type(key_type, infer_type(key))
            if value is not None:
                value_type = merge_type(value_type, infer_type(value))
        return MapType(key_type, value_type)
    elif isinstance(obj, tuple):
        if hasattr(obj, "_fields"):
            # named tuple
            return StructType(
                [StructField(f, infer_type(v)) for f, v in zip(obj._fields, obj)]
            )
        # tuple
        return StructType(
            [StructField(f"_{i + 1}", infer_type(v)) for i, v in enumerate(obj)]
        )
    elif isinstance(obj, list):
        elem_type = NullType()
        for v in obj:
            if v is not None:
                elem_type = merge_type(elem_type, infer_type(v))
        return ArrayType(elem_type)
    elif isinstance(obj, array):
        if obj.typecode in ARRAY_TYPE_MAPPINGS:
            return ArrayType(ARRAY_TYPE_MAPPINGS[obj.typecode]())
        else:
            raise TypeError(f"not supported type: array({obj.typecode})")
    else:
        raise TypeError(f"not supported type: {type(obj)}")


def infer_schema(
    row: Union[Dict, List, Tuple], names: Optional[List] = None
) -> StructType:
    if row is None or (isinstance(row, (tuple, list, dict)) and not row):
        items = zip(names if names else ["_1"], [None])
    else:
        if isinstance(row, dict):
            items = row.items()
        elif isinstance(row, (tuple, list)):
            row_fields = getattr(row, "_fields", None)
            if row_fields:  # Row or namedtuple
                items = zip(row_fields, row)
            else:
                if names is None:
                    names = [f"_{i}" for i in range(1, len(row) + 1)]
                elif len(names) < len(row):
                    names.extend(f"_{i}" for i in range(len(names) + 1, len(row) + 1))
                items = zip(names, row)
        elif isinstance(row, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            items = zip(names if names else ["_1"], [row])
        else:
            raise TypeError(f"Can not infer schema for type: {type(row)}")

    fields = []
    for k, v in items:
        try:
            fields.append(StructField(k, infer_type(v), v is None))
        except TypeError as e:
            raise TypeError(f"Unable to infer the type of the field {k}.") from e
    return StructType(fields)


def merge_type(a: DataType, b: DataType, name: Optional[str] = None) -> DataType:
    # null type
    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif isinstance(a, StringType) and isinstance(b, _AtomicType):
        return a
    elif isinstance(b, StringType) and isinstance(a, _AtomicType):
        return b
    elif type(a) is not type(b):
        err_msg = f"Cannot merge type {type(a)} and {type(b)}"
        if name:
            err_msg = f"{name}: {err_msg}"
        raise TypeError(err_msg)

    # same type
    if isinstance(a, StructType):
        name_to_datatype_b = {f.name: f.datatype for f in b.fields}
        name_to_nullable_b = {f.name: f.nullable for f in b.fields}
        fields = [
            StructField(
                f.name,
                merge_type(
                    f.datatype,
                    name_to_datatype_b.get(f.name, NullType()),
                    name=f"field {f.name} in {name}" if name else f"field {f.name}",
                ),
                f.nullable or name_to_nullable_b.get(f.name, True),
            )
            for f in a.fields
        ]
        names = {f.name for f in fields}
        for n in name_to_datatype_b:
            if n not in names:
                fields.append(StructField(n, name_to_datatype_b[n], True))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(
            merge_type(
                a.element_type,
                b.element_type,
                name=f"element in array {name}" if name else "element in array",
            )
        )

    elif isinstance(a, MapType):
        return MapType(
            merge_type(
                a.key_type,
                b.key_type,
                name=f"key of map {name}" if name else "key of map",
            ),
            merge_type(
                a.value_type,
                b.value_type,
                name=f"value of map {name}" if name else "value of map",
            ),
        )
    else:
        return a


def python_type_str_to_object(tp_str: str) -> Type:
    # handle several special cases, which we want to support currently
    if tp_str == "Decimal":
        return decimal.Decimal
    elif tp_str == "date":
        return datetime.date
    elif tp_str == "time":
        return datetime.time
    elif tp_str == "datetime":
        return datetime.datetime
    elif installed_pandas:
        import pandas

        if tp_str in ["Series", "pd.Series"]:
            return pandas.Series
        elif tp_str in ["DataFrame", "pd.DataFrame"]:
            return pandas.DataFrame
    return eval(tp_str)


# TODO(guantao.gao) remove this function as it is dead code
def python_type_to_data_type(tp: Union[str, Type]) -> Tuple[DataType, bool]:
    """Converts a Python type or a Python type string to a Zettapark type.
    Returns a Zettapark type and whether it's nullable.
    """
    from clickzetta.zettapark.dataframe import DataFrame

    # convert a type string to a type object
    if isinstance(tp, str):
        tp = python_type_str_to_object(tp)

    if tp is decimal.Decimal:
        return DecimalType(38, 18), False
    elif tp in PYTHON_TO_DATA_TYPE_MAPPINGS:
        return PYTHON_TO_DATA_TYPE_MAPPINGS[tp](), False

    tp_origin = get_origin(tp)
    tp_args = get_args(tp)

    # only typing.Optional[X], i.e., typing.Union[X, None] is accepted
    if (
        tp_origin
        and tp_origin == Union
        and tp_args
        and len(tp_args) == 2
        and tp_args[1] == NoneType
    ):
        return python_type_to_data_type(tp_args[0])[0], True

    # typing.List, typing.Tuple, list, tuple
    list_tps = [list, tuple, List, Tuple]
    if tp in list_tps or (tp_origin and tp_origin in list_tps):
        element_type = (
            python_type_to_data_type(tp_args[0])[0] if tp_args else StringType()
        )
        return ArrayType(element_type), False

    # typing.Dict, dict
    dict_tps = [dict, Dict]
    if tp in dict_tps or (tp_origin and tp_origin in dict_tps):
        key_type = python_type_to_data_type(tp_args[0])[0] if tp_args else StringType()
        value_type = (
            python_type_to_data_type(tp_args[1])[0] if tp_args else StringType()
        )
        return MapType(key_type, value_type), False

    if installed_pandas:
        import pandas

        pandas_series_tps = [PandasSeries, pandas.Series]
        if tp in pandas_series_tps or (tp_origin and tp_origin in pandas_series_tps):
            return (
                PandasSeriesType(
                    python_type_to_data_type(tp_args[0])[0] if tp_args else None
                ),
                False,
            )

        pandas_dataframe_tps = [PandasDataFrame, pandas.DataFrame]
        if tp in pandas_dataframe_tps or (
            tp_origin and tp_origin in pandas_dataframe_tps
        ):
            return (
                PandasDataFrameType(
                    [python_type_to_data_type(tp_arg)[0] for tp_arg in tp_args]
                    if tp_args
                    else ()
                ),
                False,
            )

    if tp == DataFrame:
        return StructType(), False

    if tp == Timestamp or tp_origin == Timestamp:
        if not tp_args:
            timezone = TimestampTimeZone.DEFAULT
        elif tp_args[0] == NTZ:
            timezone = TimestampTimeZone.NTZ
        elif tp_args[0] == LTZ:
            timezone = TimestampTimeZone.LTZ
        elif tp_args[0] == TZ:
            timezone = TimestampTimeZone.TZ
        else:
            raise TypeError(
                f"Only Timestamp, Timestamp[NTZ], Timestamp[LTZ] and Timestamp[TZ] are allowed, but got {tp}"
            )
        return TimestampType(timezone), False

    raise TypeError(f"invalid type {tp}")


def retrieve_func_type_hints_from_source(
    file_path: str,
    func_name: str,
    class_name: Optional[str] = None,
    _source: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """
    Retrieve type hints of a function from a source file, or a source string (test only).
    Returns None if the function is not found.
    """

    def parse_arg_annotation(annotation: ast.expr) -> str:
        if isinstance(annotation, (ast.Tuple, ast.List)):
            return ", ".join([parse_arg_annotation(e) for e in annotation.elts])
        if isinstance(annotation, ast.Attribute):
            return f"{parse_arg_annotation(annotation.value)}.{annotation.attr}"
        if isinstance(annotation, ast.Subscript):
            return f"{parse_arg_annotation(annotation.value)}[{parse_arg_annotation(annotation.slice)}]"
        if isinstance(annotation, ast.Index):
            return parse_arg_annotation(annotation.value)
        if isinstance(annotation, ast.Constant) and annotation.value is None:
            return "NoneType"
        if isinstance(annotation, ast.Name):
            return annotation.id
        raise TypeError(f"invalid type annotation: {annotation}")

    class FuncNodeVisitor(ast.NodeVisitor):
        type_hints = {}
        func_exist = False

        def visit_FunctionDef(self, node):
            if node.name == func_name:
                for arg in node.args.args:
                    if arg.annotation:
                        self.type_hints[arg.arg] = parse_arg_annotation(arg.annotation)
                if node.returns:
                    self.type_hints["return"] = parse_arg_annotation(node.returns)
                self.func_exist = True

    if not _source:
        with open(file_path) as f:
            _source = f.read()

    if class_name:

        class ClassNodeVisitor(ast.NodeVisitor):
            class_node = None

            def visit_ClassDef(self, node):
                if node.name == class_name:
                    self.class_node = node

        class_visitor = ClassNodeVisitor()
        class_visitor.visit(ast.parse(_source))
        if class_visitor.class_node is None:
            return None
        to_visit_node_for_func = class_visitor.class_node
    else:
        to_visit_node_for_func = ast.parse(_source)

    visitor = FuncNodeVisitor()
    visitor.visit(to_visit_node_for_func)
    if not visitor.func_exist:
        return None
    return visitor.type_hints


def type_string_to_type_object(type_str: str) -> DataType:
    try:
        return parse_data_type(type_str)
    except ParsingError as ex:
        raise ValueError(f"'{type_str}' is not a supported type") from ex


# Type hints
ColumnOrName = Union["clickzetta.zettapark.column.Column", str]
ColumnOrLiteralStr = Union["clickzetta.zettapark.column.Column", str]
ColumnOrSqlExpr = Union["clickzetta.zettapark.column.Column", str]
LiteralType = Union[VALID_PYTHON_TYPES_FOR_LITERAL_VALUE]
ColumnOrLiteral = Union["clickzetta.zettapark.column.Column", LiteralType]


ResultValueConverter = Callable[[Any], Any]


class ResultValueConverterFactory(DataTypeVisitor[Optional[ResultValueConverter]]):
    def visit_any(self, t: DataType) -> Optional[ResultValueConverter]:
        return None

    def visit_binary(self, t: BinaryType) -> Optional[ResultValueConverter]:
        def _to_bytearray(value: Any) -> bytearray:
            if value is None:
                return None
            if isinstance(value, bytes):
                return bytearray(value)
            if isinstance(value, bytearray):
                return value
            raise TypeError(f"Illegal binary data type: {type(value)}")

        return _to_bytearray

    # The connector returns
    #   datetime.datetime instances with some timezone (e.g. CST) for timestamp_ltz type, or
    #   datetime.datetime instances with UTC timezone for timestamp_ntz type,
    # that is not compatible with PySpark, which returns datetime.datetime instances
    # with no timezone for both timestamp_ltz and timestamp_ntz types.
    def visit_timestamp(self, t: TimestampType) -> Callable[[Any], Any] | None:
        def _to_timestamp(value: Any) -> datetime:
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                return value.replace(tzinfo=None)
            raise TypeError(f"Illegal timestamp data type: {type(value)}")

        return _to_timestamp

    _VECTOR_VALUE_PATTERN = re.compile(r"^\[[0-9.,]*\]$")

    def visit_vector(self, t: VectorType) -> Optional[ResultValueConverter]:
        def _to_vector(value: Any) -> List[Any]:
            if value is None:
                return None
            if isinstance(value, str):
                if self._VECTOR_VALUE_PATTERN.match(value):
                    return eval(value)
                raise ValueError(f"Illegal vector data value: {value}")
            raise TypeError(f"Illegal vector data type: {type(value)}")

        return _to_vector

    def visit_array(self, t: ArrayType) -> Optional[ResultValueConverter]:
        element_converter = t.element_type.accept(self)
        if element_converter is None:
            return None

        def _to_array(value: Any) -> List[Any]:
            if value is None:
                return None
            return [element_converter(v) for v in value]

        return _to_array

    def visit_map(self, t: MapType) -> Optional[ResultValueConverter]:
        key_converter = t.key_type.accept(self)
        value_converter = t.value_type.accept(self)
        return MapValueConverter(key_converter, value_converter)

    def visit_struct(self, t: StructType) -> Optional[ResultValueConverter]:
        from clickzetta.zettapark._internal.utils import unquote_name

        field_names = [unquote_name(f.name) for f in t.fields]
        field_converters = {
            unquote_name(f.name): f.datatype.accept(self) for f in t.fields
        }
        return StructValueConverter(field_names, field_converters)


_RESULT_VALUE_CONVERTER_FACTORY = ResultValueConverterFactory()


class MapValueConverter:
    def __init__(
        self,
        key_converter: Optional[ResultValueConverter],
        value_converter: Optional[ResultValueConverter],
    ) -> None:
        self.key_converter = key_converter
        self.value_converter = value_converter
        if key_converter is None and value_converter is None:
            self._mapper = self._mapper_n
        elif key_converter is None and value_converter is not None:
            self._mapper = self._mapper_v
        elif key_converter is not None and value_converter is None:
            self._mapper = self._mapper_k
        else:
            self._mapper = self._mapper_kv

    def __call__(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise TypeError(f"Illegal map data type: {type(value)}")
        return self._mapper(value)

    def _mapper_kv(self, value: Any) -> Any:
        return {self.key_converter(k): self.value_converter(v) for k, v in value}

    def _mapper_k(self, value: Any) -> Any:
        return {self.key_converter(k): v for k, v in value}

    def _mapper_v(self, value: Any) -> Any:
        return {k: self.value_converter(v) for k, v in value}

    def _mapper_n(self, value: Any) -> Any:
        return {k: v for k, v in value}


class StructValueConverter:
    def __init__(
        self,
        field_names: List[str],
        field_converters: Dict[str, Optional[ResultValueConverter]],
    ) -> None:
        self.field_names = field_names
        self.field_converters = field_converters
        self.row_struct = Row._builder.build(*field_names).to_row()
        if all(c is None for c in field_converters.values()):
            self._to_row = self._to_row_0
        else:
            self.field_converters = {
                n: (c if c is not None else lambda x: x)
                for n, c in self.field_converters.items()
            }
            self._to_row = self._to_row_1

    def __call__(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise TypeError(f"Illegal struct data type: {type(value)}")
        return self._to_row(value)

    def _to_row_0(self, value: Any) -> Row:
        values = [value.get(n) for n in self.field_names]
        return self.row_struct(*values)

    def _to_row_1(self, value: Any) -> Row:
        values = [self.field_converters[n](value.get(n)) for n in self.field_names]
        return self.row_struct(*values)


class ResultRowConverter:
    def __init__(self, result_meta: Optional[List[ResultMetadata]]) -> None:
        if result_meta is None:
            self._converters = None
        else:
            self._converters = [
                sql_type_to_data_type(m.sql_type).accept(
                    _RESULT_VALUE_CONVERTER_FACTORY
                )
                for m in result_meta
            ]
            if all(c is None for c in self._converters):
                self._converters = None
            else:
                self._converters = [
                    (c if c is not None else lambda x: x) for c in self._converters
                ]
        if self._converters is None:
            self._to_row = self._to_row_0
        else:
            self._to_row = self._to_row_1

    def __call__(self, data: Sequence[Any]) -> Sequence[Any]:
        return self._to_row(data)

    def _to_row_0(self, data: Sequence[Any]) -> Sequence[Any]:
        return data

    def _to_row_1(self, data: Sequence[Any]) -> Sequence[Any]:
        return [self._converters[i](d) for i, d in enumerate(data)]
