import base64
import io
from typing import List, Optional, Tuple

import pandas
import pyarrow as pa
import requests

from clickzetta.connector.v0._dbapi import Field
from clickzetta.connector.v0.sql_types import (
    ArrayType,
    BigintType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntType,
    MapType,
    SmallintType,
    SqlType,
    SqlTypeVisitor,
    StructType,
    TextType,
    TimestampType,
    TinyintType,
    VoidType,
)


class ToArrowDataTypeVisitor(SqlTypeVisitor):
    def visit_any(self, t: SqlType):
        raise TypeError(f"Type '{t}' not supported")

    def visit_void(self, t: VoidType):
        return pa.null()

    def visit_tinyint(self, t: TinyintType):
        return pa.int8()

    def visit_smallint(self, t: SmallintType):
        return pa.int16()

    def visit_int(self, t: IntType):
        return pa.int32()

    def visit_bigint(self, t: BigintType):
        return pa.int64()

    def visit_float(self, t: FloatType):
        return pa.float32()

    def visit_double(self, t: DoubleType):
        return pa.float64()

    def visit_decimal(self, t: DecimalType):
        return pa.decimal128(t.precision, t.scale)

    def visit_boolean(self, t: BooleanType):
        return pa.bool_()

    def visit_text(self, t: TextType):
        return pa.string()

    def visit_binary(self, t: BinaryType):
        return pa.binary()

    def visit_date(self, t: DateType):
        return pa.date32()

    def visit_timestamp(self, t: TimestampType):
        return pa.timestamp("us")

    def visit_array(self, t: ArrayType):
        elem_type = t.element_type.accept(self)
        return pa.list_(elem_type)

    def visit_map(self, t: MapType):
        key_type = t.key_type.accept(self)
        value_type = t.value_type.accept(self)
        return pa.map_(key_type, value_type)

    def visit_struct(self, t: StructType):
        fields = [pa.field(f.name, f.type_.accept(self)) for f in t.fields]
        return pa.struct(fields)


_TO_ARROW_DATA_TYPE_VISITOR = ToArrowDataTypeVisitor()


def _data_to_pandas(data: List[Tuple], schema: List[Field]) -> pandas.DataFrame:
    arrays = []
    for i, field in enumerate(schema):
        data_type = field.sql_type.accept(_TO_ARROW_DATA_TYPE_VISITOR)
        values = [d[i] for d in data]
        arrays.append(pa.array(values, type=data_type))
    names = [field.name for field in schema]
    batch = pa.RecordBatch.from_arrays(arrays, names)
    return batch.to_pandas()


def _embedded_arrow_messages_to_pandas(messages: List[str]) -> pandas.DataFrame:
    batches = []
    for message in messages:
        buffer = base64.b64decode(message)
        with pa.ipc.RecordBatchStreamReader(io.BytesIO(buffer)) as reader:
            for batch in reader:
                batches.append(batch)
    table = pa.Table.from_batches(batches)
    return table.to_pandas()


def _http_arrow_messages_to_pandas(urls: List[str]) -> pandas.DataFrame:
    batches = []
    for url in urls:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        stream = io.BytesIO(response.content)
        with pa.ipc.RecordBatchStreamReader(stream) as reader:
            for batch in reader:
                batches.append(batch)
    table = pa.Table.from_batches(batches)
    return table.to_pandas()


def to_pandas(
    schema: List[Field],
    data: Optional[List[Tuple]],
    embedded_arrow_messages: Optional[List[str]],
    http_arrow_messages: Optional[List[str]],
    timezone: Optional[str],
) -> pandas.DataFrame:
    if data is not None:
        return _data_to_pandas(data, schema)
    if embedded_arrow_messages is not None:
        return _embedded_arrow_messages_to_pandas(embedded_arrow_messages)
    if http_arrow_messages is not None:
        return _http_arrow_messages_to_pandas(http_arrow_messages)
    raise ValueError("No data to convert to pandas")
