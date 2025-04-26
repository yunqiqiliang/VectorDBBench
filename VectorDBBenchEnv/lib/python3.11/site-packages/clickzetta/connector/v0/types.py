import datetime
from decimal import Decimal
from time import struct_time
from typing import Dict, Optional, Sequence, Union

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def Binary(data):
    # Construct a DB-API binary value
    if isinstance(data, int):
        raise TypeError("cannot convert `int` object to binary")

    try:
        return bytes(data)
    except TypeError:
        if isinstance(data, str):
            return data.encode("utf-8")
        else:
            raise


def TimeFromTicks(ticks, tz=None):
    # Construct a DB-API time value from the given ticks value.

    dt = datetime.datetime.fromtimestamp(ticks, tz=tz)
    return dt.timetz()


class _DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


STRING = _DBAPITypeObject("STRING", "CHAR", "VARCHAR")
BINARY = _DBAPITypeObject("BINARY", "ARRAY", "STRUCT", "MAP", "VECTOR", "JSON")
NUMBER = _DBAPITypeObject(
    "INT8", "INT32", "INT64", "FLOAT32", "FLOAT64", "DECIMAL", "BOOL"
)
DATETIME = _DBAPITypeObject("TIMESTAMP", "DATE")
ROWID = "ROWID"
