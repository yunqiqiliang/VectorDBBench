from __future__ import annotations

import collections
import decimal
import re
import time
from base64 import b16encode
from datetime import date, datetime
from datetime import time as dt_t
from datetime import timedelta, timezone
from typing import Any, NoReturn

import pytz

try:
    import numpy
except ImportError:
    numpy = None
try:
    import tzlocal
except ImportError:
    tzlocal = None

BITS_FOR_TIMEZONE = 14
ZERO_TIMEDELTA = timedelta(seconds=0)
ZERO_EPOCH_DATE = date(1970, 1, 1)
ZERO_EPOCH = datetime.fromtimestamp(0, timezone.utc).replace(tzinfo=None)
ZERO_FILL = "000000000"


class Converter:
    def __init__(self, **kwargs) -> None:
        self._parameters: dict[str, str | int | bool] = {}

    def convert_to(self, value: Any) -> Any:
        """Converts Python data to Clickzetta data for pyformat/format style.

        The output is bound in a query in the client side.
        """
        type_name = value.__class__.__name__.lower()
        if hasattr(self, f"_{type_name}"):
            return getattr(self, f"_{type_name}")(value)
        else:
            # If the type is not found, return the value as is.
            return value

    def _int(self, value: int) -> int:
        return int(value)

    def _long(self, value):
        return int(value)

    def _float(self, value: float) -> float:
        return float(value)

    def _str(self, value: str) -> str:
        return str(value)

    _unicode = _str

    def _binary(self, binary_value: bytes) -> bytes | bytearray:
        """Encodes a "bytes" object for passing to Clickzetta."""
        result = b16encode(binary_value)

        if isinstance(binary_value, bytearray):
            return bytearray(result)
        return result

    def _bytes(self, value: bytes) -> bytes:
        return self._binary(value)

    _bytearray = _bytes

    def _bool(self, value: bool) -> bool:
        return value

    def _bool_(self, value) -> bool:
        return bool(value)

    def _nonetype(self, _: Any | None) -> Any | None:
        return None

    def _datetime(self, value: datetime) -> str:
        tzinfo_value = value.tzinfo
        if tzinfo_value:
            if pytz.utc != tzinfo_value:
                try:
                    td = tzinfo_value.utcoffset(value)
                except pytz.exceptions.AmbiguousTimeError:
                    td = tzinfo_value.utcoffset(value)
            else:
                td = ZERO_TIMEDELTA
            sign = "+" if td >= ZERO_TIMEDELTA else "-"
            td_secs = self.sfdatetime_total_seconds_from_timedelta(td)
            h, m = divmod(abs(td_secs // 60), 60)
            if value.microsecond:
                return r"TIMESTAMP '" + (
                    "{year:d}-{month:02d}-{day:02d} "
                    "{hour:02d}:{minute:02d}:{second:02d}."
                    "{microsecond:06d}{sign}{tzh:02d}:{tzm:02d}"
                ).format(
                    year=value.year,
                    month=value.month,
                    day=value.day,
                    hour=value.hour,
                    minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond,
                    sign=sign,
                    tzh=h,
                    tzm=m,
                ) + r"'"
            return r"TIMESTAMP '" + (
                "{year:d}-{month:02d}-{day:02d} "
                "{hour:02d}:{minute:02d}:{second:02d}"
                "{sign}{tzh:02d}:{tzm:02d}"
            ).format(
                year=value.year,
                month=value.month,
                day=value.day,
                hour=value.hour,
                minute=value.minute,
                second=value.second,
                sign=sign,
                tzh=h,
                tzm=m,
            ) + r"'"
        else:
            if value.microsecond:
                return r"TIMESTAMP '" + (
                    "{year:d}-{month:02d}-{day:02d} "
                    "{hour:02d}:{minute:02d}:{second:02d}."
                    "{microsecond:06d}"
                ).format(
                    year=value.year,
                    month=value.month,
                    day=value.day,
                    hour=value.hour,
                    minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond,
                ) + r"'"
            return r"TIMESTAMP '" + (
                "{year:d}-{month:02d}-{day:02d} " "{hour:02d}:{minute:02d}:{second:02d}"
            ).format(
                year=value.year,
                month=value.month,
                day=value.day,
                hour=value.hour,
                minute=value.minute,
                second=value.second,
            ) + r"'"

    def _date(self, value: date) -> str:
        """Converts Date object to Clickzetta object."""
        return r"DATE '{year:d}-{month:02d}-{day:02d}'".format(
            year=value.year, month=value.month, day=value.day
        )

    def _time(self, value: dt_t) -> str:
        if value.microsecond:
            return value.strftime("%H:%M:%S.%%06d") % value.microsecond
        return value.strftime("%H:%M:%S")

    def _struct_time(self, value: time.struct_time) -> str:
        tzinfo_value = self._generate_tzinfo_from_tzoffset(time.timezone // 60)
        t = datetime.fromtimestamp(time.mktime(value))
        if pytz.utc != tzinfo_value:
            t += tzinfo_value.utcoffset(t)
        t = t.replace(tzinfo=tzinfo_value)
        return self._datetime(t)

    def sfdatetime_total_seconds_from_timedelta(self, td: timedelta) -> int:
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) // 10 ** 6

    def _timedelta(self, value: timedelta) -> str:
        (hours, r) = divmod(value.seconds, 3600)
        (mins, secs) = divmod(r, 60)
        hours += value.days * 24
        if value.microseconds:
            return ("{hour:02d}:{minute:02d}:{second:02d}." "{microsecond:06d}").format(
                hour=hours, minute=mins, second=secs, microsecond=value.microseconds
            )
        return "{hour:02d}:{minute:02d}:{second:02d}".format(
            hour=hours, minute=mins, second=secs
        )

    def _decimal(self, value: decimal.Decimal) -> float | None:
        if isinstance(value, decimal.Decimal):
            return float(value)

        return None

    def __numpy(self, value):
        return value

    _int8 = __numpy
    _int16 = __numpy
    _int32 = __numpy
    _int64 = __numpy
    _uint8 = __numpy
    _uint16 = __numpy
    _uint32 = __numpy
    _uint64 = __numpy
    _float16 = __numpy
    _float32 = __numpy
    _float64 = __numpy

    def _datetime64(self, value) -> str:
        return str(value) + "+00:00"

    def _quoted_name(self, value) -> str:
        return str(value)

    def __getattr__(self, item: str) -> NoReturn:
        raise AttributeError(f"No method is available: {item}")

    @staticmethod
    def escape(value: Any) -> Any:
        if not value or not isinstance(value, str):
            return value
        res = value
        res = res.replace("\\", "\\\\")
        res = res.replace("\n", "\\n")
        res = res.replace("\r", "\\r")
        res = res.replace("\047", "\134\047")  # single quotes
        return res

    @staticmethod
    def quote(value) -> str:
        if isinstance(value, tuple):
            if not value:
                # STRUCT do not supported Empty
                raise ValueError("STRUCT do not support Empty")
            return 'STRUCT(' + ",".join(Converter.quote(item) for item in value) + ')'
        elif isinstance(value, (set, map, dict)):
            if not value:
                # MAP is supported Empty
                return 'MAP()'
            return 'MAP(' + ",".join(
                Converter.quote(k) + "," + Converter.quote(v) for k, v in value.items()) + ')'
        elif isinstance(value, (list, numpy.ndarray)):
            if not value:
                # ARRAY is supported Empty
                return "ARRAY()"
            return 'ARRAY(' + ",".join(Converter.quote(item) for item in value) + ')'
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, int) or isinstance(value, float) or isinstance(value, decimal.Decimal) or isinstance(
                value, numpy.int64):
            return str(repr(value))
        elif isinstance(value, (bytes, bytearray)):
            # Binary literal syntax
            return "X'{}'".format(value.decode("ascii"))
        elif isinstance(value, str):
            if value.startswith("INTERVAL "):
                return value
            if value.startswith("TIMESTAMP '") or value.startswith("DATE '"):
                return value
            if value[:4].upper() == "JSON" and re.match(r"^JSON\s*'", value.strip()):
                return value.strip()
        elif isinstance(value, datetime):
            return f"TIMESTAMP '{value}'"
        elif isinstance(value, date):
            return f"DATE'{value}'"
        return f"'{Converter.escape(value)}'"
