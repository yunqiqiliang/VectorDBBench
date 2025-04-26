"""Shared helper functions for ClickZetta API classes."""

import base64
import datetime
import decimal
import math
import os
import re
from typing import Optional, Union

import packaging.version

UTC = datetime.timezone.utc
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_RFC3339_MICROS = "%Y-%m-%dT%H:%M:%S.%fZ"
_RFC3339_NO_FRACTION = "%Y-%m-%dT%H:%M:%S"

_RFC3339_MICROS_NO_ZULU = "%Y-%m-%dT%H:%M:%S.%f"
_TIMEONLY_WO_MICROS = "%H:%M:%S"
_TIMEONLY_W_MICROS = "%H:%M:%S.%f"
_PROJECT_PREFIX_PATTERN = re.compile(
    r"""
    (?P<project_id>\S+\:[^.]+)\.(?P<dataset_id>[^.]+)(?:$|\.(?P<custom_id>[^.]+)$)
""",
    re.VERBOSE,
)

CZ_EMULATOR_HOST = "CZ_EMULATOR_HOST"

_DEFAULT_HOST = "localhost:8080"


def _get_click_zetta_host():
    return os.environ.get(CZ_EMULATOR_HOST, _DEFAULT_HOST)


def _not_null(value, field):
    return value is not None or (field is not None and field.mode != "NULLABLE")


def _int_from_json(value, field):
    if _not_null(value, field):
        return int(value)


def _date_from_iso8601_date(value):
    return datetime.datetime.strptime(value, "%Y-%m-%d").date()


def _datetime_from_microseconds(value):
    return _EPOCH + datetime.timedelta(microseconds=value)


def _float_from_json(value, field):
    if _not_null(value, field):
        return float(value)


def _decimal_from_json(value, field):
    if _not_null(value, field):
        return decimal.Decimal(value)


def _bool_from_json(value, field):
    if _not_null(value, field):
        return value.lower() in ["t", "true", "1"]


def _string_from_json(value, _):
    return value


def _to_bytes(value, encoding="ascii"):
    result = value.encode(encoding) if isinstance(value, str) else value
    if isinstance(result, bytes):
        return result
    else:
        raise TypeError("%r could not be converted to bytes" % (value,))


def _bytes_from_json(value, field):
    if _not_null(value, field):
        return base64.standard_b64decode(_to_bytes(value))


def _timestamp_from_json(value, field):
    if _not_null(value, field):
        return _datetime_from_microseconds(int(value))


def _date_from_json(value, field):
    if _not_null(value, field):
        return _date_from_iso8601_date(value)


_CELLDATA_FROM_JSON = {
    "INT8": _int_from_json,
    "INT16": _int_from_json,
    "INT32": _int_from_json,
    "INT64": _int_from_json,
    "FLOAT32": _float_from_json,
    "FLOAT64": _float_from_json,
    "DECIMAL": _decimal_from_json,
    "BOOL": _bool_from_json,
    "STRING": _string_from_json,
    "VARCHAR": _string_from_json,
    "CHAR": _string_from_json,
    "BINARY": _bytes_from_json,
    "TIMESTAMP": _timestamp_from_json,
    "DATE": _date_from_json,
}

_QUERY_PARAMS_FROM_JSON = dict(_CELLDATA_FROM_JSON)


def _field_to_index_mapping(schema):
    return {f.name: i for i, f in enumerate(schema)}


def _field_from_json(resource, field):
    converter = _CELLDATA_FROM_JSON.get(field.field_type, lambda value, _: value)
    if field.mode == "REPEATED":
        return [converter(item["v"], field) for item in resource]
    else:
        return converter(resource, field)


def _row_tuple_from_json(row, schema):
    from .schema import _to_schema_fields

    schema = _to_schema_fields(schema)

    row_data = []
    for field, cell in zip(schema, row["f"]):
        row_data.append(_field_from_json(cell["v"], field))
    return tuple(row_data)


def _int_to_json(value):
    if isinstance(value, int):
        value = str(value)
    return value


def _float_to_json(value) -> Union[None, str, float]:
    if value is None:
        return None

    if isinstance(value, str):
        value = float(value)

    return str(value) if (math.isnan(value) or math.isinf(value)) else float(value)


def _decimal_to_json(value):
    if isinstance(value, decimal.Decimal):
        value = str(value)
    return value


def _bool_to_json(value):
    if isinstance(value, bool):
        value = "true" if value else "false"
    return value


def _bytes_to_json(value):
    if isinstance(value, bytes):
        value = base64.standard_b64encode(value).decode("ascii")
    return value


def _timestamp_to_json_parameter(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo not in (None, UTC):
            # Convert to UTC and remove the time zone info.
            value = value.replace(tzinfo=None) - value.utcoffset()
        value = "%s %s+00:00" % (value.date().isoformat(), value.time().isoformat())
    return value


def _timestamp_to_json_row(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        value = value.strftime(_RFC3339_MICROS)
    return value


def _datetime_to_json(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        value = value.strftime(_RFC3339_MICROS_NO_ZULU)
    return value


def _date_to_json(value):
    if isinstance(value, datetime.date):
        value = value.isoformat()
    return value


def _time_to_json(value):
    if isinstance(value, datetime.time):
        value = value.isoformat()
    return value


_SCALAR_VALUE_TO_JSON_ROW = {
    "INT8": _int_to_json,
    "INT16": _int_to_json,
    "INT32": _int_to_json,
    "INT64": _int_to_json,
    "FLOAT32": _float_to_json,
    "FLOAT64": _float_to_json,
    "DECIMAL": _decimal_to_json,
    "BOOL": _bool_to_json,
    "BINARY": _bytes_to_json,
    "TIMESTAMP": _timestamp_to_json_row,
    "DATE": _date_to_json,
}

_SCALAR_VALUE_TO_JSON_PARAM = _SCALAR_VALUE_TO_JSON_ROW.copy()
_SCALAR_VALUE_TO_JSON_PARAM["TIMESTAMP"] = _timestamp_to_json_parameter


def _scalar_field_to_json(field, row_value):
    converter = _SCALAR_VALUE_TO_JSON_ROW.get(field.field_type)
    if converter is None:
        return row_value
    return converter(row_value)


def _repeated_field_to_json(field, row_value):
    values = []
    for item in row_value:
        values.append(_single_field_to_json(field, item))
    return values


def _record_field_to_json(fields, row_value):
    isdict = isinstance(row_value, dict)

    if not isdict and len(row_value) != len(fields):
        msg = "The number of row fields ({}) does not match schema length ({}).".format(
            len(row_value), len(fields)
        )
        raise ValueError(msg)

    record = {}

    if isdict:
        processed_fields = set()

    for subindex, subfield in enumerate(fields):
        subname = subfield.name
        subvalue = row_value.get(subname) if isdict else row_value[subindex]

        if subvalue is not None:
            record[subname] = _field_to_json(subfield, subvalue)

        if isdict:
            processed_fields.add(subname)

    if isdict:
        not_processed = set(row_value.keys()) - processed_fields

        for field_name in not_processed:
            value = row_value[field_name]
            if value is not None:
                record[field_name] = str(value)

    return record


def _single_field_to_json(field, row_value):
    if row_value is None:
        return None

    if field.field_type == "RECORD":
        return _record_field_to_json(field.fields, row_value)

    return _scalar_field_to_json(field, row_value)


def _field_to_json(field, row_value):
    if row_value is None:
        return None

    if field.mode == "REPEATED":
        return _repeated_field_to_json(field, row_value)

    return _single_field_to_json(field, row_value)


def _snake_to_camel_case(value):
    words = value.split("_")
    return words[0] + "".join(map(str.capitalize, words[1:]))


def _get_sub_prop(container, keys, default=None):
    if isinstance(keys, str):
        keys = [keys]

    sub_val = container
    for key in keys:
        if key not in sub_val:
            return default
        sub_val = sub_val[key]
    return sub_val


def _set_sub_prop(container, keys, value):
    if isinstance(keys, str):
        keys = [keys]

    sub_val = container
    for key in keys[:-1]:
        if key not in sub_val:
            sub_val[key] = {}
        sub_val = sub_val[key]
    sub_val[keys[-1]] = value


def _del_sub_prop(container, keys):
    sub_val = container
    for key in keys[:-1]:
        if key not in sub_val:
            sub_val[key] = {}
        sub_val = sub_val[key]
    if keys[-1] in sub_val:
        del sub_val[keys[-1]]


def _int_or_none(value):
    if isinstance(value, int):
        return value
    if value is not None:
        return int(value)


def _str_or_none(value):
    if value is not None:
        return str(value)


def _split_id(full_id):
    with_prefix = _PROJECT_PREFIX_PATTERN.match(full_id)
    if with_prefix is None:
        parts = full_id.split(".")
    else:
        parts = with_prefix.groups()
        parts = [part for part in parts if part]
    return parts


def _build_resource_from_properties(obj, filter_fields):
    partial = {}
    for filter_field in filter_fields:
        api_field = obj._PROPERTY_TO_API_FIELD.get(filter_field)
        if api_field is None and filter_field not in obj._properties:
            raise ValueError("No property %s" % filter_field)
        elif api_field is not None:
            partial[api_field] = obj._properties.get(api_field)
        else:
            partial[filter_field] = obj._properties[filter_field]

    return partial


def _verify_job_config_type(job_config, expected_type, param_name="job_config"):
    if not isinstance(job_config, expected_type):
        msg = (
            "Expected an instance of {expected_type} class for the {param_name} parameter, "
            "but received {param_name} = {job_config}"
        )
        raise TypeError(
            msg.format(
                expected_type=expected_type.__name__,
                param_name=param_name,
                job_config=job_config,
            )
        )
