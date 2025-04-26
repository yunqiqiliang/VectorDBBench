"""Schemas for ClickZetta tables / queries."""

import collections
import enum
from typing import Any, Dict, Iterable, Union

import clickzetta.connector.v0.standard_sql as standard_sql
from clickzetta.connector.v0.enums import StandardSqlTypeNames

_STRUCT_TYPES = ("MAP", "STRUCT", "ARRAY")

LEGACY_TO_STANDARD_TYPES = {
    "STRING": StandardSqlTypeNames.STRING,
    "BINARY": StandardSqlTypeNames.BINARY,
    "INT8": StandardSqlTypeNames.INT8,
    "INT16": StandardSqlTypeNames.INT16,
    "FLOAT32": StandardSqlTypeNames.FLOAT32,
    "FLOAT64": StandardSqlTypeNames.FLOAT64,
    "INT32": StandardSqlTypeNames.INT32,
    "INT64": StandardSqlTypeNames.INT64,
    "BOOL": StandardSqlTypeNames.BOOL,
    "MAP": StandardSqlTypeNames.MAP,
    "ARRAY": StandardSqlTypeNames.ARRAY,
    "STRUCT": StandardSqlTypeNames.STRUCT,
    "TIMESTAMP": StandardSqlTypeNames.TIMESTAMP,
    "DATE": StandardSqlTypeNames.DATE,
    "DECIMAL": StandardSqlTypeNames.DECIMAL,
    "VARCHAR": StandardSqlTypeNames.VARCHAR,
    "CHAR": StandardSqlTypeNames.CHAR,
}


class _DefaultSentinel(enum.Enum):
    DEFAULT_VALUE = object()


_DEFAULT_VALUE = _DefaultSentinel.DEFAULT_VALUE


class SchemaField(object):
    def __init__(
        self,
        name: str,
        field_type: str,
        mode: str = "NULLABLE",
        description: Union[str, _DefaultSentinel] = _DEFAULT_VALUE,
        fields: Iterable["SchemaField"] = (),
        precision: Union[int, _DefaultSentinel] = _DEFAULT_VALUE,
        scale: Union[int, _DefaultSentinel] = _DEFAULT_VALUE,
        max_length: Union[int, _DefaultSentinel] = _DEFAULT_VALUE,
    ):
        self._properties: Dict[str, Any] = {
            "name": name,
            "type": field_type,
        }
        if mode is not None:
            self._properties["mode"] = mode.upper()
        if description is not _DEFAULT_VALUE:
            self._properties["description"] = description
        if precision is not _DEFAULT_VALUE:
            self._properties["precision"] = precision
        if scale is not _DEFAULT_VALUE:
            self._properties["scale"] = scale
        if max_length is not _DEFAULT_VALUE:
            self._properties["maxLength"] = max_length
        self._fields = tuple(fields)

    @staticmethod
    def __get_int(api_repr, name):
        v = api_repr.get(name, _DEFAULT_VALUE)
        if v is not _DEFAULT_VALUE:
            v = int(v)
        return v

    @classmethod
    def from_api_repr(cls, api_repr: dict) -> "SchemaField":
        field_type = api_repr["type"].upper()
        mode = api_repr.get("mode", "NULLABLE")
        description = api_repr.get("description", _DEFAULT_VALUE)
        fields = api_repr.get("fields", ())

        return cls(
            field_type=field_type,
            fields=[cls.from_api_repr(f) for f in fields],
            mode=mode.upper(),
            description=description,
            name=api_repr["name"],
            precision=cls.__get_int(api_repr, "precision"),
            scale=cls.__get_int(api_repr, "scale"),
            max_length=cls.__get_int(api_repr, "maxLength"),
        )

    @property
    def name(self):
        return self._properties["name"]

    @property
    def field_type(self):
        return self._properties["type"]

    @property
    def mode(self):
        return self._properties.get("mode")

    @property
    def is_nullable(self):
        return self.mode == "NULLABLE"

    @property
    def description(self):
        return self._properties.get("description")

    @property
    def precision(self):
        return self._properties.get("precision")

    @property
    def scale(self):
        return self._properties.get("scale")

    @property
    def max_length(self):
        return self._properties.get("maxLength")

    @property
    def fields(self):
        return self._fields

    def to_api_repr(self) -> dict:
        answer = self._properties.copy()
        if self.field_type.upper() in _STRUCT_TYPES:
            answer["fields"] = [f.to_api_repr() for f in self.fields]

        return answer

    def _key(self):
        field_type = self.field_type.upper() if self.field_type is not None else None
        if field_type is not None:
            if (
                field_type == "STRING"
                or field_type == "VARCHAR"
                or field_type == "CHAR"
            ):
                if self.max_length is not None:
                    field_type = f"{field_type}({self.max_length})"
            elif field_type.startswith("DECIMAL"):
                if self.precision is not None:
                    if self.scale is not None:
                        field_type = f"{field_type}({self.precision}, {self.scale})"
                    else:
                        field_type = f"{field_type}({self.precision})"

        policy_tags = (
            None if self.policy_tags is None else tuple(sorted(self.policy_tags.names))
        )

        return (
            self.name,
            field_type,
            self.mode.upper(),
            self.description,
            self._fields,
        )

    def to_standard_sql(self) -> standard_sql.StandardSqlField:
        sql_type = standard_sql.StandardSqlDataType()

        sql_type.type_kind = LEGACY_TO_STANDARD_TYPES.get(
            self.field_type,
            StandardSqlTypeNames.TYPE_KIND_UNSPECIFIED,
        )

        return standard_sql.StandardSqlField(name=self.name, type=sql_type)

    def __eq__(self, other):
        if not isinstance(other, SchemaField):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        key = self._key()
        adjusted_key = key[:-1]
        return f"{self.__class__.__name__}{adjusted_key}"


def _parse_schema_resource(info):
    return [SchemaField.from_api_repr(f) for f in info.get("fields", ())]


def _build_schema_resource(fields):
    return [field.to_api_repr() for field in fields]


def _to_schema_fields(schema):
    for field in schema:
        if not isinstance(field, (SchemaField, collections.abc.Mapping)):
            raise ValueError(
                "Schema items must either be fields or compatible "
                "mapping representations."
            )

    return [
        field if isinstance(field, SchemaField) else SchemaField.from_api_repr(field)
        for field in schema
    ]
