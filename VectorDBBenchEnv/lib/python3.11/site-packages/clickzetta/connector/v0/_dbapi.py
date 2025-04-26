from typing import Any, Dict, NamedTuple, Optional, Union

from clickzetta.connector.v0.sql_types import (
    STRING,
    BaseCharType,
    DecimalType,
    SqlType,
    proto_to_sql_type,
)


class ColumnDescription(object):
    # ColumnDescription mimics a tuple, as defined in PEP 249, for the description sequence.
    # It also includes an additional property, sql_type, which is not logically part of the tuple.
    # For more information, refer to https://peps.python.org/pep-0249/#description

    def __init__(
        self,
        name: str,
        type_code: str,
        display_size: Optional[int],
        internal_size: Optional[int],
        precision: Optional[int],
        scale: Optional[int],
        null_ok: bool,
        *,
        sql_type: Optional[SqlType] = None,
    ) -> None:
        self._name = name
        self._type_code = type_code
        self._display_size = display_size
        self._internal_size = internal_size
        self._precision = precision
        self._scale = scale
        self._null_ok = null_ok
        self._sql_type = sql_type

    @property
    def name(self) -> str:
        return self._name

    @property
    def type_code(self) -> str:
        return self._type_code

    @property
    def display_size(self) -> Optional[int]:
        return self._display_size

    @property
    def internal_size(self) -> Optional[int]:
        return self._internal_size

    @property
    def precision(self) -> Optional[int]:
        return self._precision

    @property
    def scale(self) -> Optional[int]:
        return self._scale

    @property
    def null_ok(self) -> bool:
        return self._null_ok

    @property
    def sql_type(self) -> SqlType:
        return self._sql_type

    def __repr__(self) -> str:
        return (
            f"ColumnDescription("
            f"name={self.name!r}, "
            f"type_code={self.type_code!r}, "
            f"display_size={self.display_size!r}, "
            f"internal_size={self.internal_size!r}, "
            f"precision={self.precision!r}, "
            f"scale={self.scale!r}, "
            f"null_ok={self.null_ok!r})"
        )

    def __iter__(self) -> Any:
        yield self.name
        yield self.type_code
        yield self.display_size
        yield self.internal_size
        yield self.precision
        yield self.scale
        yield self.null_ok

    def __getitem__(self, item: Union[int, slice]):
        return (
            self.name,
            self.type_code,
            self.display_size,
            self.internal_size,
            self.precision,
            self.scale,
            self.null_ok,
        )[item]

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ColumnDescription):
            return False
        return (
            self.name == value.name
            and self.type_code == value.type_code
            and self.display_size == value.display_size
            and self.internal_size == value.internal_size
            and self.precision == value.precision
            and self.scale == value.scale
            and self.null_ok == value.null_ok
        )

    def __len__(self) -> int:
        return 7


class Field(NamedTuple):
    name: str
    field_type: str
    precision: Optional[int]
    scale: Optional[int]
    length: Optional[int]
    nullable: bool
    sql_type: SqlType

    def to_column_description(self) -> ColumnDescription:
        return ColumnDescription(
            self.name,
            self.field_type,
            None,
            self.length,
            self.precision,
            self.scale,
            self.nullable,
            sql_type=self.sql_type,
        )


MESSAGE_FIELD = Field("RESULT_MESSAGE", "STRING", None, None, None, False, STRING)


def proto_to_field(data: Dict) -> Field:
    name = data["name"]
    sql_type = proto_to_sql_type(data["type"])
    return create_field(name, sql_type)


def create_field(name: str, sql_type: SqlType) -> Field:
    return Field(
        name,
        sql_type.category,
        sql_type.precision if isinstance(sql_type, DecimalType) else None,
        sql_type.scale if isinstance(sql_type, DecimalType) else None,
        sql_type.length if isinstance(sql_type, BaseCharType) else None,
        sql_type.nullable,
        sql_type,
    )
