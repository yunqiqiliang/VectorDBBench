import copy
import typing
from typing import Any, Dict, Iterable, List, Optional

from .enums import StandardSqlTypeNames


class StandardSqlDataType:
    def __init__(
        self,
        type_kind: Optional[
            StandardSqlTypeNames
        ] = StandardSqlTypeNames.TYPE_KIND_UNSPECIFIED,
    ):
        self._properties: Dict[str, Any] = {}

        self.type_kind = type_kind

    @property
    def type_kind(self) -> Optional[StandardSqlTypeNames]:
        kind = self._properties["typeKind"]
        return StandardSqlTypeNames[kind]

    @type_kind.setter
    def type_kind(self, value: Optional[StandardSqlTypeNames]):
        if not value:
            kind = StandardSqlTypeNames.TYPE_KIND_UNSPECIFIED.value
        else:
            kind = value.value
        self._properties["typeKind"] = kind

    def to_api_repr(self) -> Dict[str, Any]:
        return copy.deepcopy(self._properties)

    @classmethod
    def from_api_repr(cls, resource: Dict[str, Any]):
        type_kind = resource.get("typeKind")
        if type_kind not in StandardSqlTypeNames.__members__:
            type_kind = StandardSqlTypeNames.TYPE_KIND_UNSPECIFIED
        else:
            type_kind = StandardSqlTypeNames[typing.cast(str, type_kind)]

        return cls(type_kind)

    def __eq__(self, other):
        if not isinstance(other, StandardSqlDataType):
            return NotImplemented
        else:
            return (
                self.type_kind == other.type_kind
                and self.array_element_type == other.array_element_type
                and self.struct_type == other.struct_type
            )

    def __str__(self):
        result = f"{self.__class__.__name__}(type_kind={self.type_kind!r}, ...)"
        return result


class StandardSqlField:
    def __init__(
        self, name: Optional[str] = None, type: Optional[StandardSqlDataType] = None
    ):
        type_repr = None if type is None else type.to_api_repr()
        self._properties = {"name": name, "type": type_repr}

    @property
    def name(self) -> Optional[str]:
        return typing.cast(Optional[str], self._properties["name"])

    @name.setter
    def name(self, value: Optional[str]):
        self._properties["name"] = value

    @property
    def type(self) -> Optional[StandardSqlDataType]:
        type_info = self._properties["type"]

        if type_info is None:
            return None

        result = StandardSqlDataType()
        result._properties = typing.cast(Dict[str, Any], type_info)

        return result

    @type.setter
    def type(self, value: Optional[StandardSqlDataType]):
        value_repr = None if value is None else value.to_api_repr()
        self._properties["type"] = value_repr

    def to_api_repr(self) -> Dict[str, Any]:
        return copy.deepcopy(self._properties)

    @classmethod
    def from_api_repr(cls, resource: Dict[str, Any]):
        result = cls(
            name=resource.get("name"),
            type=StandardSqlDataType.from_api_repr(resource.get("type", {})),
        )
        return result

    def __eq__(self, other):
        if not isinstance(other, StandardSqlField):
            return NotImplemented
        else:
            return self.name == other.name and self.type == other.type


class StandardSqlTableType:
    def __init__(self, columns: Iterable[StandardSqlField]):
        self._properties = {"columns": [col.to_api_repr() for col in columns]}

    @property
    def columns(self) -> List[StandardSqlField]:
        result = []

        for column_resource in self._properties.get("columns", []):
            column = StandardSqlField()
            column._properties = column_resource  # We do not use a copy on purpose.
            result.append(column)

        return result

    @columns.setter
    def columns(self, value: Iterable[StandardSqlField]):
        self._properties["columns"] = [col.to_api_repr() for col in value]

    def to_api_repr(self) -> Dict[str, Any]:
        return copy.deepcopy(self._properties)

    @classmethod
    def from_api_repr(cls, resource: Dict[str, Any]) -> "StandardSqlTableType":
        columns = []

        for column_resource in resource.get("columns", []):
            type_ = column_resource.get("type")
            if type_ is None:
                type_ = {}

            column = StandardSqlField(
                name=column_resource.get("name"),
                type=StandardSqlDataType.from_api_repr(type_),
            )
            columns.append(column)

        return cls(columns=columns)

    def __eq__(self, other):
        if not isinstance(other, StandardSqlTableType):
            return NotImplemented
        else:
            return self.columns == other.columns
