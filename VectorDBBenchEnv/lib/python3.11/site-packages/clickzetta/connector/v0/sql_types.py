import re
from enum import Enum
from typing import Dict, NamedTuple, Sequence, Tuple, Union


class _SqlTypeMeta(type):
    @staticmethod
    def _generate_eq(eq_attrs):
        assert isinstance(eq_attrs, Sequence)
        assert all(isinstance(x, str) for x in eq_attrs)

        def _eq(self, r: object) -> bool:
            return (
                isinstance(r, self.__class__)
                and self.nullable == r.nullable
                and all(getattr(self, x) == getattr(r, x) for x in eq_attrs)
            )

        return _eq

    @staticmethod
    def _patch_new(new_class, name: str):
        original_new = new_class.__new__

        def patched_new(cls_, *args, **kwargs):
            if cls_.__name__ == name:
                raise TypeError(f"Cannot instantiate {cls_.__name__} directly.")
            return original_new(cls_)

        new_class.__new__ = patched_new

    def __new__(cls, name, bases, attrs, **kwargs):
        short_name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()[:-5]
        attrs["_SHORT_NAME"] = short_name

        category = kwargs.get("CATEGORY", None)
        if category:
            assert isinstance(category, str)
            attrs["_CATEGORY"] = kwargs.get("CATEGORY")
        else:
            attrs["_CATEGORY"] = short_name.upper()

        eq_attrs = kwargs.get("EQ", None)
        if eq_attrs:
            attrs["__eq__"] = _SqlTypeMeta._generate_eq(eq_attrs)

        new_class = super().__new__(cls, name, bases, attrs)
        if kwargs.get("_ABS", False):
            _SqlTypeMeta._patch_new(new_class, name)
        return new_class


class SqlType(metaclass=_SqlTypeMeta, _ABS=True):
    def __init__(self, nullable: bool = True) -> None:
        super().__init__()
        self.nullable = nullable

    @property
    def category(self) -> str:
        return self.__class__._CATEGORY

    def accept(self, visitor):
        return getattr(visitor, f"visit_{self.__class__._SHORT_NAME}")(self)

    def __eq__(self, r: object) -> bool:
        return isinstance(r, self.__class__) and self.nullable == r.nullable

    @property
    def _not_null_s(self) -> str:
        return " not null" if not self.nullable else ""


class PrimitiveType(SqlType, _ABS=True):
    def __str__(self) -> str:
        return f"{self.__class__._SHORT_NAME}{self._not_null_s}"


class VoidType(PrimitiveType):
    def __init__(self) -> None:
        super().__init__(True)


class NumericType(PrimitiveType, _ABS=True):
    ...


class IntegralType(NumericType, _ABS=True):
    ...


class TinyintType(IntegralType, CATEGORY="INT8"):
    ...


class SmallintType(IntegralType, CATEGORY="INT16"):
    ...


class IntType(IntegralType, CATEGORY="INT32"):
    ...


class BigintType(IntegralType, CATEGORY="INT64"):
    ...


class FloatingType(NumericType, _ABS=True):
    ...


class FloatType(FloatingType, CATEGORY="FLOAT32"):
    ...


class DoubleType(FloatingType, CATEGORY="FLOAT64"):
    ...


class DecimalType(NumericType, EQ=["precision", "scale"]):
    def __init__(self, precision: int, scale: int, nullable: bool = True) -> None:
        super().__init__(nullable)
        self.precision = precision
        self.scale = scale

    def __str__(self) -> str:
        return f"decimal({self.precision},{self.scale}){self._not_null_s}"


class BooleanType(PrimitiveType):
    ...


class TextType(PrimitiveType, _ABS=True):
    ...


class BaseCharType(TextType, _ABS=True, EQ=["length"]):
    def __init__(self, length: int, nullable: bool = True) -> None:
        super().__init__(nullable)
        self.length = length

    def __str__(self) -> str:
        return f"{self.__class__._SHORT_NAME}({self.length}){self._not_null_s}"


class CharType(BaseCharType):
    ...


class VarcharType(BaseCharType):
    ...


class StringType(TextType):
    ...


class BinaryType(PrimitiveType):
    ...


class DateType(PrimitiveType):
    ...


class TimestampUnit(Enum):
    SECONDS = 0
    MILLISECONDS = 3
    MICROSECONDS = 6
    NANOSECONDS = 9


class TimestampType(PrimitiveType, _ABS=True, EQ=["unit"]):
    def __init__(
        self,
        unit: TimestampUnit,
        nullable: bool = True,
    ) -> None:
        super().__init__(nullable)
        self.unit = unit


class TimestampLtzType(TimestampType):
    ...


class TimestampNtzType(TimestampType):
    ...


class IntervalUnit(Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4
    MINUTE = 5
    SECOND = 6


class IntervalType(PrimitiveType, _ABS=True):
    ...


class IntervalYearMonthType(IntervalType, EQ=["from_", "to"]):
    def __init__(
        self, from_: IntervalUnit, to: IntervalUnit, nullable: bool = True
    ) -> None:
        super().__init__(nullable)
        self.from_ = from_
        self.to = to

    _NAME_MAPPING = {
        (IntervalUnit.YEAR, IntervalUnit.YEAR): "year",
        (IntervalUnit.YEAR, IntervalUnit.MONTH): "year to month",
        (IntervalUnit.MONTH, IntervalUnit.MONTH): "month",
    }

    def __str__(self) -> str:
        name = IntervalYearMonthType._NAME_MAPPING.get((self.from_, self.to), None)
        if name:
            return f"interval {name}{self._not_null_s}"
        raise TypeError(
            "Invalid IntervalYearMonthType ({self.from_.name}, {self.to.name})"
        )


class IntervalDayTimeType(IntervalType, EQ=["from_", "to", "precision"]):
    def __init__(
        self,
        from_: IntervalUnit,
        to: IntervalUnit,
        precision: TimestampUnit,
        nullable: bool = True,
    ) -> None:
        super().__init__(nullable)
        self.from_ = from_
        self.to = to
        self.precision = precision

    _NAME_MAPPING = {
        (IntervalUnit.DAY, IntervalUnit.DAY): "day",
        (IntervalUnit.HOUR, IntervalUnit.HOUR): "hour",
        (IntervalUnit.MINUTE, IntervalUnit.MINUTE): "minute",
        (IntervalUnit.SECOND, IntervalUnit.SECOND): "second",
        (IntervalUnit.DAY, IntervalUnit.HOUR): "day to hour",
        (IntervalUnit.DAY, IntervalUnit.MINUTE): "day to minute",
        (IntervalUnit.DAY, IntervalUnit.SECOND): "day to second",
        (IntervalUnit.HOUR, IntervalUnit.MINUTE): "hour to minute",
        (IntervalUnit.HOUR, IntervalUnit.SECOND): "hour to second",
        (IntervalUnit.MINUTE, IntervalUnit.SECOND): "minute to second",
    }

    def __str__(self) -> str:
        name = IntervalDayTimeType._NAME_MAPPING.get((self.from_, self.to), None)
        if name:
            return f"interval {name}{self._not_null_s}"
        raise TypeError(
            "Invalid IntervalDayTimeType ({self.from_.name}, {self.to.name})"
        )


class BitmapType(PrimitiveType):
    ...


class NestedType(SqlType, _ABS=True):
    ...


class ArrayType(NestedType, EQ=["element_type"]):
    def __init__(self, element_type: SqlType, nullable: bool = True) -> None:
        super().__init__(nullable)
        self.element_type = element_type

    def __str__(self) -> str:
        return f"array<{self.element_type}>{self._not_null_s}"


class MapType(NestedType, EQ=["key_type", "value_type"]):
    def __init__(
        self, key_type: SqlType, value_type: SqlType, nullable: bool = True
    ) -> None:
        super().__init__(nullable)
        self.key_type = key_type
        self.value_type = value_type

    def __str__(self) -> str:
        return f"map<{self.key_type},{self.value_type}>{self._not_null_s}"


class StructField(NamedTuple):
    name: str
    type_: SqlType

    def __eq__(self, r: object) -> bool:
        return (
            isinstance(r, StructField)
            and self.name.lower() == r.name.lower()
            and self.type_ == r.type_
        )


StructFieldLike = Union[StructField, Tuple[str, SqlType]]


class StructType(NestedType):
    def __init__(
        self, fields: Sequence[StructFieldLike], nullable: bool = True
    ) -> None:
        super().__init__(nullable)
        self.fields = StructType._normalize_fields(fields)

    @staticmethod
    def _normalize_fields(fields: Sequence[StructFieldLike]):
        return [f if isinstance(f, StructField) else StructField(*f) for f in fields]

    def __eq__(self, r: object) -> bool:
        return (
            isinstance(r, StructType)
            and self.nullable == r.nullable
            and len(self.fields) == len(r.fields)
            and all(s == r for s, r in zip(self.fields, r.fields))
        )

    def __str__(self) -> str:
        return f"struct<{','.join([f'{f.name}:{f.type_}' for f in self.fields])}>{self._not_null_s}"


class JsonType(PrimitiveType):
    ...


class VectorNumberType(Enum):
    TINYINT = "I8"
    INT = "I32"
    FLOAT16 = "F16"
    FLOAT = "F32"


class VectorType(
    PrimitiveType, CATEGORY="VECTOR_TYPE", EQ=["number_type", "dimension"]
):
    def __init__(
        self,
        number_type: VectorNumberType,
        dimension: int,
        nullable: bool = True,
    ) -> None:
        super().__init__(nullable)
        self.number_type = number_type
        self.dimension = dimension

    def __str__(self) -> str:
        return f"vector({self.number_type.name.lower()},{self.dimension}){self._not_null_s}"


VOID = VoidType()
TINYINT = TinyintType()
SMALLINT = SmallintType()
INT = IntType()
BIGINT = BigintType()
FLOAT = FloatType()
DOUBLE = DoubleType()
BOOLEAN = BooleanType()
STRING = StringType()
BINARY = BinaryType()
DATE = DateType()
BITMAP = BitmapType()
JSON = JsonType()

_NON_PARAMETERIZED_TYPES = (
    VOID,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING,
    BINARY,
    DATE,
    BITMAP,
    JSON,
)

# A mapping of category -> (nullable_singleton, non_nullable_singleton)
_CATEGORY_TO_NON_PARAMETERIZED_TYPES = {
    type_.category: (type_, type_.__class__(False) if type_ is not VOID else VOID)
    for type_ in _NON_PARAMETERIZED_TYPES
}


class SqlTypeVisitor:
    def visit_any(self, t: SqlType):
        pass

    def visit_primitive(self, t: PrimitiveType):
        return self.visit_any(t)

    def visit_void(self, t: VoidType):
        return self.visit_primitive(t)

    def visit_numeric(self, t: NumericType):
        return self.visit_primitive(t)

    def visit_integral(self, t: IntegralType):
        return self.visit_numeric(t)

    def visit_tinyint(self, t: TinyintType):
        return self.visit_integral(t)

    def visit_smallint(self, t: SmallintType):
        return self.visit_integral(t)

    def visit_int(self, t: IntType):
        return self.visit_integral(t)

    def visit_bigint(self, t: BigintType):
        return self.visit_integral(t)

    def visit_floating(self, t: FloatingType):
        return self.visit_numeric(t)

    def visit_float(self, t: FloatType):
        return self.visit_floating(t)

    def visit_double(self, t: DoubleType):
        return self.visit_floating(t)

    def visit_decimal(self, t: DecimalType):
        return self.visit_numeric(t)

    def visit_boolean(self, t: BooleanType):
        return self.visit_primitive(t)

    def visit_text(self, t: TextType):
        return self.visit_primitive(t)

    def visit_char(self, t: CharType):
        return self.visit_text(t)

    def visit_varchar(self, t: VarcharType):
        return self.visit_text(t)

    def visit_string(self, t: StringType):
        return self.visit_text(t)

    def visit_binary(self, t: BinaryType):
        return self.visit_primitive(t)

    def visit_date(self, t: DateType):
        return self.visit_primitive(t)

    def visit_timestamp(self, t: TimestampType):
        return self.visit_primitive(t)

    def visit_timestamp_ltz(self, t: TimestampLtzType):
        return self.visit_timestamp(t)

    def visit_timestamp_ntz(self, t: TimestampNtzType):
        return self.visit_timestamp(t)

    def visit_interval(self, t: IntervalType):
        return self.visit_primitive(t)

    def visit_interval_year_month(self, t: IntervalYearMonthType):
        return self.visit_interval(t)

    def visit_interval_day_time(self, t: IntervalDayTimeType):
        return self.visit_interval(t)

    def visit_bitmap(self, t: BitmapType):
        return self.visit_primitive(t)

    def visit_nested(self, t: NestedType):
        return self.visit_any(t)

    def visit_array(self, t: ArrayType):
        return self.visit_nested(t)

    def visit_map(self, t: MapType):
        return self.visit_nested(t)

    def visit_struct(self, t: StructType):
        return self.visit_nested(t)

    def visit_json(self, t: JsonType):
        return self.visit_primitive(t)

    def visit_vector(self, t: VectorType):
        return self.visit_primitive(t)


_TIMESTAMP_UNITS = {unit.name: unit for unit in TimestampUnit}


def _parse_timestamp_unit(v) -> TimestampUnit:
    unit = _TIMESTAMP_UNITS.get(v, None)
    if unit:
        return unit
    raise ValueError(f"Invalid timestamp unit: {v}")


_INTERVAL_UNITS = {unit.name: unit for unit in IntervalUnit}


def _parse_interval_unit(v) -> IntervalUnit:
    unit = _INTERVAL_UNITS.get(v, None)
    if unit:
        return unit
    raise ValueError(f"Invalid interval unit: {v}")


_VECTOR_NUMBER_TYPES = {t.value: t for t in VectorNumberType}


def _parse_vector_number_type(v) -> VectorNumberType:
    t = _VECTOR_NUMBER_TYPES.get(v, None)
    if t:
        return t
    raise ValueError(f"Invalid vector number type: {v}")


def proto_to_sql_type(proto: Dict) -> SqlType:
    try:
        nullable = proto["nullable"]
        category = proto["category"]

        # non parameterized type
        (
            nullable_t,
            non_nullable_t,
        ) = _CATEGORY_TO_NON_PARAMETERIZED_TYPES.get(category, (None, None))
        if nullable_t is not None:
            return nullable_t if nullable else non_nullable_t

        # parameterized type
        if category == "CHAR":
            length = int(proto["charTypeInfo"]["length"])
            return CharType(length, nullable)
        elif category == "VARCHAR":
            length = int(proto["varCharTypeInfo"]["length"])
            return VarcharType(length, nullable)
        elif category == "DECIMAL":
            info = proto["decimalTypeInfo"]
            return DecimalType(int(info["precision"]), int(info["scale"]), nullable)
        elif category == "ARRAY":
            element_type = proto_to_sql_type(proto["arrayTypeInfo"]["elementType"])
            return ArrayType(element_type, nullable)
        elif category == "MAP":
            key_type = proto_to_sql_type(proto["mapTypeInfo"]["keyType"])
            value_type = proto_to_sql_type(proto["mapTypeInfo"]["valueType"])
            return MapType(key_type, value_type, nullable)
        elif category == "STRUCT":
            struct_fields = [
                StructField(name=field["name"], type_=proto_to_sql_type(field["type"]))
                for field in proto["structTypeInfo"]["fields"]
            ]
            return StructType(struct_fields, nullable)
        elif category == "INTERVAL_DAY_TIME":
            info = proto["intervalDayTimeInfo"]
            from_ = _parse_interval_unit(info["from"])
            to = _parse_interval_unit(info["to"])
            precision = _parse_timestamp_unit(info["precision"])
            return IntervalDayTimeType(from_, to, precision, nullable)
        elif category == "INTERVAL_YEAR_MONTH":
            info = proto["intervalYearMonthInfo"]
            from_ = _parse_interval_unit(info["from"])
            to = _parse_interval_unit(info["to"])
            return IntervalYearMonthType(from_, to, nullable)
        elif category == "TIMESTAMP_LTZ":
            info = proto["timestampInfo"]
            unit = _parse_timestamp_unit(info["tsUnit"])
            return TimestampLtzType(unit, nullable)
        elif category == "TIMESTAMP_NTZ":
            info = proto["timestampInfo"]
            unit = _parse_timestamp_unit(info["tsUnit"])
            return TimestampNtzType(unit, nullable)
        elif category == "VECTOR_TYPE":
            info = proto["vectorInfo"]
            number_type = _parse_vector_number_type(info["numberType"])
            return VectorType(number_type, info["dimension"], nullable)
        elif category == "VOID":
            return VoidType()
        raise ValueError(f"Invalid category: {category}")
    except KeyError:
        raise ValueError(f"Invalid type proto: {proto}")
