#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

import re
from enum import Enum
from typing import List, NamedTuple, Optional, Tuple

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

TOKEN_PATTERN = re.compile(
    (
        r"(\s+)|"  # whitespace
        "([<>(),:])|"  # punctuation
        r"(not\s+null)|"  # not null
        "([a-z0-9_]+)|"  # word
        "(`(?:[^`]|``)+`)"  # quoted field string
    ),
    re.I,
)

DIGITS_PATTERN = re.compile(r"\d+")


def _is_int(s: str) -> bool:
    return DIGITS_PATTERN.fullmatch(s) is not None


class TokenType(Enum):
    EOF = 0
    WHITESPACE = 1
    PUNCTUATION = 2
    NOT_NULL = 3
    WORD = 4
    QUOTED = 5
    INVALID = 127


class Token(NamedTuple):
    token_type: TokenType
    content: str
    pos: int


class Tokenizer:
    def __init__(self, text) -> None:
        self._text = text
        self._pos = 0
        self._buf: Optional[Token] = None

    def take(self) -> Token:
        if self._buf is not None:
            token = self._buf
            self._buf = None
            return token

        return self._next(True)

    def peak(self) -> Token:
        if self._buf is None:
            self._buf = self._next(True)
        return self._buf

    def _next(self, omit_whitespace=True) -> Token:
        if self._pos >= len(self._text):
            return Token(TokenType.EOF, "<EOF>", self._pos)

        match = TOKEN_PATTERN.search(self._text, self._pos)
        if match is None:
            return Token(TokenType.INVALID, self._text[self._pos :], self._pos)

        pos = self._pos
        self._pos = match.end()
        token_type = TokenType(match.lastindex)
        if token_type == TokenType.WHITESPACE and omit_whitespace:
            return self._next(omit_whitespace)
        else:
            content = match.group(match.lastindex)
            return Token(token_type, content, pos)


class ParsingError(ValueError):
    def __init__(self, token: Token, expected: str) -> None:
        super().__init__(
            f"[{token.token_type.name}] bad input '{token.content}' at {token.pos}, expecting {expected}"
        )


_NON_PARAMETERIZED_TYPES = {
    "void": NullType(),
    # integral
    "byte": ByteType(),
    "tinyint": ByteType(),
    "short": ShortType(),
    "smallint": ShortType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    # floating point
    "real": FloatType(),
    "float": FloatType(),
    "double": DoubleType(),
    # boolean
    "boolean": BooleanType(),
    # string
    "binary": BinaryType(),
    "string": StringType(),
    # datetime
    "date": DateType(),
    "timestamp": TimestampType(TimestampTimeZone.LTZ),
    "timestamp_ltz": TimestampType(TimestampTimeZone.LTZ),
    "timestamp_ntz": TimestampType(TimestampTimeZone.NTZ),
}

Nullable = bool


class DataTypeParser:
    def __init__(self, tokenizer: Tokenizer) -> None:
        self._tokenizer = tokenizer

    def parse(self) -> DataType:
        data_type, nullable = self._read_type()
        # check eof
        if self._tokenizer.peak().token_type != TokenType.EOF:
            raise ParsingError(self._tokenizer.peak(), "<EOF>")
        return data_type

    def _read_type(self) -> Tuple[DataType, Nullable]:
        token = self._tokenizer.peak()
        name = self._read_word()
        name = name.lower()

        if name in _NON_PARAMETERIZED_TYPES:
            data_type = _NON_PARAMETERIZED_TYPES[name]
        elif name in ("decimal", "number", "numeric"):
            data_type = self._read_ahead_decimal_type()
        elif name in ("char", "varchar"):
            # TODO support char type
            data_type = self._read_ahead_varchar_type(name)
        elif name == "vector":
            data_type = self._read_ahead_vector_type()
        elif name == "array":
            data_type = self._read_ahead_array_type()
        elif name == "map":
            data_type = self._read_ahead_map_type()
        elif name == "struct":
            data_type = self._read_ahead_struct_type()
        else:
            raise ParsingError(token, "valid data type")

        nullable = True
        if self._tokenizer.peak().token_type == TokenType.NOT_NULL:
            self._tokenizer.take()
            nullable = False
        return (data_type, nullable)

    def _read_ahead_decimal_type(self) -> DecimalType:
        # valid formats:
        #   - decimal(10,1)
        #   - decimal(10) (alias to decimal(10,0)
        #   - decimal (alias to decimal(10,0))
        if self._read_punctuation("(", try_=True):
            precision = self._read_int()
            if self._read_punctuation(",", try_=True):
                scale = self._read_int()
            else:
                scale = 0
            self._read_punctuation(")")
            return DecimalType(precision, scale)
        else:
            return DecimalType(10, 0)

    def _read_ahead_varchar_type(self, name: str) -> StringType:
        # valid formats:
        #   - varchar(10)
        #   - varchar
        if self._read_punctuation("(", try_=True):
            length = self._read_int()
            self._read_punctuation(")")
            return StringType(length)
        else:
            return StringType()

    def _read_ahead_vector_type(self) -> VectorType:
        # valid formats:
        #   - vector(4) (eq to vector(int, 4)),
        #   - vector(float, 4)
        # TODO support full vector types
        self._read_punctuation("(", try_=True)
        t = self._read_word()
        if _is_int(t):
            element_type = "int"
            dimension = int(t)
        else:
            element_type = t
            self._read_punctuation(",")
            dimension = self._read_int()
        self._read_punctuation(")")
        return VectorType(element_type, dimension)

    def _read_ahead_array_type(self) -> ArrayType:
        self._read_punctuation("<")
        element_type, nullable = self._read_type()
        self._read_punctuation(">")
        return ArrayType(element_type)

    def _read_ahead_map_type(self) -> MapType:
        self._read_punctuation("<")
        key_type, nullable = self._read_type()
        self._read_punctuation(",")
        value_type, nullable = self._read_type()
        self._read_punctuation(">")
        return MapType(key_type, value_type)

    def _read_ahead_struct_type(self) -> StructType:
        fields: List[StructField] = []
        self._read_punctuation("<")
        while True:
            field_name = self._try_read_quoted()
            if field_name is None:
                field_name = self._read_word()
            self._read_punctuation(":")
            field_type, nullable = self._read_type()
            fields.append(StructField(field_name, field_type, nullable))
            if not self._read_punctuation(",", try_=True):
                break
        self._read_punctuation(">")
        return StructType(fields)

    def _read_punctuation(self, punctuation: str, try_=False) -> bool:
        token = self._tokenizer.peak()
        if token.token_type == TokenType.PUNCTUATION:
            if token.content == punctuation:
                self._tokenizer.take()
                return True
        if try_:
            return False
        raise ParsingError(token, f"'{punctuation}'")

    def _read_word(self) -> str:
        token = self._tokenizer.take()
        if token.token_type != TokenType.WORD:
            raise ParsingError(token, "a type name")
        return token.content

    def _read_int(self) -> int:
        token = self._tokenizer.peak()
        content = token.content
        if token.token_type != TokenType.WORD or not DIGITS_PATTERN.match(content):
            raise ParsingError(token, "an integer")
        self._tokenizer.take()
        return int(content)

    def _try_read_quoted(self) -> Optional[str]:
        token = self._tokenizer.peak()
        if token.token_type == TokenType.QUOTED:
            self._tokenizer.take()
            return token.content[1:-1].replace("``", "`")
        return None


def parse_data_type(t: str) -> DataType:
    return DataTypeParser(Tokenizer(t)).parse()
