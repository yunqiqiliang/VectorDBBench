#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

from typing import AbstractSet, Optional

from clickzetta.zettapark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from clickzetta.zettapark._internal.analyzer.unary_expression import MathUnaryExpression


class BinaryExpression(Expression):
    sql_operator: str

    def __init__(self, left: Expression, right: Expression) -> None:
        super().__init__()
        self.left = left
        self.right = right
        self.children = [self.left, self.right]
        self.nullable = self.left.nullable or self.right.nullable

    def __str__(self):
        return f"{self.left} {self.sql_operator} {self.right}"

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.left, self.right)


class BinaryArithmeticExpression(BinaryExpression):
    @property
    def column_name(self) -> str:
        return f"{self.left.column_name} {self.sql_operator} {self.right.column_name}"


class EqualTo(BinaryArithmeticExpression):
    sql_operator = "="


class NotEqualTo(BinaryArithmeticExpression):
    sql_operator = "<>"


class GreaterThan(BinaryArithmeticExpression):
    sql_operator = ">"


class LessThan(BinaryArithmeticExpression):
    sql_operator = "<"


class GreaterThanOrEqual(BinaryArithmeticExpression):
    sql_operator = ">="


class LessThanOrEqual(BinaryArithmeticExpression):
    sql_operator = "<="


class EqualNullSafe(BinaryExpression):
    sql_operator = "EQUAL_NULL"


class And(BinaryArithmeticExpression):
    sql_operator = "AND"


class Or(BinaryArithmeticExpression):
    sql_operator = "OR"


class Add(BinaryArithmeticExpression):
    sql_operator = "+"


class Subtract(BinaryArithmeticExpression):
    sql_operator = "-"


class Multiply(BinaryArithmeticExpression):
    sql_operator = "*"


class Divide(BinaryArithmeticExpression):
    sql_operator = "/"


class Remainder(BinaryArithmeticExpression):
    sql_operator = "%"


class Pow(BinaryExpression):
    sql_operator = "POWER"


class BitwiseAnd(MathUnaryExpression):
    sql_operator = "bit_and"
    operator_first = True


class BitwiseOr(MathUnaryExpression):
    sql_operator = "bit_or"
    operator_first = True


class BitwiseXor(MathUnaryExpression):
    sql_operator = "bit_xor"
    operator_first = True
