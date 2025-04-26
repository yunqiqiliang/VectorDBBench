#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import functools
from typing import TYPE_CHECKING, Callable, Optional, Type, Union

from clickzetta.zettapark._internal.analyzer.expression import UDF
from clickzetta.zettapark._internal.type_utils import ColumnOrName
from clickzetta.zettapark.column import Column
from clickzetta.zettapark.types import DataType

if TYPE_CHECKING:
    import clickzetta.zettapark.session


class UserDefinedAggregateFunction:
    def __init__(self) -> None: ...

    def __call__(self, *args: ColumnOrName) -> Column:
        exprs = []
        for c in args:
            if isinstance(c, Column):
                exprs.append(c._expression)
            elif isinstance(c, str):
                exprs.append(Column(c)._expression)
            else:
                raise TypeError(
                    f"The input of UDF {self.name} must be Column, column name, or a list of them"
                )
        return UDF(
            self.name,
            exprs,
            self._return_type,
            nullable=True,
            api_call_source="UserDefinedFunction.__call__",
        )


class UDAFRegistration:
    """Wrapper for user-defined function registration."""

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self.session = session

    def register(self, *args) -> UserDefinedAggregateFunction: ...


def _create_udaf(
    cls: Optional[Type] = None,
    *,
    return_type: Union[DataType, str],
    use_arrow: Optional[bool] = None,
) -> UserDefinedAggregateFunction: ...


def udaf(
    cls: Optional[Type] = None,
    *,
    return_type: Union[DataType, str],
    use_arrow: Optional[bool] = None,
) -> Union[
    UserDefinedAggregateFunction, Callable[[Type], UserDefinedAggregateFunction]
]:
    """Create a user defined aggregate function (UDAF)"""

    if cls is None or isinstance(cls, (str, DataType)):
        return_type = cls or return_type
        return functools.partial(
            _create_udaf,
            return_type=return_type,
            use_arrow=use_arrow,
        )
    return _create_udaf(cls, return_type)
