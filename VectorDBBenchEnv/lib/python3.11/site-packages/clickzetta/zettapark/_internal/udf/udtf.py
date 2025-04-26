#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import functools
from typing import TYPE_CHECKING, Callable, Optional, Type, Union

from clickzetta.zettapark._internal.type_utils import ColumnOrName
from clickzetta.zettapark.table_function import TableFunctionCall
from clickzetta.zettapark.types import StructType

if TYPE_CHECKING:
    import clickzetta.zettapark.session


class UserDefinedTableFunction:
    def __init__(self) -> None: ...

    def __call__(self, *args: ColumnOrName) -> TableFunctionCall: ...


class UDTFRegistration:
    """Wrapper for user-defined function registration."""

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self.session = session

    def register(self, *args) -> UserDefinedTableFunction: ...


def _create_udtf(
    cls: Optional[Type] = None,
    *,
    return_type: Union[StructType, str],
    use_arrow: Optional[bool] = None,
) -> UserDefinedTableFunction: ...


def udtf(
    cls: Optional[Type] = None,
    *,
    return_type: Union[StructType, str],
    use_arrow: Optional[bool] = None,
) -> Union[UserDefinedTableFunction, Callable[[Type], UserDefinedTableFunction]]:
    """Create a user defined table function (UDTF)"""

    if cls is None or isinstance(cls, (str, StructType)):
        return_type = cls or return_type
        return functools.partial(
            _create_udtf,
            return_type=return_type,
            use_arrow=use_arrow,
        )
    return _create_udtf(cls, return_type)
