#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import functools
import pickle
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union, overload

import cloudpickle

import clickzetta.zettapark
from clickzetta.zettapark._internal.analyzer.analyzer_utils import create_udf_statement
from clickzetta.zettapark._internal.analyzer.expression import UDF, Expression
from clickzetta.zettapark._internal.type_utils import (
    ColumnOrName,
    convert_data_type_to_name,
    type_string_to_type_object,
)
from clickzetta.zettapark._internal.udf.udf_utils import check_runtime_version
from clickzetta.zettapark._internal.utils import (
    SINGLE_IDENTIFIER_RE,
    TempObjectType,
    generate_random_alphanumeric,
)
from clickzetta.zettapark.column import Column
from clickzetta.zettapark.exceptions import ZettaparkInvalidObjectNameException
from clickzetta.zettapark.types import DataType, StringType

if TYPE_CHECKING:
    import clickzetta.zettapark.session

DEFAULT_RETURN_TYPE = StringType()


class UserDefinedFunction:
    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        name: str,
        identifier: str,
        session: "clickzetta.zettapark.session.Session",
    ) -> None:
        self._func = func
        self._return_type = return_type
        self._name = name
        self._identifier = identifier
        self._session = session

    @property
    def identifier(self) -> str:
        return self._identifier

    def __call__(self, *args: ColumnOrName) -> Column:
        exprs: List[Expression] = []
        for c in args:
            if isinstance(c, Column):
                exprs.append(c._expression)
            elif isinstance(c, str):
                exprs.append(Column(c)._expression)
            else:
                raise TypeError(
                    f"The input of UDF {self._name} must be Column, column name, or a list of them"
                )
        return Column(
            UDF(
                self._identifier,
                exprs,
                self._return_type,
                nullable=True,
                api_call_source="UserDefinedFunction.__call__",
                callable_name=self._name,
            )
        )


def _create_handler_script(
    func: Callable,
    return_type: DataType,
    handler_class: str,
) -> str:
    encoded_func = cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL).hex()
    return_type_str = convert_data_type_to_name(return_type)
    script = f"""
from cz.udf import annotate

def _decode_func():
    import pickle
    return pickle.loads(bytes.fromhex('{encoded_func}'))

@annotate("*->{return_type_str}")
class {handler_class}:
    def __init__(self):
        self._func = _decode_func()

    def evaluate(self, *args):
        return self._func(*args)
"""
    return script


class UDFRegistration:
    """Wrapper for user-defined function registration."""

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self.session = session
        self._all_identifiers = set()
        self._name_to_identifier = {}

    def register(
        self, name: str, f: Callable, return_type: Optional[DataType] = None
    ) -> UserDefinedFunction:
        t = return_type or DEFAULT_RETURN_TYPE
        return self._create_udf(f, t, name)

    def _write_to_volume(self, volume_path: str, content: str) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(content.encode())
            temp_file.close()
            self.session.file.put(temp_file.name, volume_path)

    def _to_identifier(self, name: str) -> Optional[str]:
        name = name.lower()
        if name in self._all_identifiers:
            return name
        if name in self._name_to_identifier:
            return self._name_to_identifier[name]
        return None

    def _create_udf(
        self,
        f: Callable,
        return_type: DataType,
        name: Optional[str] = None,
    ) -> UserDefinedFunction:
        if name is not None and not SINGLE_IDENTIFIER_RE.match(name):
            raise ZettaparkInvalidObjectNameException(f"Invalid UDF name: {name}")
        if not callable(f):
            raise TypeError(f"Invalid function: not a function or callable: {type(f)}")
        if not name:
            name = f.__name__

        handler_module = f"zp_{datetime.now().strftime('%Y%m%d%H%M%S')}_{generate_random_alphanumeric(16)}"
        handler_class = "Evaluator"
        handler_script = _create_handler_script(f, return_type, handler_class)
        handler_file = (
            f"{self.session._session_volume_directory}udf/{handler_module}.py"
        )
        self._write_to_volume(handler_file, handler_script)
        schema = self.session.get_current_schema()
        ddl = create_udf_statement(schema, handler_module, handler_class, handler_file)
        identifier = f"{schema}.{handler_module}".lower()
        self.session._register_temp_object(TempObjectType.FUNCTION, identifier)
        self.session._run_query(ddl)
        self._all_identifiers.add(identifier)
        self._name_to_identifier[name.lower()] = identifier
        return UserDefinedFunction(f, return_type, name, identifier, self.session)


@overload
def udf(
    f: Callable[..., Any], return_type: Union[DataType, str] = DEFAULT_RETURN_TYPE
) -> UserDefinedFunction: ...


@overload
def udf(
    return_type: Optional[Union[DataType, str]] = None,
) -> Callable[[Callable[..., Any]], UserDefinedFunction]: ...


def udf(
    f: Optional[Union[Callable[..., Any], DataType, str]] = None,
    return_type: Union[DataType, str] = DEFAULT_RETURN_TYPE,
) -> Union[UserDefinedFunction, Callable[[Callable[..., Any]], UserDefinedFunction]]:
    """Create a user defined function (UDF)"""
    check_runtime_version()
    session = clickzetta.zettapark.session._get_active_session()
    reg = session.udf
    if f is None or isinstance(f, (str, DataType)):
        return_type = f or return_type
        if isinstance(return_type, str):
            return_type = type_string_to_type_object(return_type)
        return functools.partial(
            reg._create_udf,
            return_type=return_type,
            name=None,
        )
    if not callable(f):
        raise TypeError(f"Invalid function: not a function or callable: {type(f)}")
    return reg._create_udf(f, return_type, name=None)
