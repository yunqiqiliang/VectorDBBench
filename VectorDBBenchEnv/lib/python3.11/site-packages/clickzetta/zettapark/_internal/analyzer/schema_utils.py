#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

from typing import TYPE_CHECKING, List

import clickzetta.zettapark
from clickzetta.zettapark._connector import ClickzettaCursor, ResultMetadata
from clickzetta.zettapark._internal.analyzer.expression import Attribute
from clickzetta.zettapark._internal.type_utils import convert_metadata_to_data_type
from clickzetta.zettapark._internal.utils import quote_name
from clickzetta.zettapark.types import DecimalType, LongType, StringType

if TYPE_CHECKING:
    import clickzetta.zettapark.session


def command_attributes() -> List[Attribute]:
    return [Attribute('"status"', StringType())]


# def show_tables_attributes() -> List[Attribute]:
#     # schema_name,table_name,is_view,is_materialized_view
#     return [
#         Attribute('"schema_name"', StringType(), nullable=False),
#         Attribute('"table_name"', StringType(), nullable=False),
#         Attribute('"is_view"', BooleanType(), nullable=False),
#         Attribute('"is_materialized_view"', BooleanType(), nullable=False),
#         Attribute('"is_external"', BooleanType(), nullable=False),
#         Attribute('"is_dynamic"', BooleanType(), nullable=False),
#     ]


# def show_volume_attributes() -> List[Attribute]:
#     # relative_path,url,size,last_modified_time
#     return [
#         Attribute('"relative_path"', StringType()),
#         Attribute('"url"', StringType()),
#         Attribute('"size"', LongType()),
#         Attribute('"last_modified_time"', StringType()),
#     ]


def list_volume_attributes() -> List[Attribute]:
    return [
        Attribute('"name"', StringType()),
        Attribute('"size"', LongType()),
        Attribute('"md5"', StringType()),
        Attribute('"last_modified"', StringType()),
    ]


def remove_state_file_attributes() -> List[Attribute]:
    return [Attribute('"name"', StringType()), Attribute('"result"', StringType())]


def put_attributes() -> List[Attribute]:
    return [
        Attribute('"source"', StringType(), nullable=False),
        Attribute('"target"', StringType(), nullable=False),
        Attribute('"source_size"', DecimalType(10, 0), nullable=False),
        Attribute('"target_size"', DecimalType(10, 0), nullable=False),
        Attribute('"source_compression"', StringType(), nullable=False),
        Attribute('"target_compression"', StringType(), nullable=False),
        Attribute('"status"', StringType(), nullable=False),
        Attribute('"encryption"', StringType(), nullable=False),
        Attribute('"message"', StringType(), nullable=False),
    ]


def get_attributes() -> List[Attribute]:
    return [
        Attribute('"file"', StringType(), nullable=False),
        Attribute('"size"', DecimalType(10, 0), nullable=False),
        Attribute('"status"', StringType(), nullable=False),
        Attribute('"encryption"', StringType(), nullable=False),
        Attribute('"message"', StringType(), nullable=False),
    ]


def analyze_attributes(
    sql: str, session: "clickzetta.zettapark.session.Session"
) -> List[Attribute]:
    lowercase = sql.strip().lower()

    # SQL commands which cannot be prepared
    # https://doc.clickzetta.com/
    if lowercase.startswith(
        ("alter", "drop", "use", "create", "grant", "revoke", "comment")
    ):
        return command_attributes()
    if lowercase.startswith(("ls", "list")):
        return list_volume_attributes()
    if lowercase.startswith(("rm", "remove")):
        return remove_state_file_attributes()
    if lowercase.startswith("put"):
        return put_attributes()
    if lowercase.startswith("get"):
        return get_attributes()
    if lowercase.startswith("show"):
        sql = "select * from (" + sql.strip("; ") + ") limit 0"
    if lowercase.startswith("describe"):
        session._run_query(sql)
        return convert_result_meta_to_attribute(session._conn._cursor.description)

    return session._get_result_attributes(sql)


def convert_result_meta_to_attribute(meta: List[ResultMetadata]) -> List[Attribute]:
    attributes = []
    for column_metadata in meta:
        quoted_name = quote_name(column_metadata.name)
        attributes.append(
            Attribute(
                quoted_name,
                convert_metadata_to_data_type(column_metadata),
                column_metadata.null_ok,
            )
        )
    return attributes


def run_new_describe(cursor: ClickzettaCursor, query: str) -> List[ResultMetadata]:
    """Execute describe() on a cursor, returning the new metadata format if possible.

    If an older connector is in use, this function falls back to the old metadata format.
    """
    if hasattr(cursor, "_describe_internal"):
        # Pyright does not perform narrowing here
        return cursor._describe_internal(query)
    else:
        query = f"select * from ({query}) limit 0"
        cursor.execute(query)
        return cursor.description
