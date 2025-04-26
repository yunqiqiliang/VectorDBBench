#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

"""
Contains core classes of Zettapark.
"""

# types, udf, functions, exceptions still use its own modules

__all__ = [
    "Column",
    "CaseExpr",
    "Row",
    "Session",
    "FileOperation",
    "PutResult",
    "GetResult",
    "DataFrame",
    "DataFrameStatFunctions",
    "DataFrameAnalyticsFunctions",
    "DataFrameNaFunctions",
    "DataFrameWriter",
    "DataFrameReader",
    "GroupingSets",
    "RelationalGroupedDataFrame",
    "Window",
    "WindowSpec",
    "Table",
    "UpdateResult",
    "DeleteResult",
    "MergeResult",
    "WhenMatchedClause",
    "WhenNotMatchedClause",
    "QueryRecord",
    "QueryHistory",
]

from pathlib import Path

__version__ = Path(__file__).parent.joinpath("version").read_text().strip()


from clickzetta.zettapark.column import CaseExpr, Column
from clickzetta.zettapark.dataframe import DataFrame
from clickzetta.zettapark.dataframe_analytics_functions import (
    DataFrameAnalyticsFunctions,
)
from clickzetta.zettapark.dataframe_na_functions import DataFrameNaFunctions
from clickzetta.zettapark.dataframe_reader import DataFrameReader
from clickzetta.zettapark.dataframe_stat_functions import DataFrameStatFunctions
from clickzetta.zettapark.dataframe_writer import DataFrameWriter
from clickzetta.zettapark.file_operation import FileOperation, GetResult, PutResult
from clickzetta.zettapark.query_history import QueryHistory, QueryRecord
from clickzetta.zettapark.relational_grouped_dataframe import (
    GroupingSets,
    RelationalGroupedDataFrame,
)
from clickzetta.zettapark.row import Row
from clickzetta.zettapark.session import Session
from clickzetta.zettapark.table import (
    DeleteResult,
    MergeResult,
    Table,
    UpdateResult,
    WhenMatchedClause,
    WhenNotMatchedClause,
)
from clickzetta.zettapark.window import Window, WindowSpec
