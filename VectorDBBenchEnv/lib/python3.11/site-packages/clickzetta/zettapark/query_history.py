#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

from typing import List, NamedTuple

import clickzetta.zettapark


class QueryRecord(NamedTuple):
    """Contains the query information returned from the ClickZetta database after the query is run."""

    query_id: str
    sql_text: str


class QueryHistory:
    """A context manager that listens to and records SQL queries that are pushed down to the ClickZetta database.

    See also:
        :meth:`clickzetta.zettapark.Session.query_history`.
    """

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self.session = session
        self._queries: List[QueryRecord] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _add_query(self, query_record: QueryRecord):
        self._queries.append(query_record)

    @property
    def queries(self) -> List[QueryRecord]:
        return self._queries
