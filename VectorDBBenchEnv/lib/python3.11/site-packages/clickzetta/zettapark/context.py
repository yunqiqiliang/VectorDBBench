#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

"""Context module for Zettapark."""

import clickzetta.zettapark


def get_active_session() -> "clickzetta.zettapark.Session":
    """Returns the current active Zettapark session.

    Raises: ZettaparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return clickzetta.zettapark.session._get_active_session()
