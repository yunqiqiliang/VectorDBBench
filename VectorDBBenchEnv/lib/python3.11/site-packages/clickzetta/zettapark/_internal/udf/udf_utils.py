#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#

import re
import sys

_IS_PY_310 = sys.version_info.major == 3 and sys.version_info.minor == 10

_NON_IDENTIFIER_CHARS = re.compile(r"[^a-z0-9_]", re.IGNORECASE)


def check_runtime_version():
    if not _IS_PY_310:
        raise RuntimeError("Python 3.10 is required for udf support")
