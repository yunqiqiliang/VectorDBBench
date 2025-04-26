import typing
import re
from itertools import count
import datetime
from logging import getLogger
from typing import Optional

from clickzetta.connector.v0 import PY39
from dateutil import tz

if PY39:
    from zoneinfo import ZoneInfo

_log = getLogger(__name__)

def split_sql(query: str) -> typing.List[str]:
    ret = []
    c = None  # current char
    p = None  # previous char
    b = 0  # begin of current sql
    state = 1  # current state
    NORMAL = 1
    IDENTIFIER = 2
    SINGLE_QUOTATION = 3
    DOUBLE_QUOTATION = 4
    SINGLE_LINE_COMMENT = 5
    MULTI_LINE_COMMENT = 6
    for i in range(0, len(query)):
        c = query[i]
        if state == NORMAL:
            if c == ";":
                if i - b > 0:
                    ret.append(query[b:i])
                b = i + 1
                p = None
            elif p == "-" and c == "-":
                state = SINGLE_LINE_COMMENT
                p = None
            elif p == "/" and c == "*":
                state = MULTI_LINE_COMMENT
                p = None
            elif c == "`":
                state = IDENTIFIER
                p = None
            elif c == "'":
                state = SINGLE_QUOTATION
                p = None
            elif c == '"':
                state = DOUBLE_QUOTATION
                p = None
            else:
                p = c
        elif state == IDENTIFIER:
            if c == "`" and p != "\\":
                state = NORMAL
                p = None
            else:
                p = c
        elif state == SINGLE_QUOTATION:
            if c == "'" and p != "\\":
                state = NORMAL
                p = None
            elif p == "\\":
                p = None
            else:
                p = c
        elif state == DOUBLE_QUOTATION:
            if c == '"' and p != "\\":
                state = NORMAL
                p = None
            elif p == "\\":
                p = None
            else:
                p = c
        elif state == SINGLE_LINE_COMMENT:
            if c == "\n":
                state = NORMAL
                p = None
            else:
                p = c
        elif state == MULTI_LINE_COMMENT:
            if p == "*" and c == "/":
                state = NORMAL
                p = None
            else:
                p = c

    if b < len(query):
        ret.append(query[b:])
    return ret


# def myassert(sql, num, sqls):
#     if len(sqls) == num:
#         print('-- OK --')
#     else:
#         print('-- FAIL --')
#     print(sql)
#     for s in sqls:
#         print(f'[{s}]')

# if __name__ == "__main__":
#     sql = "select 1";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select 1;";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select 1;select 2";
#     sqls = split_sql(sql);
#     myassert(sql, 2, sqls);

#     sql = "select 1;select 2;";
#     sqls = split_sql(sql);
#     myassert(sql, 2, sqls);

#     sql = "select 1\n\n\nfrom table;";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = ";";
#     sqls = split_sql(sql);
#     myassert(sql, 0, sqls);

#     sql = ";;";
#     sqls = split_sql(sql);
#     myassert(sql, 0, sqls);

#     sql = "; ;";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = ";\n;";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "";
#     sqls = split_sql(sql);
#     myassert(sql, 0, sqls);

#     sql = "\n";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select *\n-- -- ;\nfrom world\n";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select *\n-- -- ;\nfrom world\n";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select `aaaa";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select 'aaa;\nbbb'\n";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "select \"--\\\"/*;\n*/\"";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "-- line 1\nselect\n/* comment -- -- ;\n****/*\nfrom foo";
#     sqls = split_sql(sql);
#     myassert(sql, 1, sqls);

#     sql = "/*/--/*/;\nselect /* -- 1; */\n1;-- sql 2";
#     sqls = split_sql(sql);
#     myassert(sql, 3, sqls);

#     sql = """select "1\\\\";select2
# """
#     sqls = split_sql(sql);
#     myassert(sql, 2, sqls);

class BaseExecuteContext(object):
    def __init__(self):
        self.result = None



def normalize_file_path(file_url):
    """
    Normalize a file path by removing the file:/, file://, or file:/// protocol if present.
    Preserves the leading slash and returns the result as a raw string.

    Args:
    file_url (str): The file path or URL to normalize.

    Returns:
    str: The normalized local file path as a raw string.
    """
    pattern = r'^file:/{0,3}'

    if re.match(pattern, file_url):
        path = re.sub(pattern, '', file_url)
        # If the path is a Windows path, do not add a leading slash
        if not re.match(r'^[a-zA-Z]:', path):
            path = '/' + path.lstrip('/')
        normalized_path = path
    else:
        normalized_path = file_url

    return fr"{normalized_path}"


def strip_leading_comment(input_str):
    ret = input_str.strip()
    while ret.startswith("--") or ret.startswith("/*"):
        if ret.startswith("--"):
            index = ret.find("\n")
            if index == -1:
                return ""
            else:
                ret = ret[index + 1:].strip()
        else:
            index = ret.find("*/")
            if index == -1:
                return ""
            else:
                ret = ret[index + 2:].strip()
    return ret

def _handle(tried, **kwargs):
    return None


def _checker(context: BaseExecuteContext, tried, exception=None, **kwargs):
    return True


def _on_failure(result, exception, **kwargs):
    raise Exception(f"Failed to execute! Result: {result}, {exception or ''}")


def execute_with_retrying(handle=_handle, checker=_checker, on_failure=_on_failure, max_tries=1, **kwargs):
    e = None
    counter = count() if max_tries is None or max_tries == -1 else range(1, max_tries + 1)
    for tried in counter:
        context = BaseExecuteContext()
        try:
            context.result = handle(tried=tried, **kwargs)
        except Exception as exc:
            e = exc
            _log.warning("Failed to execute: %s", exc)

        # The checker may also throw an exception to terminate the retry
        if checker(context=context, tried=tried, exception=e, **kwargs):
            return context.result

        # If it's the last try, it means we've exhausted all attempts
        if tried == max_tries:
            on_failure(context.result, e, **kwargs)
            return e or context.result
    return None


def as_timezone(dt: datetime.datetime, timezone_str: Optional[str],
                is_timestamp_ntz: bool = False) -> datetime.datetime:
    if PY39:
        if is_timestamp_ntz:
            return dt.replace(tzinfo=ZoneInfo("UTC"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        if timezone_str:
            timezone = ZoneInfo(timezone_str)
        else:
            # System default timezone
            timezone = None
    else:
        if is_timestamp_ntz:
            return dt.replace(tzinfo=tz.tzutc())
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=tz.tzutc())
        if timezone_str:
            timezone = tz.gettz(timezone_str)
        else:
            timezone = tz.tzlocal()

    return dt.astimezone(timezone)
