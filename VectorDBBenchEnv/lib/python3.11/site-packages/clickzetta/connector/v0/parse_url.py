import re
from sqlalchemy.engine.url import make_url

GROUP_DELIMITER = re.compile(r"\s*\,\s*")
KEY_VALUE_DELIMITER = re.compile(r"\s*\:\s*")
HTTP_PROTOCOL_DEFAULT_PORT = '80'
HTTP_PROTOCOL_PREFIX = 'http://'
HTTPS_PROTOCOL_DEFAULT_PORT = '443'
HTTPS_PROTOCOL_PREFIX = 'https://'


def parse_boolean(bool_string):
    bool_string = bool_string.lower()
    if bool_string == "true":
        return True
    elif bool_string == "false":
        return False
    else:
        raise ValueError()


def parse_url(origin_url):
    url = make_url(origin_url)
    query = dict(url.query)
    port = url.port

    instance = url.host.split('.')[0]
    length = len(instance) + 1
    protocol = None
    service = None
    token_expire_time_ms = None
    vcluster = None
    host = url.host[length:]

    if "protocol" in query:
        protocol = query.pop("protocol")
        if protocol == "http" or protocol == "https":
            if not port:
                service = (
                        HTTP_PROTOCOL_PREFIX
                        + host
                        + ":"
                        + HTTP_PROTOCOL_DEFAULT_PORT
                )
            else:
                service = (
                        HTTP_PROTOCOL_PREFIX + host + ":" + str(port)
                )
        else:
            raise ValueError(
                "protocol parameter must be http. Other protocols are not supported yet."
            )
    else:
        protocol = "https"
        if not port:
            service = (
                    HTTPS_PROTOCOL_PREFIX
                    + host
                    + ":"
                    + HTTPS_PROTOCOL_DEFAULT_PORT
            )
        else:
            service = (
                    HTTPS_PROTOCOL_PREFIX + host + ":" + str(port)
            )

    workspace = url.database
    username = url.username
    driver_name = url.drivername
    password = url.password
    schema = None
    magic_token = None

    if (
            "virtualcluster" in query
            or "virtualCluster" in query
            or "vcluster" in query
    ):
        if "virtualcluster" in query:
            vcluster = query.pop("virtualcluster")
        elif "virtualCluster" in query:
            vcluster = query.pop("virtualCluster")
        else:
            vcluster = query.pop("vcluster")
    else:
        raise ValueError(
            "url must have `virtualcluster` or `virtualCluster` or `vcluster` parameter."
        )
    if "schema" in query:
        schema = query.pop("schema")
    if "magic_token" in query:
        magic_token = query.pop("magic_token")
    if "token_expire_time_ms" in query:
        token_expire_time_ms = int(query.pop("token_expire_time_ms"))

    return (
        service,
        username,
        driver_name,
        password,
        instance,
        workspace,
        vcluster,
        schema,
        magic_token,
        protocol,
        host,
        token_expire_time_ms,
        query,
    )


def generate_url(client):
    return (
            f"clickzetta://{client.username}:{client.password}@"
            + f"{client.instance}.{client.host}/{client.workspace}?virtualcluster={client.vcluster or 'default'}"
            + ("" if client.schema is None else f"&schema={client.schema}")
            + ("" if client.magic_token is None else f"&magic_token={client.magic_token}")
            + ("" if client.token_expire_time_ms is None else f"&token_expire_time_ms={client.token_expire_time_ms}")
            + ("" if client.protocol is None else f"&protocol={client.protocol}")
            + "".join(
        f"&{key}={value}"
        for key, value in client.extra.items()
    )
    )
