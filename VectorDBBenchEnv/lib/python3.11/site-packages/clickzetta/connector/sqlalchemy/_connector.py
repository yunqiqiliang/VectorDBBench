_CONNECTOR_TYPE = "v0"
try:
    import clickzetta.connector.v0
except ImportError:
    _CONNECTOR_TYPE = "legacy"

if _CONNECTOR_TYPE == "v0":
    from clickzetta.connector.v0 import dbapi
    from clickzetta.connector.v0.client import Client
    # from clickzetta.connector.v0.enums import LoginParams
else:
    from clickzetta import dbapi
    from clickzetta.client import Client
    # from clickzetta.enums import LoginParams
