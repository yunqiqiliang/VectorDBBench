import sqlalchemy.types
import sqlalchemy.util
from sqlalchemy.dialects.mysql import TINYINT

from clickzetta.connector.sqlalchemy.datatype import VECTOR, BITMAP, STRUCT, MAP

_type_map = {
    # === Integer ===
    "INT8": TINYINT,
    "INT16": sqlalchemy.types.SMALLINT,
    "INT32": sqlalchemy.types.Integer,
    "INT64": sqlalchemy.types.BIGINT,
    # === Boolean ===
    "BOOL": sqlalchemy.types.Boolean,
    "BOOLEAN": sqlalchemy.types.Boolean,
    # == binary ==
    "BINARY": sqlalchemy.types.BINARY,
    # === Date and time ===
    "DATE": sqlalchemy.types.DATE,
    "TIMESTAMP_LTZ": sqlalchemy.types.TIMESTAMP,
    "TIMESTAMP_NTZ": sqlalchemy.types.TIMESTAMP,
    "INTERVAL": sqlalchemy.types.Interval,
    # === Floating-point ===
    "FLOAT32": sqlalchemy.types.Float,
    "FLOAT64": sqlalchemy.dialects.mysql.DOUBLE,
    # === Fixed-precision ===
    "DECIMAL": sqlalchemy.types.DECIMAL,
    # === String ===
    "STRING": sqlalchemy.types.String,
    "VARCHAR": sqlalchemy.types.VARCHAR,
    "CHAR": sqlalchemy.types.CHAR,
    "VOID": sqlalchemy.types.NullType,
    "VECTOR": VECTOR,
    "JSON": sqlalchemy.types.JSON,
    # === Structural ===
    "ARRAY": sqlalchemy.types.ARRAY,
    "MAP": MAP,
    "STRUCT": STRUCT,
    "BITMAP": BITMAP,
}

# By convention, dialect-provided types are spelled with all upper case.
INT8 = _type_map["INT8"]
INT16 = _type_map["INT16"]
INT32 = _type_map["INT32"]
INT64 = _type_map["INT64"]
BOOL = _type_map["BOOL"]
BINARY = _type_map["BINARY"]
DATE = _type_map["DATE"]
TIMESTAMP = _type_map["TIMESTAMP_LTZ"]
TIMESTAMP_NTZ = _type_map["TIMESTAMP_NTZ"]
INTERVAL = _type_map["INTERVAL"]
FLOAT32 = _type_map["FLOAT32"]
FLOAT64 = _type_map["FLOAT64"]
DECIMAL = _type_map["DECIMAL"]
STRING = _type_map["STRING"]
VARCHAR = _type_map["VARCHAR"]
CHAR = _type_map["CHAR"]
JSON = _type_map["JSON"]
ARRAY = _type_map["ARRAY"]


def get_clickzetta_column_type(field):
    try:
        coltype = _type_map[field.field_type]
    except KeyError:
        sqlalchemy.util.warn(
            "Did not recognize type '%s' of column '%s'"
            % (field.field_type, field.name)
        )
        coltype = sqlalchemy.types.NullType
    else:
        if field.field_type == "DECIMAL":
            coltype = coltype(precision=field.precision, scale=field.scale)
        elif field.field_type == "VARCHAR" or field.field_type == "CHAR":
            coltype = coltype(field.length)
        else:
            coltype = coltype()

    return coltype
