#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# Copyright (c) 2023-2025 Yunqi Inc. All rights reserved.
#


import sys
from logging import getLogger
from typing import Any, Dict, Optional, Union

import clickzetta.zettapark
from clickzetta.zettapark._internal.analyzer.select_statement import (
    SelectQueryPlan,
    SelectStatement,
)
from clickzetta.zettapark._internal.telemetry import set_api_call_source
from clickzetta.zettapark._internal.utils import INFER_SCHEMA_FORMAT_TYPES
from clickzetta.zettapark.dataframe import DataFrame
from clickzetta.zettapark.table import Table
from clickzetta.zettapark.types import StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

logger = getLogger(__name__)


class DataFrameReader:
    """Provides methods to load data in various supported formats from a volume
    to a :class:`DataFrame`. The paths provided to the DataFrameReader must refer
    to volumes.

    To use this object:

    1. Access an instance of a :class:`DataFrameReader` by using the
    :attr:`Session.read` property.

    2. Specify any `format-specific options <https://doc.clickzetta.com/>`_ and `copy options <https://doc.clickzetta.com/>`_
    by calling the :func:`option` or :func:`options` method. These methods return a
    DataFrameReader that is configured with these options. (Note that although
    specifying copy options can make error handling more robust during the reading
    process, it may have an effect on performance.)

    3. Specify the schema of the data that you plan to load by constructing a
    :class:`types.StructType` object and passing it to the :func:`schema` method if the file format is CSV. Other file
    formats such as JSON, XML, Parquet, ORC, and AVRO don't accept a schema.
    This method returns a :class:`DataFrameReader` that is configured to read data that uses the specified schema.
    Currently, inferring schema is also supported for CSV and JSON formats as a preview feature open to all accounts.

    4. Specify the format of the data by calling the method named after the format
    (e.g. :func:`csv`, :func:`json`, etc.). These methods return a :class:`DataFrame`
    that is configured to load data in the specified format.

    5. Call a :class:`DataFrame` method that performs an action (e.g.
    :func:`DataFrame.collect`) to load the data from the file.

    The following examples demonstrate how to use a DataFrameReader.
            >>> # Create a table (which is associated with a volume) to run the example code.
            >>> _ = session.sql("CREATE TABLE my_table (a int)").collect()

    Example 1:
        Loading the first two columns of a CSV file and skipping the first header line:

            >>> from clickzetta.zettapark.types import StructType, StructField, IntegerType, StringType, FloatType
            >>> _ = session.file.put("tests/resources/testCSV.csv", "volume:table//my_table/", auto_compress=False)
            >>> # Define the schema for the data in the CSV file.
            >>> user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType()), StructField("c", FloatType())])
            >>> # Create a DataFrame that is configured to load data from the CSV file.
            >>> df = session.read.options({"field_delimiter": ",", "skip_header": 1}).schema(user_schema).csv("volume:table//my_table/testCSV.csv")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(A=2, B='two', C=2.2)]

    Example 2:
        Loading a gzip compressed json file:

            >>> _ = session.file.put("tests/resources/testJson.json", "volume:table//my_table/", auto_compress=True)
            >>> # Create a DataFrame that is configured to load data from the gzipped JSON file.
            >>> json_df = session.read.option("compression", "gzip").json("volume:table//my_table/testJson.json.gz")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> json_df.show()
            -----------------------
            |"$1"                 |
            -----------------------
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            -----------------------
            <BLANKLINE>


    In addition, if you want to load only a subset of files from the volume, you can use the
    `pattern <https://doc.clickzetta.com/>`_
    option to specify a regular expression that matches the files that you want to load.

    Example 3:
        Loading only the CSV files from a volume path:

            >>> from clickzetta.zettapark.types import StructType, StructField, IntegerType, StringType
            >>> from clickzetta.zettapark.functions import col
            >>> _ = session.file.put("tests/resources/*.csv", "volume:table//my_table/", auto_compress=False)
            >>> # Define the schema for the data in the CSV files.
            >>> user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType()), StructField("c", FloatType())])
            >>> # Create a DataFrame that is configured to load data from the CSV files in the volume.
            >>> csv_df = session.read.option("pattern", ".*V[.]csv").schema(user_schema).csv("volume:table//my_table/").sort(col("a"))
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> csv_df.collect()
            [Row(A=1, B='one', C=1.2), Row(A=2, B='two', C=2.2), Row(A=3, B='three', C=3.3), Row(A=4, B='four', C=4.4)]

    To load Parquet, ORC and AVRO files, no schema is accepted because the schema will be automatically inferred.
    Inferring the schema can be disabled by setting option "infer_schema" to ``False``. Then you can use ``$1`` to access
    the column data as an OBJECT.

    Example 4:
        Loading a Parquet file with inferring the schema.
            >>> from clickzetta.zettapark.functions import col
            >>> _ = session.file.put("tests/resources/test.parquet", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.parquet("volume:table//my_table/test.parquet").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 5:
        Loading an ORC file and infer the schema:
            >>> from clickzetta.zettapark.functions import col
            >>> _ = session.file.put("tests/resources/test.orc", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.orc("volume:table//my_table/test.orc").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 6:
        Loading an AVRO file and infer the schema:
            >>> from clickzetta.zettapark.functions import col
            >>> _ = session.file.put("tests/resources/test.avro", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.avro("volume:table//my_table/test.avro").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 7:
        Loading a Parquet file without inferring the schema:
            >>> from clickzetta.zettapark.functions import col
            >>> _ = session.file.put("tests/resources/test.parquet", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.option("infer_schema", False).parquet("volume:table//my_table/test.parquet").where(col('$1')["num"] == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            -------------------
            |"$1"             |
            -------------------
            |{                |
            |  "num": 2,      |
            |  "str": "str2"  |
            |}                |
            -------------------
            <BLANKLINE>

    Loading JSON and XML files doesn't support schema either. You also need to use ``$1`` to access the column data as an OBJECT.

    Example 8:
        Loading a JSON file:
            >>> from clickzetta.zettapark.functions import col, lit
            >>> _ = session.file.put("tests/resources/testJson.json", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.json("volume:table//my_table/testJson.json").where(col("$1")["fruit"] == lit("Apple"))
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            -----------------------
            |"$1"                 |
            -----------------------
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            -----------------------
            <BLANKLINE>

    Example 9:
        Loading an XML file:
            >>> _ = session.file.put("tests/resources/test.xml", "volume:table//my_table/", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a volume.
            >>> df = session.read.xml("volume:table//my_table/test.xml")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            ---------------------
            |"$1"               |
            ---------------------
            |<test>             |
            |  <num>1</num>     |
            |  <str>str1</str>  |
            |</test>            |
            |<test>             |
            |  <num>2</num>     |
            |  <str>str2</str>  |
            |</test>            |
            ---------------------
            <BLANKLINE>

    Example 10:
        Loading a CSV file with an already existing FILE_FORMAT:
            >>> from clickzetta.zettapark.types import StructType, StructField, IntegerType, StringType
            >>> _ = session.sql("create file format if not exists csv_format type=csv skip_header=1 null_if='none';").collect()
            >>> _ = session.file.put("tests/resources/testCSVspecialFormat.csv", "volume:table//my_table/", auto_compress=False)
            >>> # Define the schema for the data in the CSV files.
            >>> schema = StructType([StructField("ID", IntegerType()),StructField("USERNAME", StringType()),StructField("FIRSTNAME", StringType()),StructField("LASTNAME", StringType())])
            >>> # Create a DataFrame that is configured to load data from the CSV files in the volume.
            >>> df = session.read.schema(schema).option("format_name", "csv_format").csv("volume:table//my_table/testCSVspecialFormat.csv")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(ID=0, USERNAME='admin', FIRSTNAME=None, LASTNAME=None), Row(ID=1, USERNAME='test_user', FIRSTNAME='test', LASTNAME='user')]

    Example 11:
        Inferring schema for csv and json files (Preview Feature - Open):
            >>> # Read a csv file without a header
            >>> df = session.read.option("INFER_SCHEMA", True).csv("volume:table//my_table/testCSV.csv")
            >>> df.show()
            ----------------------
            |"c1"  |"c2"  |"c3"  |
            ----------------------
            |1     |one   |1.2   |
            |2     |two   |2.2   |
            ----------------------
            <BLANKLINE>

            >>> # Read a csv file with header and parse the header
            >>> _ = session.file.put("tests/resources/testCSVheader.csv", "volume:table//my_table/", auto_compress=False)
            >>> df = session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv("volume:table//my_table/testCSVheader.csv")
            >>> df.show()
            ----------------------------
            |"id"  |"name"  |"rating"  |
            ----------------------------
            |1     |one     |1.2       |
            |2     |two     |2.2       |
            ----------------------------
            <BLANKLINE>

            >>> df = session.read.option("INFER_SCHEMA", True).json("volume:table//my_table/testJson.json")
            >>> df.show()
            ------------------------------
            |"color"  |"fruit"  |"size"  |
            ------------------------------
            |Red      |Apple    |Large   |
            |Red      |Apple    |Large   |
            ------------------------------
            <BLANKLINE>
    """

    def __init__(self, session: "clickzetta.zettapark.session.Session") -> None:
        self._session = session
        self._user_schema: Optional[StructType] = None
        self._cur_options: dict[str, Any] = {}
        self._file_path: Optional[str] = None
        self._file_type: Optional[str] = None

    @property
    def _infer_schema(self):
        # let _cur_options to be the source of truth
        if self._file_type in INFER_SCHEMA_FORMAT_TYPES:
            return self._cur_options.get("INFER_SCHEMA", True)
        return False

    def table(self, name: Union[str, Iterable[str]]) -> Table:
        """Returns a Table that points to the specified table.

        This method is an alias of :meth:`~clickzetta.zettapark.session.Session.table`.

        Args:
            name: Name of the table to use.
        """
        return self._session.table(name)

    def schema(self, schema: StructType) -> "DataFrameReader":
        """Define the schema for CSV files that you want to read.

        Args:
            schema: Schema configuration for the CSV file to be read.

        Returns:
            a :class:`DataFrameReader` instance with the specified schema configuration for the data to be read.
        """
        self._user_schema = schema
        return self

    def csv(self, path: str) -> DataFrame:
        """Specify the path of the CSV file(s) to load.

        Args:
            path: The volume path of a CSV file, or a volume path that has CSV files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified CSV file(s) in a ClickZetta volume.
        """
        self._file_path = path
        self._file_type = "CSV"

        schema = self._user_schema._to_attributes() if self._user_schema else None

        if self._session.sql_simplifier_enabled:
            df = DataFrame(
                self._session,
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_query_plan(
                        self._session._analyzer.plan_builder.read_file(
                            path,
                            self._file_type,
                            self._cur_options,
                            schema,
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
            )
        else:
            df = DataFrame(
                self._session,
                self._session._plan_builder.read_file(
                    path,
                    self._file_type,
                    self._cur_options,
                    schema,
                ),
            )
        df._reader = self
        set_api_call_source(df, "DataFrameReader.csv")
        return df

    def json(self, path: str) -> DataFrame:
        """Specify the path of the JSON file(s) to load.

        Args:
            path: The volume path of a JSON file, or a volume path that has JSON files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified JSON file(s) in a volume.
        """
        return self._read_semi_structured_file(path, "JSON")

    def avro(self, path: str) -> DataFrame:
        """Specify the path of the AVRO file(s) to load.

        Args:
            path: The volume path of an AVRO file, or a volume path that has AVRO files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from AVRO to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified AVRO file(s) in a volume.
        """
        return self._read_semi_structured_file(path, "AVRO")

    def parquet(self, path: str) -> DataFrame:
        """Specify the path of the PARQUET file(s) to load.

        Args:
            path: The volume path of a PARQUET file, or a volume path that has PARQUET files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from PARQUET to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified PARQUET file(s) in a volume.
        """
        return self._read_semi_structured_file(path, "PARQUET")

    def orc(self, path: str) -> DataFrame:
        """Specify the path of the ORC file(s) to load.

        Args:
            path: The volume path of a ORC file, or a volume path that has ORC files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from ORC to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified ORC file(s) in a volume.
        """
        return self._read_semi_structured_file(path, "ORC")

    def xml(self, path: str) -> DataFrame:
        """Specify the path of the XML file(s) to load.

        Args:
            path: The volume path of an XML file, or a volume path that has XML files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified XML file(s) in a volume.
        """
        return self._read_semi_structured_file(path, "XML")

    def option(self, key: str, value: Any) -> "DataFrameReader":
        """Sets the specified option in the DataFrameReader.

        Use this method to configure any
        `format-specific options <https://doc.clickzetta.com/>`_
        and
        `copy options <https://doc.clickzetta.com/>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        Args:
            key: Name of the option (e.g. ``compression``, ``skip_header``, etc.).
            value: Value of the option.
        """
        self._cur_options[key.lower()] = value
        return self

    def options(self, configs: Dict) -> "DataFrameReader":
        """Sets multiple specified options in the DataFrameReader.

        This method is the same as the :meth:`option` except that you can set multiple options in one call.

        Args:
            configs: Dictionary of the names of options (e.g. ``compression``,
                ``skip_header``, etc.) and their corresponding values.
        """
        for k, v in configs.items():
            self.option(k, v)
        return self

    def _read_semi_structured_file(self, path: str, format: str) -> DataFrame:
        self._file_path = path
        self._file_type = format

        schema = self._user_schema._to_attributes() if self._user_schema else None

        if self._session.sql_simplifier_enabled:
            df = DataFrame(
                self._session,
                SelectStatement(
                    from_=SelectQueryPlan(
                        self._session._plan_builder.read_file(
                            path,
                            format,
                            self._cur_options,
                            schema,
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
            )
        else:
            df = DataFrame(
                self._session,
                self._session._plan_builder.read_file(
                    path,
                    format,
                    self._cur_options,
                    schema,
                ),
            )
        df._reader = self
        set_api_call_source(df, f"DataFrameReader.{format.lower()}")
        return df
