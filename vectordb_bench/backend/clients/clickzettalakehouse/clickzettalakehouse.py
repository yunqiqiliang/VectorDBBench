import concurrent.futures
import io
import logging
import time
from contextlib import contextmanager
from typing import Any
import pandas as pd
import multiprocessing

from clickzetta.zettapark.session import Session
from clickzetta.zettapark.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, VectorType
from clickzetta.connector.sqlalchemy.datatype import VECTOR, BIGINT

from clickzetta import connect

from ..api import VectorDB
from .config import ClickZettaLakehouseIndexConfig,default_hints

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

logging.getLogger("clickzetta").setLevel(logging.DEBUG)

CLICKZETTA_LAKEHOUSE_MAX_SIZE_PER_BATCH = 1000 * 1024 * 1024  # 1000MB
CLICKZETTA_LAKEHOUSE_MAX_NUM_PER_BATCH = 10000

EXECUTE_SEARCH_EMBEDDING_WITH_SESSION = False

class ClickZettaLakehouse(VectorDB):
    def _init_database(self):
        """Initialize the database connection."""
        with self.get_session() as session:
            log.info("Database connection initialized.")

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: ClickZettaLakehouseIndexConfig,
        collection_name: str = "vector_bench_test",
        drop_old: bool = False,
        auth_expiration_time: int = 3600,
        **kwargs,
    ):
        self.name = "ClickZettaLakehouse"
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name
        self.dim = dim
        self.AUTH_EXPIRATION_TIME = auth_expiration_time
        self.batch_size = int(
            min(
                CLICKZETTA_LAKEHOUSE_MAX_SIZE_PER_BATCH / (dim * 25),
                CLICKZETTA_LAKEHOUSE_MAX_NUM_PER_BATCH,
            ),
        )
        self.search_fn = db_case_config.search_param()["metric_fn"]
        self.auth_time = None
        # 关键：hints为None时赋值为default_hints
        if not self.db_config.get("hints"):
            self.db_config["hints"] = dict(default_hints)
        # log.info(f"self.db_config : {self.db_config }")
        # 仅主进程复用 session，子进程不复用
        self.session = None
        if multiprocessing.current_process().name == "MainProcess":
            try:
                self.session = Session.builder.configs(self.db_config).create()
                self.session.sql(f"select 'Initialize connection to the Clickzetta by VectorBenchmark Tool';")
                self.auth_time = time.time()
            except Exception as e:
                raise ValueError(f"Failed to connect to Clickzetta workspace/database: {e}")
            
        if drop_old:
            self._drop_table()
            self._create_table()
    @contextmanager
    def _get_connection(self):
        """
        用 self.db_config 自动建立连接。
        创建cursor，用来执行select query语句，提高性能。
        """
        conn = connect(
            username=self.db_config.get("username"),
            password=self.db_config.get("password"),
            service=self.db_config.get("service", "api.clickzetta.com"),
            instance=self.db_config.get("instance"),
            workspace=self.db_config.get("workspace"),
            schema=self.db_config.get("schema", "public"),
            vcluster=self.db_config.get("vcluster", "default"),
            hints=self.db_config.get("hints", {}),
        )
        cursor = conn.cursor()
        yield conn, cursor

    @contextmanager
    def init(self):
        with self._get_connection() as (conn, cursor):
            self.conn = conn
            self.cursor = cursor
            try:
                yield
            finally:
                self.conn = None
                self.cursor = None
        # """兼容基类抽象方法，实际调用 get_session 实现"""
        # with self.get_session() as session:
        #     yield session
        

    @contextmanager
    def get_session(self):
        """主进程复用 session，子进程自动新建 session，避免 pickle 问题"""
        if multiprocessing.current_process().name == "MainProcess" and self.session:
            yield self.session
        else:
            session = None
            try:
                session = Session.builder.configs(self.db_config).create()
                session.sql(f"select 'Initialize connection to the Clickzetta by VectorBenchmark Tool';")
                yield session
            finally:
                if session:
                    session.close()

    def close(self):
        """关闭 session，建议在不再使用时手动调用"""
        if self.session:
            self.session.close()
            self.session = None

    @contextmanager
    def execute_query(self, query: str):
        """Execute a SQL query and return results as a list of dictionaries"""
        with self.get_session() as session:
            log.info(f"Executing query: {query}")
            try:
                result = session.sql(query).to_pandas()
                result_rows = result.to_dict(orient="records")
                yield result_rows  # 使用 yield 返回结果
            except Exception as e:
                log.error(f'Database error executing "{query}": {e}')
                raise
            finally:
                log.info("Query execution completed.")

    def _drop_table(self):
        try:
            with self.execute_query(f"DROP TABLE IF EXISTS {self.table_name}") as _:
                log.info(f"Table {self.table_name} dropped successfully.")
        except Exception as e:
            log.warning("Failed to drop table: %s error: %s", self.table_name, e)
            raise

    def _create_table(self):
        try:
            index_param = self.case_config.index_param()
            query = f"""
            CREATE TABLE {self.table_name} (
                id BIGINT,
                embedding VECTOR({self.dim}) NOT NULL,
                INDEX idx_vector_on_{self.table_name}_embedding (embedding) USING VECTOR PROPERTIES (
                    "scalar.type" = "f32",
                    "distance.function" = "{index_param["metric_fn"]}"
                )
            );
            """
            with self.execute_query(query) as _:
                log.info(f"Table {self.table_name} created successfully.")
        except Exception as e:
            log.warning("Failed to create table: %s error: %s", self.table_name, e)
            raise

    def ready_to_load(self) -> bool:
        pass

    def optimize(self, data_size: int | None = None) -> None:
        pass
        log.info("Passed:Index build finished successfully.")

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> tuple[int, Exception]:
        """
        Insert embeddings and metadata into the database.

        Args:
            embeddings (list[list[float]]): The list of embedding vectors.
            metadata (list[int]): The list of metadata IDs corresponding to the embeddings.

        Returns:
            tuple[int, Exception]: Number of rows inserted and an exception if any.
        """
        if not embeddings or not metadata:
            log.error("Embeddings or metadata is empty.")
            return 0, ValueError("Embeddings or metadata cannot be empty.")
        if len(embeddings) != len(metadata):
            log.error("Embeddings and metadata length mismatch.")
            return 0, ValueError("Embeddings and metadata must have the same length.")

        df_schema = [
            ("id", BIGINT),
            ("embeddings", VECTOR("FLOAT", self.dim))
        ]

        total_inserted = 0
        batch_size = self.batch_size
        log.info(f"Executing insert batch_size: {batch_size}")
        try:
            with self.get_session() as session:
                for i in range(0, len(metadata), batch_size):
                    batch_embeddings = embeddings[i:i+batch_size]
                    batch_metadata = metadata[i:i+batch_size]
                    df_loaded = pd.DataFrame({
                        "id": batch_metadata,
                        "embeddings": batch_embeddings,
                    })
                    zetta_df = session.create_dataframe(df_loaded, schema=df_schema)
                    zetta_df.write.mode("append").save_as_table(self.table_name)
                    total_inserted += len(batch_metadata)
                log.info(f"Successfully inserted {total_inserted} rows into table {self.table_name}.")
                return total_inserted, None
        except Exception as save_error:
            log.error(f"Error loading data to table {self.table_name}: {save_error}")
            return total_inserted, save_error

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> list[int]:
        """
        Search for the top-k nearest neighbors of the given query embedding.
        """
        try:
            query_str = ",".join(map(str, query))
            sql_query = f"""
            SELECT id FROM {self.table_name}
            ORDER BY {self.search_fn}(embedding, ARRAY[{query_str}]) LIMIT {k};
            """
            if EXECUTE_SEARCH_EMBEDDING_WITH_SESSION:
                with self.execute_query(sql_query) as result:
                    log.info(f"Search query executed successfully for table {self.table_name}.")
            else:
                with self._get_connection() as (conn, cursor):
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
            return [int(i[0]) for i in result]
        except Exception as e:
            log.error(f"Error executing search query on table {self.table_name}: {e}")
            raise

