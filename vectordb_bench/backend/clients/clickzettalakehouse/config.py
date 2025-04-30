from ..api import DBCaseConfig, DBConfig,IndexType, MetricType

from typing import Dict, Any, Literal
from pydantic import BaseModel, SecretStr, Field, root_validator
from datetime import datetime
from typing import Sequence, Mapping, Any, Literal, TypedDict

default_hints = {
        "sdk.job.timeout": 600,
        "query_tag": "VectorBench",
        "cz.storage.parquet.vector.index.read.memory.cache": True,
        "cz.storage.parquet.vector.index.read.local.cache": False,
        "cz.sql.table.scan.push.down.filter": True,
        "cz.sql.table.scan.enable.ensure.filter": True,
        "cz.storage.always.prefetch.internal": True,
        "cz.optimizer.generate.columns.always.valid": True,
        "cz.sql.index.prewhere.enabled": True,
        "cz.storage.parquet.enable.io.prefetch": False,
    }
def default_table_name():
    return f"vector_bench_test_{datetime.now().strftime('%Y%m%d%H%M%S')}"

class ClickZettaLakehouseConfig(DBConfig):
    user_name: str = ""
    password: SecretStr
    service: str = "ap-beijing-tencentcloud.api.clickzetta.com"
    instance: str = ""
    workspace: str = "quick_start"
    schema_: str = Field(default="vector_benchmark", alias="schema")
    vcluster: str = "VECTOR_BENCH"
    table_name: str = "vector_bench_test_"
    sdk_job_timeout: int = 600
    hints: Dict[str, Any] = Field(default_factory=lambda: default_hints.copy())


    def to_dict(self) -> dict:
        print(f"type(self.hints) is : {type(self.hints)}, self.hints is : {self.hints}")
        pwd_str = self.password.get_secret_value()
        return {
            "username": self.user_name,
            "password": pwd_str,
            "service": self.service,
            "instance": self.instance,
            "workspace": self.workspace,
            "schema": self.schema_,
            "vcluster": self.vcluster,
            "sdk_job_timeout": self.sdk_job_timeout,
            "table_name": self.table_name,
            "hints": self.hints,
        }




class ClickZettaLakehouseIndexParam(TypedDict):
    index_type: str
    distance_function: str
    scalar_type: str
    m: int
    ef_construction: int
    reuse_vector_column: bool
    compress_codec: str
    compress_level: str
    compress_byte_stream_split: bool
    compress_block_size: int
    conversion_rule: str

class ClickZettaLakehouseSearchParam(TypedDict):
    distance_function: str

class ClickZettaLakehouseIndexConfig(BaseModel, DBCaseConfig):
    create_index_before_load: bool = True
    create_index_after_load: bool = False
    index_type: Literal["HNSW"] = "HNSW"
    distance_function: Literal["l2_distance", "cosine_distance", "jaccard_distance", "hamming_distance"] = "cosine_distance"
    scalar_type: Literal["f32", "f16", "i8", "b1"] = "f32"
    m: int = 16
    ef_construction: int = 128
    reuse_vector_column: bool = False
    compress_codec: Literal["uncompressed", "zstd", "lz4"] = "uncompressed"
    compress_level: Literal["fastest", "default", "best"] = "default"
    compress_byte_stream_split: bool = True
    compress_block_size: int = 16777216
    conversion_rule: Literal["default", "as_bits"] = "default"
    metric_type: MetricType | None = None

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "l2_distance"
        if self.metric_type == MetricType.COSINE:
            return "cosine_distance"
        if self.metric_type == MetricType.JACCARD:
            return "jaccard_distance"
        if self.metric_type == MetricType.HAMMING:
            return "hamming_distance"
        return self.distance_function

    def index_param(self) -> ClickZettaLakehouseIndexParam:
        return {
            "index_type": self.index_type,
            "distance_function": self.parse_metric(),
            "scalar_type": self.scalar_type,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "reuse_vector_column": self.reuse_vector_column,
            "compress_codec": self.compress_codec,
            "compress_level": self.compress_level,
            "compress_byte_stream_split": self.compress_byte_stream_split,
            "compress_block_size": self.compress_block_size,
            "conversion_rule": self.conversion_rule,
        }

    def search_param(self) -> ClickZettaLakehouseSearchParam:
        return {
            "distance_function": self.parse_metric(),
            "scalar_type": self.scalar_type,
        }

# 注册映射，便于后续扩展
_clickzettalakehouse_case_config = {
    IndexType.HNSW: ClickZettaLakehouseIndexConfig,
}
