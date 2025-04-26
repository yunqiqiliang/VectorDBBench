from ..api import DBCaseConfig, DBConfig, MetricType

from typing import Dict, Any
from pydantic import BaseModel, SecretStr, Field, root_validator
default_hints = {
        "sdk.job.timeout": 60,
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

class ClickZettaLakehouseConfig(DBConfig):
    user_name: str = "qiliang"
    password: SecretStr
    service: str = "api.clickzetta.com"
    instance: str = "f8866243"
    workspace: str = "quick_start"
    schema_: str = Field(default="vector_benchmark", alias="schema")
    vcluster: str = "VECTOR_BENCH"
    sdk_job_timeout: int = 60
    hints: Dict = None


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
            "hints": self.hints,
        }


class ClickZettaLakehouseIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def get_metric_fn(self) -> str:
        if self.metric_type == MetricType.L2:
            return "l2_distance"
        if self.metric_type == MetricType.COSINE:
            return "cosine_distance"
        msg = f"Unsupported metric type: {self.metric_type}"
        raise ValueError(msg)

    def index_param(self) -> dict:
        return {
            "metric_fn": self.get_metric_fn(),
        }

    def search_param(self) -> dict:
        return {
            "metric_fn": self.get_metric_fn(),
        }
