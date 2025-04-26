from typing import Annotated, Unpack

import click
from pydantic import SecretStr

from vectordb_bench.backend.clients import DB

from ....cli.cli import CommonTypedDict, cli, click_parameter_decorators_from_typed_dict, run


class ClickZettaLakehouseTypedDict(CommonTypedDict):
    user_name: Annotated[
        str,
        click.option(
            "--username",
            type=str,
            help="Username",
            default="root",
            show_default=True,
            required=True,
        ),
    ]
    password: Annotated[
        str,
        click.option(
            "--password",
            type=str,
            default="",
            show_default=True,
            help="Password",
        ),
    ]
    service: Annotated[
        str,
        click.option(
            "--service",
            type=str,
            required=True,
            help="Service endpoint (e.g., api.clickzetta.com)",
        ),
    ]
    instance: Annotated[
        str,
        click.option(
            "--instance",
            type=str,
            required=True,
            help="Instance name of the ClickZetta Lakehouse",
        ),
    ]
    workspace: Annotated[
        str,
        click.option(
            "--workspace",
            type=str,
            required=True,
            help="Workspace name of the ClickZetta Lakehouse",
        ),
    ]
    schema: Annotated[
        str,
        click.option(
            "--schema",
            type=str,
            required=True,
            help="Schema name of the ClickZetta Lakehouse",
        ),
    ]
    vcluster: Annotated[
        str,
        click.option(
            "--vcluster",
            type=str,
            required=True,
            help="VCluster name of the ClickZetta Lakehouse",
        ),
    ]
    sdk_job_timeout: Annotated[
        int,
        click.option(
            "--sdk-job-timeout",
            type=int,
            default=60,
            show_default=True,
            help="SDK job timeout in seconds",
        ),
    ]
    hints: Annotated[
        str,
        click.option(
            "--hints",
            type=str,
            default='{"sdk.job.timeout": 60, "query_tag": "VectorBench", "cz.storage.parquet.vector.index.read.memory.cache": "true", "cz.storage.parquet.vector.index.read.local.cache": "false", "cz.sql.table.scan.push.down.filter": "true", "cz.sql.table.scan.enable.ensure.filter": "true", "cz.storage.always.prefetch.internal": "true", "cz.optimizer.generate.columns.always.valid": "true", "cz.sql.index.prewhere.enabled": "true", "cz.storage.parquet.enable.io.prefetch": "false"}',
            show_default=True,
            help="Hints for ClickZetta Lakehouse in JSON format",
        ),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(ClickZettaLakehouseTypedDict)
def ClickZettaLakehouse(
    **parameters: Unpack[ClickZettaLakehouseTypedDict],
):
    from .config import ClickZettaLakehouseConfig, ClickZettaLakehouseIndexConfig
    import json

    # Parse hints from JSON string
    hints = json.loads(parameters["hints"])

    run(
        db=DB.ClickZettaLakehouse,
        db_config=ClickZettaLakehouseConfig(
            db_label=parameters["db_label"],
            user_name=parameters["username"],
            password=SecretStr(parameters["password"]),
            service=parameters["service"],
            instance=parameters["instance"],
            workspace=parameters["workspace"],
            schema=parameters["schema"],
            vcluster=parameters["vcluster"],
            sdk_job_timeout=parameters["sdk_job_timeout"],
            hints=hints,
        ),
        db_case_config=ClickZettaLakehouseIndexConfig(),
        **parameters,
    )