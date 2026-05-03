from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class AppConfig:
    """Configuracoes centralizadas do pipeline."""

    project_root: Path
    app_name: str
    spark_master: str
    pedidos_input_path: str
    pagamentos_input_path: str
    output_path: str
    report_year: int
    write_mode: str
    spark_configs: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_project_root(cls, project_root: Path) -> "AppConfig":
        root = project_root.resolve()

        default_pedidos_path = root / "datasets-csv-pedidos" / "data" / "pedidos" / "*.csv.gz"
        default_pagamentos_path = (
            root / "dataset-json-pagamentos" / "data" / "pagamentos" / "*.json.gz"
        )
        default_output_path = (
            root / "data_output" / "relatorio_pedidos_recusados_legitimos_2025"
        )

        return cls(
            project_root=root,
            app_name=os.getenv("SPARK_APP_NAME", "sales-orders-report-pipeline"),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            pedidos_input_path=os.getenv(
                "PEDIDOS_INPUT_PATH", default_pedidos_path.as_posix()
            ),
            pagamentos_input_path=os.getenv(
                "PAGAMENTOS_INPUT_PATH", default_pagamentos_path.as_posix()
            ),
            output_path=os.getenv("OUTPUT_PATH", default_output_path.as_posix()),
            report_year=int(os.getenv("REPORT_YEAR", "2025")),
            write_mode=os.getenv("WRITE_MODE", "overwrite"),
            spark_configs={
                "spark.driver.bindAddress": "127.0.0.1",
                "spark.driver.host": "127.0.0.1",
                "spark.python.worker.faulthandler.enabled": "true",
                "spark.python.worker.reuse": "false",
                "spark.sql.session.timeZone": "UTC",
                "spark.sql.execution.pyspark.udf.faulthandler.enabled": "true",
                "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"),
                "spark.ui.enabled": os.getenv("SPARK_UI_ENABLED", "false"),
            },
        )
