from datetime import datetime
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ.setdefault("PYTHONFAULTHANDLER", "1")

import pytest
from pyspark.sql import SparkSession

from sales_orders_pipeline.business import OrdersReportTransformer
from sales_orders_pipeline.config import OrdersSchemas


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("orders-report-transformer-test")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.python.worker.reuse", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform_keeps_only_declined_legitimate_orders_from_report_year(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    schemas = OrdersSchemas()
    transformer = OrdersReportTransformer(schemas=schemas, report_year=2025)

    pedidos_path = tmp_path / "pedidos.csv"
    pedidos_path.write_text(
        "\n".join(
            [
                "ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE",
                "pedido-valido;MONITOR;600.0;2;2025-01-03T07:26:01;SP;100",
                "pedido-aprovado;TABLET;1100.0;1;2025-01-17T14:40:35;RS;3216",
                "pedido-fraude;NOTEBOOK;1500.0;1;2025-02-01T09:10:11;RJ;3217",
                "pedido-2024;CELULAR;1000.0;1;2024-12-31T23:59:59;MG;3218",
            ]
        ),
        encoding="utf-8",
    )

    pagamentos_path = tmp_path / "pagamentos.json"
    pagamentos_rows = [
        {
            "id_pedido": "pedido-valido",
            "forma_pagamento": "PIX",
            "valor_pagamento": 1200.0,
            "status": False,
            "data_processamento": "2025-01-04T02:46:26.582439",
            "avaliacao_fraude": {"fraude": False, "score": 0.20},
        },
        {
            "id_pedido": "pedido-aprovado",
            "forma_pagamento": "BOLETO",
            "valor_pagamento": 1100.0,
            "status": True,
            "data_processamento": "2025-01-18T03:11:27.267872",
            "avaliacao_fraude": {"fraude": False, "score": 0.23},
        },
        {
            "id_pedido": "pedido-fraude",
            "forma_pagamento": "CARTAO_CREDITO",
            "valor_pagamento": 1500.0,
            "status": False,
            "data_processamento": "2025-02-02T10:53:32.944239",
            "avaliacao_fraude": {"fraude": True, "score": 0.98},
        },
        {
            "id_pedido": "pedido-2024",
            "forma_pagamento": "PIX",
            "valor_pagamento": 1000.0,
            "status": False,
            "data_processamento": "2025-01-01T00:00:00.000000",
            "avaliacao_fraude": {"fraude": False, "score": 0.10},
        },
    ]
    pagamentos_path.write_text(
        "\n".join(json.dumps(row) for row in pagamentos_rows),
        encoding="utf-8",
    )

    pedidos_df = (
        spark_session.read.schema(schemas.PEDIDOS_SCHEMA)
        .option("header", "true")
        .option("sep", ";")
        .csv(pedidos_path.as_posix())
    )
    pagamentos_df = spark_session.read.schema(schemas.PAGAMENTOS_SCHEMA).json(
        pagamentos_path.as_posix()
    )

    result = transformer.transform(pedidos_df, pagamentos_df)
    rows = result.collect()

    assert result.columns == [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total_pedido",
        "data_pedido",
    ]
    assert len(rows) == 1
    assert rows[0].asDict() == {
        "id_pedido": "pedido-valido",
        "uf": "SP",
        "forma_pagamento": "PIX",
        "valor_total_pedido": 1200.0,
        "data_pedido": datetime(2025, 1, 3, 7, 26, 1),
    }
