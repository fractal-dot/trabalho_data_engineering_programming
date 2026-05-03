from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_orders_pipeline.config import OrdersSchemas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class DataTransformationError(RuntimeError):
    """Erro de transformacao da camada de negocio."""


class OrdersReportTransformer:
    """Aplica as regras de negocio do relatorio de pedidos."""

    def __init__(self, schemas: OrdersSchemas, report_year: int) -> None:
        self._schemas = schemas
        self._report_year = report_year
        self._logger = logging.getLogger(self.__class__.__name__)

    def transform(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        try:
            self._logger.info("Iniciando transformacao do relatorio")

            self._logger.info("Normalizando e agregando pedidos")
            pedidos = (
                pedidos_df.select(
                    F.col("ID_PEDIDO").alias("id_pedido"),
                    F.col("UF").alias("uf"),
                    F.to_timestamp(
                        F.col("DATA_CRIACAO"), "yyyy-MM-dd'T'HH:mm:ss"
                    ).alias("data_pedido"),
                    (
                        F.col("VALOR_UNITARIO").cast("double")
                        * F.col("QUANTIDADE").cast("long")
                    ).alias("valor_item"),
                )
                .groupBy("id_pedido", "uf", "data_pedido")
                .agg(F.sum("valor_item").alias("valor_total_pedido"))
            )

            self._logger.info("Normalizando pagamentos e avaliacao de fraude")
            pagamentos = pagamentos_df.select(
                F.col("id_pedido").alias("id_pedido"),
                F.col("forma_pagamento").alias("forma_pagamento"),
                F.col("status").alias("status_pagamento"),
                F.col("avaliacao_fraude.fraude").alias("fraude"),
            )

            self._logger.info("Unindo pedidos e pagamentos")
            joined = pedidos.join(pagamentos, on="id_pedido", how="inner")

            self._logger.info("Aplicando filtros de negocio")
            filtered = joined.filter(
                (F.col("status_pagamento") == F.lit(False))
                & (F.col("fraude") == F.lit(False))
                & (F.year(F.col("data_pedido")) == F.lit(self._report_year))
            )

            self._logger.info("Selecionando schema final e ordenando relatorio")
            result = (
                filtered.select(
                    F.col("id_pedido").cast("string").alias("id_pedido"),
                    F.col("uf").cast("string").alias("uf"),
                    F.col("forma_pagamento").cast("string").alias("forma_pagamento"),
                    F.col("valor_total_pedido")
                    .cast("double")
                    .alias("valor_total_pedido"),
                    F.col("data_pedido").cast("timestamp").alias("data_pedido"),
                )
                .select([field.name for field in self._schemas.REPORT_SCHEMA.fields])
                .orderBy("uf", "forma_pagamento", "data_pedido")
            )

            self._logger.info("Transformacao concluida")
            return result
        except Exception as exc:
            self._logger.exception("Erro ao transformar dados do relatorio")
            raise DataTransformationError("Falha na transformacao do relatorio") from exc
