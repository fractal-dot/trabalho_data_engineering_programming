from __future__ import annotations

import glob
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sales_orders_pipeline.config import OrdersSchemas


class DataIO:
    """Responsavel por leitura e escrita de dados."""

    def __init__(self, spark: SparkSession, schemas: OrdersSchemas) -> None:
        self._spark = spark
        self._schemas = schemas
        self._logger = logging.getLogger(self.__class__.__name__)

    def read_pedidos(self, path: str) -> DataFrame:
        self._logger.info("Lendo pedidos em CSV: %s", path)
        input_paths = self._resolve_local_paths(path)
        return (
            self._spark.read.format("csv")
            .schema(self._schemas.PEDIDOS_SCHEMA)
            .option("header", "true")
            .option("sep", ";")
            .option("mode", "FAILFAST")
            .load(input_paths)
        )

    def read_pagamentos(self, path: str) -> DataFrame:
        self._logger.info("Lendo pagamentos em JSON: %s", path)
        input_paths = self._resolve_local_paths(path)
        return (
            self._spark.read.format("json")
            .schema(self._schemas.PAGAMENTOS_SCHEMA)
            .option("mode", "FAILFAST")
            .load(input_paths)
        )

    def write_parquet(self, dataframe: DataFrame, path: str, mode: str) -> None:
        self._logger.info("Gravando relatorio em Parquet: %s", path)
        dataframe.write.mode(mode).parquet(path)

    def _resolve_local_paths(self, path: str) -> str | list[str]:
        if "://" in path or not glob.has_magic(path):
            return path

        matches = sorted(glob.glob(path))
        if not matches:
            raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrao: {path}")

        resolved_paths = [Path(match).resolve().as_posix() for match in matches]
        self._logger.info("Arquivos encontrados para leitura: %s", len(resolved_paths))
        return resolved_paths
