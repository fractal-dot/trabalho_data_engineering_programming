from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from sales_orders_pipeline.config import AppConfig


class SparkSessionManager:
    """Gerencia o ciclo de vida da SparkSession."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._spark: SparkSession | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def create_session(self) -> SparkSession:
        self._logger.info("Criando SparkSession")
        builder = (
            SparkSession.builder.appName(self._config.app_name).master(
                self._config.spark_master
            )
            .config("spark.pyspark.python", os.environ.get("PYSPARK_PYTHON", "python"))
            .config(
                "spark.pyspark.driver.python",
                os.environ.get("PYSPARK_DRIVER_PYTHON", "python"),
            )
        )

        for key, value in self._config.spark_configs.items():
            builder = builder.config(key, value)

        self._spark = builder.getOrCreate()
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._logger.info("Encerrando SparkSession")
            self._spark.stop()
            self._spark = None
