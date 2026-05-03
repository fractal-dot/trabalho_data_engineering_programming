from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from sales_orders_pipeline.business import OrdersReportTransformer
from sales_orders_pipeline.config import AppConfig, OrdersSchemas
from sales_orders_pipeline.io import DataIO
from sales_orders_pipeline.pipeline import OrdersReportPipeline
from sales_orders_pipeline.spark import SparkSessionManager


def main() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYTHONFAULTHANDLER", "1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    project_root = Path(__file__).resolve().parent
    config = AppConfig.from_project_root(project_root)
    schemas = OrdersSchemas()

    spark_manager = SparkSessionManager(config)
    spark = spark_manager.create_session()

    try:
        data_io = DataIO(spark=spark, schemas=schemas)
        transformer = OrdersReportTransformer(
            schemas=schemas,
            report_year=config.report_year,
        )
        pipeline = OrdersReportPipeline(
            config=config,
            data_io=data_io,
            transformer=transformer,
        )
        pipeline.run()
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    main()
