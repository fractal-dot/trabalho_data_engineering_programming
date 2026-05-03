from __future__ import annotations

import logging

from sales_orders_pipeline.business import OrdersReportTransformer
from sales_orders_pipeline.config import AppConfig
from sales_orders_pipeline.io import DataIO


class OrdersReportPipeline:
    """Orquestra leitura, transformacao e escrita do relatorio."""

    def __init__(
        self,
        config: AppConfig,
        data_io: DataIO,
        transformer: OrdersReportTransformer,
    ) -> None:
        self._config = config
        self._data_io = data_io
        self._transformer = transformer
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> None:
        self._logger.info("Iniciando pipeline")

        pedidos_df = self._data_io.read_pedidos(self._config.pedidos_input_path)
        pagamentos_df = self._data_io.read_pagamentos(self._config.pagamentos_input_path)

        report_df = self._transformer.transform(pedidos_df, pagamentos_df)

        self._data_io.write_parquet(
            dataframe=report_df,
            path=self._config.output_path,
            mode=self._config.write_mode,
        )

        self._logger.info("Pipeline finalizado com sucesso")
