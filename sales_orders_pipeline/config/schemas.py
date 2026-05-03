from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class OrdersSchemas:
    """Schemas explicitos usados na leitura e na saida do relatorio."""

    PEDIDOS_SCHEMA: StructType = StructType(
        [
            StructField("ID_PEDIDO", StringType(), nullable=False),
            StructField("PRODUTO", StringType(), nullable=True),
            StructField("VALOR_UNITARIO", DoubleType(), nullable=True),
            StructField("QUANTIDADE", LongType(), nullable=True),
            StructField("DATA_CRIACAO", StringType(), nullable=False),
            StructField("UF", StringType(), nullable=True),
            StructField("ID_CLIENTE", LongType(), nullable=True),
        ]
    )

    PAGAMENTOS_SCHEMA: StructType = StructType(
        [
            StructField("id_pedido", StringType(), nullable=False),
            StructField("forma_pagamento", StringType(), nullable=True),
            StructField("valor_pagamento", DoubleType(), nullable=True),
            StructField("status", BooleanType(), nullable=True),
            StructField("data_processamento", StringType(), nullable=True),
            StructField(
                "avaliacao_fraude",
                StructType(
                    [
                        StructField("fraude", BooleanType(), nullable=True),
                        StructField("score", DoubleType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    REPORT_SCHEMA: StructType = StructType(
        [
            StructField("id_pedido", StringType(), nullable=False),
            StructField("uf", StringType(), nullable=True),
            StructField("forma_pagamento", StringType(), nullable=True),
            StructField("valor_total_pedido", DoubleType(), nullable=True),
            StructField("data_pedido", TimestampType(), nullable=True),
        ]
    )
