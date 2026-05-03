# Sales Orders Pipeline

Pipeline PySpark para gerar um relatorio de pedidos de venda com pagamento recusado, avaliacao de fraude legitima e data de criacao no ano de 2025.

## Objetivo

O projeto le pedidos em CSV compactado e pagamentos em JSON compactado, aplica regras de negocio com schemas explicitos e grava o resultado final em Parquet.

O relatorio final contem:

1. Identificador do pedido
2. UF onde o pedido foi feito
3. Forma de pagamento
4. Valor total do pedido
5. Data do pedido

## Estrutura

```text
.
|-- main.py
|-- sales_orders_pipeline/
|   |-- config/
|   |   |-- app_config.py
|   |   `-- schemas.py
|   |-- spark/
|   |   `-- session_manager.py
|   |-- io/
|   |   `-- data_io.py
|   |-- business/
|   |   `-- orders_report_transformer.py
|   `-- pipeline/
|       `-- orders_report_pipeline.py
|-- tests/
|   `-- test_orders_report_transformer.py
|-- pyproject.toml
|-- requirements.txt
|-- MANIFEST.in
`-- readme.md
```

O pacote raiz `sales_orders_pipeline` evita conflito com o modulo padrao `io` do Python, mantendo ainda um subpacote `io` dedicado a leitura e escrita.

## Arquitetura

- `AppConfig`: centraliza caminhos, ano do relatorio, modo de escrita e configuracoes Spark.
- `OrdersSchemas`: define schemas explicitos para pedidos, pagamentos e saida.
- `SparkSessionManager`: cria e encerra a `SparkSession`.
- `DataIO`: le CSV/JSON com schema explicito e escreve Parquet.
- `OrdersReportTransformer`: contem a logica de negocio, logs e tratamento de erros.
- `OrdersReportPipeline`: orquestra leitura, transformacao e escrita.
- `main.py`: aggregation root, onde todas as dependencias sao instanciadas e injetadas.

## Schemas

Pedidos CSV:

```text
ID_PEDIDO string
PRODUTO string
VALOR_UNITARIO double
QUANTIDADE long
DATA_CRIACAO string
UF string
ID_CLIENTE long
```

Pagamentos JSON:

```text
id_pedido string
forma_pagamento string
valor_pagamento double
status boolean
data_processamento string
avaliacao_fraude struct<fraude:boolean,score:double>
```

Saida Parquet:

```text
id_pedido string
uf string
forma_pagamento string
valor_total_pedido double
data_pedido timestamp
```

## Regra de negocio

A transformacao:

1. Calcula `valor_total_pedido = VALOR_UNITARIO * QUANTIDADE`.
2. Agrega o valor por `id_pedido`, `uf` e `data_pedido`.
3. Faz join entre pedidos e pagamentos.
4. Mantem apenas registros com `status = false`.
5. Mantem apenas registros com `avaliacao_fraude.fraude = false`.
6. Mantem apenas pedidos criados no ano `2025`.
7. Seleciona somente as colunas finais.
8. Ordena por `uf`, `forma_pagamento` e `data_pedido`.
9. Grava o resultado em Parquet.

## Como executar

Crie ou ative a venv:

```powershell
cd d:\trabalho_marcelo\trabalho_data_engineering_programming
.\venv312\Scripts\Activate.ps1
```

Instale as dependencias:

```powershell
pip install -r requirements.txt
```

Execute o pipeline:

```powershell
python main.py
```

A saida padrao sera gravada em:

```text
data_output/relatorio_pedidos_recusados_legitimos_2025/
```

## Variaveis de ambiente

Os valores padrao podem ser sobrescritos:

```powershell
$env:PEDIDOS_INPUT_PATH="datasets-csv-pedidos/data/pedidos/*.csv.gz"
$env:PAGAMENTOS_INPUT_PATH="dataset-json-pagamentos/data/pagamentos/*.json.gz"
$env:OUTPUT_PATH="data_output/relatorio_customizado"
$env:REPORT_YEAR="2025"
$env:WRITE_MODE="overwrite"
$env:SPARK_MASTER="local[*]"
```

## Testes

Execute:

```powershell
pytest
```

O teste unitario valida a classe de transformacao, garantindo que apenas pedidos recusados, legitimos e criados no ano configurado sejam retornados.

## Observacao para Windows

Ao executar testes ou pipeline pela primeira vez, o Windows pode pedir permissao de rede para o Java. Autorize em redes privadas, pois o Spark usa comunicacao local entre o driver Java e os workers Python mesmo em modo `local`.

O projeto tambem fixa automaticamente:

```text
PYSPARK_PYTHON
PYSPARK_DRIVER_PYTHON
SPARK_LOCAL_IP=127.0.0.1
SPARK_LOCAL_HOSTNAME=localhost
```

Esses valores ajudam o Spark a usar o Python da venv ativa e a se comunicar via localhost.
