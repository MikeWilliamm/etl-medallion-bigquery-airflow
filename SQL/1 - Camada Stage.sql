-- ================================================================
-- 1. CAMADA STAGE
-- Considerando que o Google Cloud Storage seja a fonte inicial dos dados
-- Fazemos a carga direta do Storage sem nenhuma transformação
-- Os tipos são todos STRING para preservar o dado original
-- ================================================================

-- Tabela de staging de cliente
CREATE OR REPLACE TABLE `acoes-378306.pmweb_stage.stg_cadastro` (
  ID            STRING,
  EMAIL         STRING,
  NOME          STRING,
  DT_NASC       STRING,
  SEXO          STRING,
  CADASTRO      STRING,
  CIDADE        STRING,
  ESTADO        STRING,
  RECEBE_EMAIL  STRING,
  string_field_9   STRING,
  string_field_10  STRING,
  string_field_11  STRING,
  string_field_12  STRING
)
OPTIONS (description = 'Stage: ingestão bruta do arquivo CADASTROS.csv');

-- Tabela de staging de pedido
CREATE OR REPLACE TABLE `acoes-378306.pmweb_stage.stg_pedido` (
  COD_CLIENTE       STRING,
  COD_PEDIDO        STRING,
  CODIGO_PRODUTO    STRING,
  DEPTO             STRING,
  QUANTIDADE        STRING,
  VALOR_UNITARIO    STRING,
  QTD_PARCELAS      STRING,
  DT_PEDIDO         STRING,
  MEIO_PAGTO        STRING,
  STATUS_PAGAMENTO  STRING
)
OPTIONS (description = 'Stage: ingestão bruta do arquivo pedido.csv');

-- ================================================================
-- CARGA DO GCS PARA STAGE (LOAD JOB)
-- Executar via bq CLI ou Cloud Composer/Airflow
-- ================================================================
-- CADASTROS
bq load \
  --source_format=CSV \
  --field_delimiter=";" \
  --skip_leading_rows=1 \
  --replace \
  pmweb_stage.stg_cadastro \
  gs://pmweb/CADASTRO/CADASTROS.csv

-- pedido
bq load \
  --source_format=CSV \
  --field_delimiter=";" \
  --skip_leading_rows=1 \
  --replace \
  pmweb_stage.stg_pedido \
  gs://pmweb/PEDIDO/PEDIDOS.csv
