-- ================================================================
-- 2. CAMADA RAW
-- Adiciona colunas de controle de carga, sem alterar os dados
-- Histórico de dados é mantido completamente
-- ================================================================

CREATE TABLE IF NOT EXISTS `acoes-378306.pmweb_raw.raw_cadastro` (
  ID               STRING,
  EMAIL            STRING,
  NOME             STRING,
  DT_NASC          STRING,
  SEXO             STRING,
  CADASTRO         STRING,
  CIDADE           STRING,
  ESTADO           STRING,
  RECEBE_EMAIL     STRING,
  DT_CARGA         TIMESTAMP,
  ORIGEM           STRING,
  UUID_LINHA       STRING
);

CREATE TABLE IF NOT EXISTS `acoes-378306.pmweb_raw.raw_pedido` (
  COD_CLIENTE      STRING,
  COD_PEDIDO       STRING,
  CODIGO_PRODUTO   STRING,
  DEPTO            STRING,
  QUANTIDADE       STRING,
  VALOR_UNITARIO   STRING,
  QTD_PARCELAS     STRING,
  DT_PEDIDO        STRING,
  MEIO_PAGTO       STRING,
  STATUS_PAGAMENTO STRING,
  DT_CARGA         TIMESTAMP,
  ORIGEM           STRING,
  UUID_LINHA       STRING
);

INSERT INTO `acoes-378306.pmweb_raw.raw_cadastro`
SELECT
  ID, EMAIL, NOME, DT_NASC, SEXO, CADASTRO, CIDADE, ESTADO, RECEBE_EMAIL,
  CURRENT_TIMESTAMP() AS DT_CARGA,
  'GCS:CADASTROS.csv' AS ORIGEM,
  GENERATE_UUID()     AS UUID_LINHA
FROM `acoes-378306.pmweb_stage.stg_cadastro`
WHERE ID IS NOT NULL AND TRIM(ID) != '' AND TRIM(ID) != 'ID';

INSERT INTO `acoes-378306.pmweb_raw.raw_pedido`
SELECT
  COD_CLIENTE, COD_PEDIDO, CODIGO_PRODUTO, DEPTO, QUANTIDADE,
  VALOR_UNITARIO, QTD_PARCELAS, DT_PEDIDO, MEIO_PAGTO, STATUS_PAGAMENTO,
  CURRENT_TIMESTAMP() AS DT_CARGA,
  'GCS:pedido.csv'   AS ORIGEM,
  GENERATE_UUID()     AS UUID_LINHA
FROM `acoes-378306.pmweb_stage.stg_pedido`
WHERE COD_CLIENTE IS NOT NULL AND TRIM(COD_CLIENTE) != '' AND TRIM(COD_CLIENTE) != 'COD_CLIENTE';
