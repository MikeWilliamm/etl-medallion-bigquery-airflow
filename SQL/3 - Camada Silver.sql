-- ================================================================
-- 4. CAMADA SILVER
-- Limpeza, tipagem correta e padronização
-- ================================================================

-- ================================================================
-- SILVER: cliente
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_silver.cadastro`
OPTIONS (description = 'Silver: cliente limpos, tipados e padronizados')
AS

WITH ultima_carga AS (
  SELECT MAX(DT_CARGA) AS DT_CARGA
  FROM `acoes-378306.pmweb_raw.raw_cadastro`
),
ufs_validas AS (
  -- 26 estados + DF
  SELECT uf FROM UNNEST([
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
    'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
    'RS','RO','RR','SC','SP','SE','TO'
  ]) AS uf
)

SELECT
  CAST(ID AS INT64)                                         AS ID_CLIENTE,
  LOWER(TRIM(EMAIL))                                        AS EMAIL,
  INITCAP(TRIM(NOME))                                       AS NOME,

  -- DT_NASC: SAFE_ retorna NULL para datas inválidas (ex: 29/02 em ano não bissexto)
  SAFE.PARSE_DATE('%d/%m/%Y', TRIM(DT_NASC))               AS DATA_NASCIMENTO,

  -- Idade calculada com base na data de nascimento
  DATE_DIFF(
    CURRENT_DATE(),
    SAFE.PARSE_DATE('%d/%m/%Y', TRIM(DT_NASC)),
    YEAR
  )                                                         AS IDADE,

  TRIM(SEXO)                                                AS SEXO,

  SAFE.PARSE_DATE('%d/%m/%Y', TRIM(CADASTRO))              AS DATA_CADASTRO,

  -- CIDADE: anula valores claramente inválidos (numéricos, pontuação)
  CASE
    WHEN TRIM(CIDADE) = ''           THEN NULL
    WHEN TRIM(CIDADE) = '.'          THEN NULL
    WHEN REGEXP_CONTAINS(TRIM(CIDADE), r'^\d+$') THEN NULL
    ELSE UPPER(TRIM(CIDADE))
  END                                                       AS CIDADE,

  -- ESTADO: mantém apenas UFs oficiais do Brasil
  CASE
    WHEN u.uf IS NOT NULL THEN TRIM(ESTADO)
    ELSE NULL
  END                                                       AS UF,

  CAST(RECEBE_EMAIL AS INT64) = 1                          AS PERMISSAO_RECEBE_EMAIL,

  -- controle
  r.DT_CARGA

FROM `acoes-378306.pmweb_raw.raw_cadastro` r
LEFT JOIN ufs_validas u ON u.uf = TRIM(r.ESTADO)
INNER JOIN ultima_carga uc ON r.DT_CARGA = uc.DT_CARGA

-- Remove linhas completamente vazias
WHERE TRIM(r.ID) IS NOT NULL
  AND TRIM(r.ID) != '';


-- ================================================================
-- SILVER: pedido
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_silver.pedido`
OPTIONS (description = 'Silver: pedido limpos, tipados e com colunas analíticas derivadas')
AS

WITH ultima_carga AS (
  SELECT MAX(DT_CARGA) AS DT_CARGA
  FROM `acoes-378306.pmweb_raw.raw_pedido`
)

SELECT
  CAST(COD_CLIENTE   AS INT64)                              AS ID_CLIENTE,
  CAST(COD_PEDIDO    AS INT64)                              AS ID_PEDIDO,
  CAST(CODIGO_PRODUTO AS INT64)                             AS ID_PRODUTO,
  UPPER(TRIM(DEPTO))                                        AS DEPARTAMENTO,
  CAST(QUANTIDADE    AS INT64)                              AS QUANTIDADE,
  CAST(VALOR_UNITARIO AS FLOAT64)                           AS VALOR_UNITARIO,

  -- PARCELAS: nulo ou 0 tratado como 1 (à vista)
  CASE
    WHEN QTD_PARCELAS IS NULL        THEN 1
    WHEN TRIM(QTD_PARCELAS) = ''     THEN 1
    WHEN CAST(QTD_PARCELAS AS INT64) = 0 THEN 1
    ELSE CAST(QTD_PARCELAS AS INT64)
  END                                                       AS PARCELAS,

  PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))                  AS DATA_PEDIDO,
  UPPER(TRIM(MEIO_PAGTO))                                   AS MEIO_PAGAMENTO,
  UPPER(TRIM(STATUS_PAGAMENTO))                             AS STATUS_PAGAMENTO,

  -- Valor total do item (quantidade x valor unitário)
  CAST(QUANTIDADE AS INT64)
    * CAST(VALOR_UNITARIO AS FLOAT64)                       AS VALOR_TOTAL,

  -- Flag de parcelamento (parcelas > 1)
  CASE
    WHEN QTD_PARCELAS IS NULL        THEN FALSE
    WHEN TRIM(QTD_PARCELAS) = ''     THEN FALSE
    WHEN CAST(QTD_PARCELAS AS INT64) <= 1 THEN FALSE
    ELSE TRUE
  END                                                       AS PARCELADO,

  -- Campos de tempo derivados (úteis nas views Gold)
  EXTRACT(YEAR  FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) AS ANO,
  EXTRACT(MONTH FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) AS MES,
  CASE
    WHEN EXTRACT(MONTH FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) <= 6
    THEN 1 ELSE 2
  END                                                       AS SEMESTRE,

  -- controle
  r.DT_CARGA

FROM `acoes-378306.pmweb_raw.raw_pedido` r
INNER JOIN ultima_carga uc ON r.DT_CARGA = uc.DT_CARGA

WHERE
  -- Apenas pedido com desfecho definido
  UPPER(TRIM(STATUS_PAGAMENTO)) IN ('CONFIRMADO', 'CANCELADO')

  -- Remove valores inválidos
  AND CAST(VALOR_UNITARIO AS FLOAT64) > 0

  -- Remove confirmados sem valor (sem significado analítico)
  AND NOT (
    UPPER(TRIM(STATUS_PAGAMENTO)) = 'CONFIRMADO'
    AND CAST(VALOR_UNITARIO AS FLOAT64) = 0
  );
