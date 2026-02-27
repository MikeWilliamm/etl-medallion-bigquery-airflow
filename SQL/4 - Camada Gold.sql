--------------------- ITEM 4 DA TAREFA
-- ================================================================
-- CAMADA GOLD — Tabelas Fixas com Particionamento e Clustering
-- Projeto: acoes-378306 | pmweb_gold
-- ================================================================


-- ================================================================
-- GOLD 1: Quantidade de pedidos por cliente por semestre/ano
--         Apenas clientes que parcelaram (PARCELAS > 1)
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.pedidos_por_cliente_semestre`
PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
CLUSTER BY ID_CLIENTE
OPTIONS (description = 'Qtd de pedidos por cliente por semestre/ano — apenas parcelados')
AS
WITH clientes_parcelados AS (
  SELECT DISTINCT ID_CLIENTE
  FROM `acoes-378306.pmweb_silver.pedido`
  WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
    AND PARCELADO = TRUE
)
SELECT
  p.ID_CLIENTE,
  c.NOME,
  p.ANO,
  p.SEMESTRE,
  CONCAT(CAST(p.ANO AS STRING), ' - S', CAST(p.SEMESTRE AS STRING)) AS ANO_SEMESTRE,
  COUNT(DISTINCT p.ID_PEDIDO)                                        AS QTD_PEDIDOS,
  ROUND(SUM(p.VALOR_TOTAL), 2)                                       AS VALOR_TOTAL_PERIODO
FROM `acoes-378306.pmweb_silver.pedido` p
INNER JOIN clientes_parcelados cp ON cp.ID_CLIENTE = p.ID_CLIENTE
LEFT  JOIN `acoes-378306.pmweb_silver.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
GROUP BY p.ID_CLIENTE, c.NOME, p.ANO, p.SEMESTRE;

-- ================================================================
-- GOLD 2: Ticket médio por cliente por ano e mês
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.ticket_medio_cliente_ano_mes`
PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
CLUSTER BY ID_CLIENTE
OPTIONS (description = 'Ticket médio por cliente agrupado por ano e mês')
AS
SELECT
  p.ID_CLIENTE,
  c.NOME,
  p.ANO,
  p.MES,
  FORMAT('%04d-%02d', p.ANO, p.MES)      AS ANO_MES,
  COUNT(DISTINCT p.ID_PEDIDO)             AS QTD_PEDIDOS,
  ROUND(SUM(p.VALOR_TOTAL), 2)            AS VALOR_TOTAL,
  ROUND(
    SUM(p.VALOR_TOTAL) / COUNT(DISTINCT p.ID_PEDIDO)
  , 2)                                    AS TICKET_MEDIO
FROM `acoes-378306.pmweb_silver.pedido` p
LEFT JOIN `acoes-378306.pmweb_silver.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
GROUP BY p.ID_CLIENTE, c.NOME, p.ANO, p.MES;


-- ================================================================
-- GOLD 3: Intervalo médio entre compras por cliente por ano
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.intervalo_medio_compras`
PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
CLUSTER BY ID_CLIENTE
OPTIONS (description = 'Intervalo médio em dias entre compras de cada cliente, por ano')
AS
WITH datas_distintas AS (
  SELECT DISTINCT
    ID_CLIENTE,
    ANO,
    DATA_PEDIDO
  FROM `acoes-378306.pmweb_silver.pedido`
  WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
),
com_lag AS (
  SELECT
    ID_CLIENTE,
    ANO,
    DATA_PEDIDO,
    LAG(DATA_PEDIDO) OVER (
      PARTITION BY ID_CLIENTE
      ORDER BY DATA_PEDIDO
    ) AS DATA_COMPRA_ANTERIOR
  FROM datas_distintas
),
intervalos AS (
  SELECT
    ID_CLIENTE,
    ANO,
    DATE_DIFF(DATA_PEDIDO, DATA_COMPRA_ANTERIOR, DAY) AS DIAS_ENTRE_COMPRAS
  FROM com_lag
  WHERE DATA_COMPRA_ANTERIOR IS NOT NULL
)
SELECT
  i.ID_CLIENTE,
  c.NOME,
  i.ANO,
  COUNT(*)                                    AS QTD_INTERVALOS,
  ROUND(AVG(i.DIAS_ENTRE_COMPRAS), 1)         AS INTERVALO_MEDIO_DIAS,
  MIN(i.DIAS_ENTRE_COMPRAS)                   AS MENOR_INTERVALO_DIAS,
  MAX(i.DIAS_ENTRE_COMPRAS)                   AS MAIOR_INTERVALO_DIAS
FROM intervalos i
LEFT JOIN `acoes-378306.pmweb_silver.cadastro` c ON c.ID_CLIENTE = i.ID_CLIENTE
GROUP BY i.ID_CLIENTE, c.NOME, i.ANO;


-- ================================================================
-- GOLD 4: Classificação de clientes em tiers por valor mensal
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.tiers_clientes`
PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
CLUSTER BY ID_CLIENTE, TIER
OPTIONS (description = 'Tier mensal de cada cliente baseado no valor total de compras confirmadas')
AS
WITH valor_mensal AS (
  SELECT
    ID_CLIENTE,
    ANO,
    MES,
    FORMAT('%04d-%02d', ANO, MES) AS ANO_MES,
    ROUND(SUM(VALOR_TOTAL), 2)    AS VALOR_MES
  FROM `acoes-378306.pmweb_silver.pedido`
  WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
  GROUP BY ID_CLIENTE, ANO, MES
)
SELECT
  vm.ID_CLIENTE,
  c.NOME,
  vm.ANO,
  vm.MES,
  vm.ANO_MES,
  vm.VALOR_MES,
  CASE
    WHEN vm.VALOR_MES <  1000 THEN 'Básico'
    WHEN vm.VALOR_MES <  2000 THEN 'Prata'
    WHEN vm.VALOR_MES <  5000 THEN 'Ouro'
    ELSE                           'Super'
  END                             AS TIER
FROM valor_mensal vm
LEFT JOIN `acoes-378306.pmweb_silver.cadastro` c ON c.ID_CLIENTE = vm.ID_CLIENTE;


-- ================================================================
-- GOLD 5: Comparativo percentual SOM vs PAPELARIA — 2019 x 2020
--         Sem partição — resultado é apenas 2 linhas
-- ================================================================
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.comparativo_som_papelaria`
OPTIONS (description = 'Comparativo % de vendas confirmadas entre 2019 e 2020 — SOM e PAPELARIA')
AS
WITH base AS (
  SELECT
    DEPARTAMENTO,
    ANO,
    ROUND(SUM(VALOR_TOTAL), 2) AS TOTAL_VENDAS
  FROM `acoes-378306.pmweb_silver.pedido`
  WHERE STATUS_PAGAMENTO  = 'CONFIRMADO'
    AND DEPARTAMENTO      IN ('SOM', 'PAPELARIA')
    AND ANO               IN (2019, 2020)
  GROUP BY DEPARTAMENTO, ANO
),
pivot AS (
  SELECT
    DEPARTAMENTO,
    SUM(CASE WHEN ANO = 2019 THEN TOTAL_VENDAS ELSE 0 END) AS TOTAL_2019,
    SUM(CASE WHEN ANO = 2020 THEN TOTAL_VENDAS ELSE 0 END) AS TOTAL_2020
  FROM base
  GROUP BY DEPARTAMENTO
)
SELECT
  DEPARTAMENTO,
  TOTAL_2019,
  TOTAL_2020,
  ROUND(TOTAL_2020 - TOTAL_2019, 2)                                  AS VARIACAO_ABSOLUTA,
  ROUND(SAFE_DIVIDE(TOTAL_2020 - TOTAL_2019, TOTAL_2019) * 100, 2)   AS VARIACAO_PERCENTUAL
FROM pivot;