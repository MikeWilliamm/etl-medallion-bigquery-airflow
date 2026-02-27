--------------------- ITEM 2 DA TAREFA
-- ================================================================
-- ITEM 2A — Análises aplicáveis à amostra de dados
-- ================================================================
--
-- A partir da exploração dos dados de cadastro e pedidos, foram
-- identificadas as seguintes análises com potencial de negócio:
--
-- 1. TAXA DE CANCELAMENTO POR DEPARTAMENTO
--    Todos os departamentos têm taxa de cancelamento entre 17-20%,
--    com BRINQUEDOS (19,7%) e PAPELARIA (19,0%) no topo. Identificar
--    quais departamentos cancelam mais permite priorizar ações de
--    melhoria na experiência de compra e logística.
--
-- 2. COMPORTAMENTO DE COMPRA POR DIA DA SEMANA
--    Sexta-feira concentra 18.210 pedidos — mais que o dobro de
--    terça (7.505). Fim de semana também é forte. Isso permite
--    otimizar campanhas de marketing e disponibilidade de estoque
--    nos dias de maior demanda.
--
-- 3. IMPACTO DA PERMISSÃO DE E-MAIL NO TICKET MÉDIO
--    Clientes que aceitam receber e-mail têm ticket médio de
--    R$6.777 vs R$5.697 dos que não aceitam — uma diferença de
--    19%. Isso sugere que a comunicação por e-mail influencia
--    positivamente o valor das compras, justificando investimento
--    em estratégias de opt-in.
--
-- 4. CRESCIMENTO DA BASE DE CLIENTES POR ANO
--    A base cresceu de 12.510 novos clientes em 2019 para 10.854
--    em 2020 e 9.074 em 2021, indicando desaceleração na aquisição.
--    Cruzado com a baixa retenção, sugere necessidade de equilibrar
--    investimento entre aquisição e retenção.
--
-- ================================================================


-- ================================================================
-- ITEM 2B — Queries SQL das análises descritas acima
-- ================================================================

-- ----------------------------------------------------------------
-- ANÁLISE 1: Taxa de cancelamento por departamento
-- Permite identificar onde a operação tem mais problemas
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.cancelamento_por_departamento`
OPTIONS (description = 'Taxa de cancelamento por departamento')
AS
SELECT
  DEPARTAMENTO,
  COUNT(*)                                              AS TOTAL_PEDIDOS,
  COUNTIF(STATUS_PAGAMENTO = 'CONFIRMADO')              AS CONFIRMADOS,
  COUNTIF(STATUS_PAGAMENTO = 'CANCELADO')               AS CANCELADOS,
  ROUND(
    100 * COUNTIF(STATUS_PAGAMENTO = 'CANCELADO')
    / COUNT(*), 1
  )                                                     AS PCT_CANCELAMENTO,
  ROUND(SUM(
    CASE WHEN STATUS_PAGAMENTO = 'CONFIRMADO'
    THEN VALOR_TOTAL ELSE 0 END), 2)                    AS RECEITA_CONFIRMADA,
  ROUND(SUM(
    CASE WHEN STATUS_PAGAMENTO = 'CANCELADO'
    THEN VALOR_TOTAL ELSE 0 END), 2)                    AS RECEITA_PERDIDA
FROM `acoes-378306.pmweb_silver.pedido`
GROUP BY DEPARTAMENTO
ORDER BY PCT_CANCELAMENTO DESC;


-- ----------------------------------------------------------------
-- ANÁLISE 2: Volume de pedidos por dia da semana
-- Identifica os melhores dias para campanhas e operação
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.pedidos_por_dia_semana`
OPTIONS (description = 'Distribuição de pedidos confirmados por dia da semana')
AS
SELECT
  FORMAT_DATE('%A', DATA_PEDIDO)                        AS DIA_SEMANA,
  EXTRACT(DAYOFWEEK FROM DATA_PEDIDO)                   AS NUM_DIA,
  COUNT(DISTINCT ID_PEDIDO)                             AS QTD_PEDIDOS,
  ROUND(SUM(VALOR_TOTAL), 2)                            AS RECEITA_TOTAL,
  ROUND(AVG(VALOR_TOTAL), 2)                            AS TICKET_MEDIO,
  ROUND(
    100 * COUNT(DISTINCT ID_PEDIDO)
    / SUM(COUNT(DISTINCT ID_PEDIDO)) OVER(), 1
  )                                                     AS PCT_DO_TOTAL
FROM `acoes-378306.pmweb_silver.pedido`
WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
GROUP BY DIA_SEMANA, NUM_DIA
ORDER BY NUM_DIA;


-- ----------------------------------------------------------------
-- ANÁLISE 3: Impacto da permissão de e-mail no ticket médio
-- Avalia se clientes opt-in compram mais
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.impacto_permissao_email`
OPTIONS (description = 'Comparativo de comportamento de compra por permissão de e-mail')
AS
SELECT
  CASE WHEN c.PERMISSAO_RECEBE_EMAIL THEN 'Aceita e-mail' ELSE 'Não aceita e-mail' END
                                                        AS PERMISSAO_EMAIL,
  COUNT(DISTINCT p.ID_CLIENTE)                          AS QTD_CLIENTES,
  COUNT(DISTINCT p.ID_PEDIDO)                           AS QTD_PEDIDOS,
  ROUND(SUM(p.VALOR_TOTAL), 2)                          AS RECEITA_TOTAL,
  ROUND(AVG(p.VALOR_TOTAL), 2)                          AS TICKET_MEDIO,
  ROUND(
    100 * AVG(p.VALOR_TOTAL)
    / SUM(AVG(p.VALOR_TOTAL)) OVER() , 1
  )                                                     AS PCT_TICKET_RELATIVO
FROM `acoes-378306.pmweb_silver.pedido` p
INNER JOIN `acoes-378306.pmweb_silver.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
GROUP BY PERMISSAO_EMAIL
ORDER BY TICKET_MEDIO DESC;


-- ----------------------------------------------------------------
-- ANÁLISE 4: Crescimento da base de clientes por ano
-- Monitora velocidade de aquisição de novos clientes
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `acoes-378306.pmweb_gold.crescimento_base_clientes`
OPTIONS (description = 'Novos clientes cadastrados por ano e mês')
AS
WITH base AS (
  SELECT
    EXTRACT(YEAR  FROM DATA_CADASTRO) AS ANO,
    EXTRACT(MONTH FROM DATA_CADASTRO) AS MES,
    FORMAT('%04d-%02d',
      EXTRACT(YEAR  FROM DATA_CADASTRO),
      EXTRACT(MONTH FROM DATA_CADASTRO)) AS ANO_MES
  FROM `acoes-378306.pmweb_silver.cadastro`
  WHERE DATA_CADASTRO IS NOT NULL
),
agrupado AS (
  SELECT
    ANO,
    MES,
    ANO_MES,
    COUNT(*) AS NOVOS_CLIENTES
  FROM base
  GROUP BY ANO, MES, ANO_MES
)
SELECT
  ANO,
  MES,
  ANO_MES,
  NOVOS_CLIENTES,
  SUM(NOVOS_CLIENTES) OVER (
    ORDER BY ANO, MES
  ) AS TOTAL_ACUMULADO
FROM agrupado;
