-- ================================================================
-- 6_validacoes.sql
-- Validações realizadas em cada camada do pipeline
-- Projeto: acoes-378306 | pmweb-pipeline
-- ================================================================
-- Cada seção contém as queries de validação executadas após a carga
-- de cada camada, com os resultados esperados documentados.
-- ================================================================


-- ================================================================
-- CAMADA STAGE
-- Validação básica de volume após bq load
-- ================================================================

-- Esperado: 35.736 linhas (incluindo vazias — limpeza ocorre na Silver)
SELECT COUNT(*) AS total_stage_cadastro
FROM `acoes-378306.pmweb_stage.stg_cadastro`;

-- Esperado: 134.396 linhas
SELECT COUNT(*) AS total_stage_pedido
FROM `acoes-378306.pmweb_stage.stg_pedido`;


-- ================================================================
-- CAMADA RAW
-- Validação de volume e controle de carga
-- ================================================================

-- Esperado: mesmo volume do stage (sem filtros na raw)
SELECT COUNT(*) AS total_raw_cadastro
FROM `acoes-378306.pmweb_raw.raw_cadastro`;

SELECT COUNT(*) AS total_raw_pedido
FROM `acoes-378306.pmweb_raw.raw_pedido`;

-- Verificar se o log foi registrado corretamente
-- Esperado: ao menos 2 linhas (uma por tabela carregada)
SELECT *
FROM `acoes-378306.pmweb_raw.log_rodadas`
ORDER BY DATA_RODADA DESC;


-- ================================================================
-- CAMADA SILVER
-- Validação completa de volume, qualidade e integridade
-- ================================================================

-- 1. Volume final de cada tabela
-- Esperado: cadastro ~32.477 | pedido ~102.211
SELECT 'cadastro' AS tabela, COUNT(*) AS total
FROM `acoes-378306.pmweb_silver.cadastro`
UNION ALL
SELECT 'pedido', COUNT(*)
FROM `acoes-378306.pmweb_silver.pedido`;

-- 2. Distribuição de status na silver de pedido
-- Esperado: apenas CONFIRMADO e CANCELADO — PENDENTE não deve aparecer
SELECT STATUS_PAGAMENTO, COUNT(*) AS qtd
FROM `acoes-378306.pmweb_silver.pedido`
GROUP BY 1
ORDER BY 2 DESC;
-- Resultado obtido:
-- CONFIRMADO: 78.626
-- CANCELADO:  23.585

-- 3. Checar nulos críticos no cadastro
-- Esperado: cliente_sem_id = 0 (nenhum cliente sem identificador)
--           sem_dt_nasc = 87 (datas inválidas convertidas para NULL pelo SAFE.PARSE_DATE)
--           sem_uf e sem_cidade ~13k (nulos reais da fonte + valores inválidos limpos)
SELECT
  COUNTIF(ID_CLIENTE IS NULL)      AS cliente_sem_id,
  COUNTIF(DATA_NASCIMENTO IS NULL) AS sem_dt_nasc,
  COUNTIF(UF IS NULL)              AS sem_uf,
  COUNTIF(CIDADE IS NULL)          AS sem_cidade
FROM `acoes-378306.pmweb_silver.cadastro`;
-- Resultado obtido:
-- cliente_sem_id: 0
-- sem_dt_nasc:    87
-- sem_uf:         13.401
-- sem_cidade:     13.116

-- 4. Checar nulos e valores inválidos no pedido
-- Esperado: todos zerados — nenhum pedido sem cliente, data ou com valor inválido
SELECT
  COUNTIF(ID_CLIENTE IS NULL)  AS pedido_sem_cliente,
  COUNTIF(DATA_PEDIDO IS NULL) AS pedido_sem_data,
  COUNTIF(VALOR_TOTAL <= 0)    AS pedido_valor_invalido
FROM `acoes-378306.pmweb_silver.pedido`;
-- Resultado obtido:
-- pedido_sem_cliente:  0
-- pedido_sem_data:     0
-- pedido_valor_invalido: 0


-- ================================================================
-- CAMADA GOLD — Item 4 (Consolidações obrigatórias)
-- Validação unificada em uma única consulta
-- ================================================================

SELECT
  -- Gold 1: clientes parcelados
  -- Esperado: ~13.411
  (SELECT COUNT(DISTINCT ID_CLIENTE)
   FROM `acoes-378306.pmweb_gold.pedidos_por_cliente_semestre`)        AS g1_clientes_parcelados,

  -- Gold 2: ticket médio
  -- Esperado: nulos=0, inválidos=0, média geral ~R$24.273
  (SELECT COUNTIF(TICKET_MEDIO IS NULL)
   FROM `acoes-378306.pmweb_gold.ticket_medio_cliente_ano_mes`)        AS g2_ticket_nulos,

  (SELECT COUNTIF(TICKET_MEDIO <= 0)
   FROM `acoes-378306.pmweb_gold.ticket_medio_cliente_ano_mes`)        AS g2_ticket_invalidos,

  (SELECT ROUND(AVG(TICKET_MEDIO), 2)
   FROM `acoes-378306.pmweb_gold.ticket_medio_cliente_ano_mes`)        AS g2_ticket_media_geral,

  -- Gold 3: intervalo médio
  -- Esperado: ~1.630 (apenas clientes com mais de 1 data de compra)
  (SELECT COUNT(DISTINCT ID_CLIENTE)
   FROM `acoes-378306.pmweb_gold.intervalo_medio_compras`)             AS g3_clientes_com_intervalo,

  -- Gold 4: tiers
  -- Esperado: Básico=570 | Prata=729 | Ouro=2.610 | Super=12.753
  (SELECT STRING_AGG(
      CONCAT(TIER, ': ', CAST(QTD AS STRING)),
      ' | ' ORDER BY TIER)
   FROM (
     SELECT TIER, COUNT(DISTINCT ID_CLIENTE) AS QTD
     FROM `acoes-378306.pmweb_gold.tiers_clientes`
     GROUP BY TIER
   ))                                                                   AS g4_distribuicao_tiers,

  -- Gold 5: comparativo SOM e PAPELARIA
  -- Esperado: SOM +407% | PAPELARIA +522%
  (SELECT STRING_AGG(
      CONCAT(DEPARTAMENTO,
             ' | 2019: R$', CAST(TOTAL_2019 AS STRING),
             ' | 2020: R$', CAST(TOTAL_2020 AS STRING),
             ' | var%: +', CAST(VARIACAO_PERCENTUAL AS STRING), '%'),
      ' || ' ORDER BY DEPARTAMENTO)
   FROM `acoes-378306.pmweb_gold.comparativo_som_papelaria`)           AS g5_comparativo;

-- Resultado obtido:
-- g1_clientes_parcelados:  13.411
-- g2_ticket_nulos:         0
-- g2_ticket_invalidos:     0
-- g2_ticket_media_geral:   24.273,39
-- g3_clientes_com_intervalo: 1.630
-- g4_distribuicao_tiers:   Básico: 570 | Ouro: 2.610 | Prata: 729 | Super: 12.753
-- g5_comparativo:
--   PAPELARIA | 2019: R$116.131,47 | 2020: R$722.752,63 | var%: +522,36%
--   SOM       | 2019: R$301.553,77 | 2020: R$1.528.864,9 | var%: +407,0%


-- ================================================================
-- CAMADA GOLD — Item 2b (Análises exploratórias)
-- Validação unificada em uma única consulta
-- ================================================================

SELECT
  -- Análise 1: Cancelamento por departamento
  -- Esperado: DECORAÇÃO e BRINQUEDOS no topo (~24-25%), SOM menor (~19%)
  (SELECT STRING_AGG(
      CONCAT(DEPARTAMENTO, ': ', CAST(PCT_CANCELAMENTO AS STRING), '%'),
      ' | ' ORDER BY PCT_CANCELAMENTO DESC)
   FROM `acoes-378306.pmweb_gold.cancelamento_por_departamento`)       AS a1_cancelamento_por_depto,

  -- Análise 2: Dia da semana
  -- Esperado: Friday com maior volume, Tuesday com menor
  (SELECT STRING_AGG(
      CONCAT(DIA_SEMANA, ': ', CAST(QTD_PEDIDOS AS STRING)),
      ' | ' ORDER BY NUM_DIA)
   FROM `acoes-378306.pmweb_gold.pedidos_por_dia_semana`)              AS a2_pedidos_por_dia,

  (SELECT CONCAT('Maior: ', DIA_SEMANA, ' (', CAST(QTD_PEDIDOS AS STRING), ')')
   FROM `acoes-378306.pmweb_gold.pedidos_por_dia_semana`
   ORDER BY QTD_PEDIDOS DESC LIMIT 1)                                  AS a2_dia_maior_volume,

  (SELECT CONCAT('Menor: ', DIA_SEMANA, ' (', CAST(QTD_PEDIDOS AS STRING), ')')
   FROM `acoes-378306.pmweb_gold.pedidos_por_dia_semana`
   ORDER BY QTD_PEDIDOS ASC LIMIT 1)                                   AS a2_dia_menor_volume,

  -- Análise 3: Permissão de e-mail
  -- Esperado: clientes opt-in com ticket ~19% maior
  (SELECT STRING_AGG(
      CONCAT(PERMISSAO_EMAIL, ': R$', CAST(TICKET_MEDIO AS STRING)),
      ' | ' ORDER BY TICKET_MEDIO DESC)
   FROM `acoes-378306.pmweb_gold.impacto_permissao_email`)             AS a3_ticket_por_permissao,

  (SELECT ROUND(
      100 * (MAX(TICKET_MEDIO) - MIN(TICKET_MEDIO)) / MIN(TICKET_MEDIO), 1)
   FROM `acoes-378306.pmweb_gold.impacto_permissao_email`)             AS a3_diferenca_pct_ticket,

  -- Análise 4: Crescimento base de clientes
  -- Esperado: 2019 maior, desaceleração em 2020 e 2021, total acumulado = 32.477
  (SELECT STRING_AGG(
      CONCAT(CAST(ANO AS STRING), ': ', CAST(SUM_CLIENTES AS STRING)),
      ' | ' ORDER BY ANO)
   FROM (
     SELECT ANO, SUM(NOVOS_CLIENTES) AS SUM_CLIENTES
     FROM `acoes-378306.pmweb_gold.crescimento_base_clientes`
     GROUP BY ANO
   ))                                                                   AS a4_novos_por_ano,

  (SELECT CAST(MAX(TOTAL_ACUMULADO) AS STRING)
   FROM `acoes-378306.pmweb_gold.crescimento_base_clientes`)           AS a4_total_acumulado;