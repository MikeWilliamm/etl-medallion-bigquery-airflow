"""
================================================================
DAG: pmweb_pipeline
Descrição: Pipeline completo de integração de dados PMWEB
           Stage → Raw → Silver → Gold + Log de rodadas
Schedule: Diário à meia-noite (America/Sao_Paulo)
Projeto: acoes-378306
================================================================
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# ================================================================
# CONFIGURAÇÕES GERAIS
# ================================================================
PROJECT_ID  = "acoes-378306"
BUCKET      = "pmweb"
DATASET_STAGE  = "pmweb_stage"
DATASET_RAW    = "pmweb_raw"
DATASET_SILVER = "pmweb_silver"
DATASET_GOLD   = "pmweb_gold"

default_args = {
    "owner"           : "pmweb",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id          = "pmweb_pipeline",
    description     = "Pipeline PMWEB: Stage → Raw → Silver → Gold",
    default_args    = default_args,
    start_date      = days_ago(1),
    schedule        = "0 0 * * *",   # todo dia à meia-noite
    catchup         = False,
    tags            = ["pmweb", "bigquery", "etl"],
) as dag:

    # ================================================================
    # STAGE — Carga dos CSVs do GCS para o BigQuery
    # ================================================================

    load_stage_cadastro = GCSToBigQueryOperator(
        task_id              = "load_stage_cadastro",
        bucket               = BUCKET,
        source_objects       = ["CADASTRO/CADASTROS.csv"],
        destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_STAGE}.stg_cadastro",
        schema_fields=[
            {"name": "ID",              "type": "STRING", "mode": "NULLABLE"},
            {"name": "EMAIL",           "type": "STRING", "mode": "NULLABLE"},
            {"name": "NOME",            "type": "STRING", "mode": "NULLABLE"},
            {"name": "DT_NASC",         "type": "STRING", "mode": "NULLABLE"},
            {"name": "SEXO",            "type": "STRING", "mode": "NULLABLE"},
            {"name": "CADASTRO",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "CIDADE",          "type": "STRING", "mode": "NULLABLE"},
            {"name": "ESTADO",          "type": "STRING", "mode": "NULLABLE"},
            {"name": "RECEBE_EMAIL",    "type": "STRING", "mode": "NULLABLE"},
            {"name": "string_field_9",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "string_field_10", "type": "STRING", "mode": "NULLABLE"},
            {"name": "string_field_11", "type": "STRING", "mode": "NULLABLE"},
            {"name": "string_field_12", "type": "STRING", "mode": "NULLABLE"},
        ],
        field_delimiter      = ";",
        skip_leading_rows    = 1,
        write_disposition    = "WRITE_TRUNCATE",  # sobrescreve sempre
        create_disposition   = "CREATE_IF_NEEDED",
    )

    load_stage_pedido = GCSToBigQueryOperator(
        task_id              = "load_stage_pedido",
        bucket               = BUCKET,
        source_objects       = ["PEDIDO/PEDIDOS.csv"],
        destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_STAGE}.stg_pedido",
        schema_fields=[
            {"name": "COD_CLIENTE",      "type": "STRING", "mode": "NULLABLE"},
            {"name": "COD_PEDIDO",       "type": "STRING", "mode": "NULLABLE"},
            {"name": "CODIGO_PRODUTO",   "type": "STRING", "mode": "NULLABLE"},
            {"name": "DEPTO",            "type": "STRING", "mode": "NULLABLE"},
            {"name": "QUANTIDADE",       "type": "STRING", "mode": "NULLABLE"},
            {"name": "VALOR_UNITARIO",   "type": "STRING", "mode": "NULLABLE"},
            {"name": "QTD_PARCELAS",     "type": "STRING", "mode": "NULLABLE"},
            {"name": "DT_PEDIDO",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "MEIO_PAGTO",       "type": "STRING", "mode": "NULLABLE"},
            {"name": "STATUS_PAGAMENTO", "type": "STRING", "mode": "NULLABLE"},
        ],
        field_delimiter      = ";",
        skip_leading_rows    = 1,
        write_disposition    = "WRITE_TRUNCATE",
        create_disposition   = "CREATE_IF_NEEDED",
    )

    # ================================================================
    # RAW — Append com colunas de controle
    # ================================================================

    load_raw_cadastro = BigQueryInsertJobOperator(
        task_id    = "load_raw_cadastro",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.raw_cadastro`
                    SELECT
                        ID, EMAIL, NOME, DT_NASC, SEXO, CADASTRO, CIDADE, ESTADO, RECEBE_EMAIL,
                        CURRENT_TIMESTAMP() AS DT_CARGA,
                        'GCS:CADASTROS.csv'  AS ORIGEM,
                        GENERATE_UUID()      AS UUID_LINHA
                    FROM `{PROJECT_ID}.{DATASET_STAGE}.stg_cadastro`
                    WHERE ID IS NOT NULL
                      AND TRIM(ID) != ''
                      AND TRIM(ID) != 'ID'
                """,
                "useLegacySql": False,
            }
        },
    )

    load_raw_pedido = BigQueryInsertJobOperator(
        task_id    = "load_raw_pedido",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.raw_pedido`
                    SELECT
                        COD_CLIENTE, COD_PEDIDO, CODIGO_PRODUTO, DEPTO, QUANTIDADE,
                        VALOR_UNITARIO, QTD_PARCELAS, DT_PEDIDO, MEIO_PAGTO, STATUS_PAGAMENTO,
                        CURRENT_TIMESTAMP() AS DT_CARGA,
                        'GCS:PEDIDOS.csv'    AS ORIGEM,
                        GENERATE_UUID()      AS UUID_LINHA
                    FROM `{PROJECT_ID}.{DATASET_STAGE}.stg_pedido`
                    WHERE COD_CLIENTE IS NOT NULL
                      AND TRIM(COD_CLIENTE) != ''
                      AND TRIM(COD_CLIENTE) != 'COD_CLIENTE'
                """,
                "useLegacySql": False,
            }
        },
    )

    # ================================================================
    # LOG — Registra a rodada após o RAW
    # ================================================================

    log_cadastro = BigQueryInsertJobOperator(
        task_id    = "log_cadastro",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`
                    SELECT
                        (SELECT IFNULL(MAX(ID_RODADA), 0) + 1 FROM `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`),
                        CURRENT_TIMESTAMP(),
                        'raw_cadastro',
                        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_STAGE}.stg_cadastro`
                         WHERE ID IS NOT NULL AND TRIM(ID) != '' AND TRIM(ID) != 'ID'),
                        'GCS:CADASTROS.csv',
                        'SUCESSO',
                        NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    log_pedido = BigQueryInsertJobOperator(
        task_id    = "log_pedido",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`
                    SELECT
                        (SELECT IFNULL(MAX(ID_RODADA), 0) + 1 FROM `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`),
                        CURRENT_TIMESTAMP(),
                        'raw_pedido',
                        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_STAGE}.stg_pedido`
                         WHERE COD_CLIENTE IS NOT NULL AND TRIM(COD_CLIENTE) != '' AND TRIM(COD_CLIENTE) != 'COD_CLIENTE'),
                        'GCS:PEDIDOS.csv',
                        'SUCESSO',
                        NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    # ================================================================
    # SILVER — Limpeza e tipagem
    # ================================================================

    load_silver_cadastro = BigQueryInsertJobOperator(
        task_id    = "load_silver_cadastro",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_SILVER}.cadastro`
                    OPTIONS (description = 'Silver: clientes limpos — última carga')
                    AS
                    WITH ultima_carga AS (
                        SELECT MAX(DT_CARGA) AS DT_CARGA
                        FROM `{PROJECT_ID}.{DATASET_RAW}.raw_cadastro`
                    ),
                    ufs_validas AS (
                        SELECT uf FROM UNNEST([
                            'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
                            'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
                            'RS','RO','RR','SC','SP','SE','TO'
                        ]) AS uf
                    )
                    SELECT
                        CAST(r.ID AS INT64)                                        AS ID_CLIENTE,
                        LOWER(TRIM(r.EMAIL))                                       AS EMAIL,
                        INITCAP(TRIM(r.NOME))                                      AS NOME,
                        SAFE.PARSE_DATE('%d/%m/%Y', TRIM(r.DT_NASC))              AS DATA_NASCIMENTO,
                        DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%d/%m/%Y', TRIM(r.DT_NASC)), YEAR) AS IDADE,
                        TRIM(r.SEXO)                                               AS SEXO,
                        SAFE.PARSE_DATE('%d/%m/%Y', TRIM(r.CADASTRO))             AS DATA_CADASTRO,
                        CASE
                            WHEN TRIM(r.CIDADE) = ''    THEN NULL
                            WHEN TRIM(r.CIDADE) = '.'   THEN NULL
                            WHEN REGEXP_CONTAINS(TRIM(r.CIDADE), r'^\\d+$') THEN NULL
                            ELSE UPPER(TRIM(r.CIDADE))
                        END                                                        AS CIDADE,
                        CASE WHEN u.uf IS NOT NULL THEN TRIM(r.ESTADO) ELSE NULL END AS UF,
                        CAST(r.RECEBE_EMAIL AS INT64) = 1                         AS PERMISSAO_RECEBE_EMAIL,
                        r.DT_CARGA
                    FROM `{PROJECT_ID}.{DATASET_RAW}.raw_cadastro` r
                    LEFT JOIN ufs_validas u ON u.uf = TRIM(r.ESTADO)
                    INNER JOIN ultima_carga uc ON r.DT_CARGA = uc.DT_CARGA
                    WHERE TRIM(r.ID) IS NOT NULL AND TRIM(r.ID) != ''
                """,
                "useLegacySql": False,
            }
        },
    )

    load_silver_pedido = BigQueryInsertJobOperator(
        task_id    = "load_silver_pedido",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                    OPTIONS (description = 'Silver: pedidos limpos — última carga')
                    AS
                    WITH ultima_carga AS (
                        SELECT MAX(DT_CARGA) AS DT_CARGA
                        FROM `{PROJECT_ID}.{DATASET_RAW}.raw_pedido`
                    )
                    SELECT
                        CAST(COD_CLIENTE    AS INT64)                              AS ID_CLIENTE,
                        CAST(COD_PEDIDO     AS INT64)                              AS ID_PEDIDO,
                        CAST(CODIGO_PRODUTO AS INT64)                              AS ID_PRODUTO,
                        UPPER(TRIM(DEPTO))                                         AS DEPARTAMENTO,
                        CAST(QUANTIDADE     AS INT64)                              AS QUANTIDADE,
                        CAST(VALOR_UNITARIO AS FLOAT64)                            AS VALOR_UNITARIO,
                        CASE
                            WHEN QTD_PARCELAS IS NULL            THEN 1
                            WHEN TRIM(QTD_PARCELAS) = ''         THEN 1
                            WHEN CAST(QTD_PARCELAS AS INT64) = 0 THEN 1
                            ELSE CAST(QTD_PARCELAS AS INT64)
                        END                                                        AS PARCELAS,
                        PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))                   AS DATA_PEDIDO,
                        UPPER(TRIM(MEIO_PAGTO))                                    AS MEIO_PAGAMENTO,
                        UPPER(TRIM(STATUS_PAGAMENTO))                              AS STATUS_PAGAMENTO,
                        CAST(QUANTIDADE AS INT64) * CAST(VALOR_UNITARIO AS FLOAT64) AS VALOR_TOTAL,
                        CASE
                            WHEN QTD_PARCELAS IS NULL            THEN FALSE
                            WHEN TRIM(QTD_PARCELAS) = ''         THEN FALSE
                            WHEN CAST(QTD_PARCELAS AS INT64) <= 1 THEN FALSE
                            ELSE TRUE
                        END                                                        AS PARCELADO,
                        EXTRACT(YEAR  FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) AS ANO,
                        EXTRACT(MONTH FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) AS MES,
                        CASE
                            WHEN EXTRACT(MONTH FROM PARSE_DATE('%d/%m/%Y', TRIM(DT_PEDIDO))) <= 6
                            THEN 1 ELSE 2
                        END                                                        AS SEMESTRE,
                        r.DT_CARGA
                    FROM `{PROJECT_ID}.{DATASET_RAW}.raw_pedido` r
                    INNER JOIN ultima_carga uc ON r.DT_CARGA = uc.DT_CARGA
                    WHERE UPPER(TRIM(STATUS_PAGAMENTO)) IN ('CONFIRMADO', 'CANCELADO')
                      AND CAST(VALOR_UNITARIO AS FLOAT64) > 0
                      AND NOT (
                            UPPER(TRIM(STATUS_PAGAMENTO)) = 'CONFIRMADO'
                            AND CAST(VALOR_UNITARIO AS FLOAT64) = 0
                      )
                """,
                "useLegacySql": False,
            }
        },
    )

    # ================================================================
    # GOLD — Tabelas analíticas
    # ================================================================

    gold_pedidos_semestre = BigQueryInsertJobOperator(
        task_id    = "gold_pedidos_semestre",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.pedidos_por_cliente_semestre`
                    PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
                    CLUSTER BY ID_CLIENTE
                    OPTIONS (description = 'Qtd de pedidos por cliente por semestre/ano — apenas parcelados')
                    AS
                    WITH clientes_parcelados AS (
                        SELECT DISTINCT ID_CLIENTE
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                        WHERE STATUS_PAGAMENTO = 'CONFIRMADO' AND PARCELADO = TRUE
                    )
                    SELECT
                        p.ID_CLIENTE, c.NOME, p.ANO, p.SEMESTRE,
                        CONCAT(CAST(p.ANO AS STRING), ' - S', CAST(p.SEMESTRE AS STRING)) AS ANO_SEMESTRE,
                        COUNT(DISTINCT p.ID_PEDIDO)   AS QTD_PEDIDOS,
                        ROUND(SUM(p.VALOR_TOTAL), 2)  AS VALOR_TOTAL_PERIODO
                    FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido` p
                    INNER JOIN clientes_parcelados cp ON cp.ID_CLIENTE = p.ID_CLIENTE
                    LEFT  JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
                    WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
                    GROUP BY p.ID_CLIENTE, c.NOME, p.ANO, p.SEMESTRE
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_ticket_medio = BigQueryInsertJobOperator(
        task_id    = "gold_ticket_medio",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.ticket_medio_cliente_ano_mes`
                    PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
                    CLUSTER BY ID_CLIENTE
                    OPTIONS (description = 'Ticket médio por cliente por ano e mês')
                    AS
                    SELECT
                        p.ID_CLIENTE, c.NOME, p.ANO, p.MES,
                        FORMAT('%04d-%02d', p.ANO, p.MES)                       AS ANO_MES,
                        COUNT(DISTINCT p.ID_PEDIDO)                             AS QTD_PEDIDOS,
                        ROUND(SUM(p.VALOR_TOTAL), 2)                            AS VALOR_TOTAL,
                        ROUND(SUM(p.VALOR_TOTAL) / COUNT(DISTINCT p.ID_PEDIDO), 2) AS TICKET_MEDIO
                    FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido` p
                    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
                    WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
                    GROUP BY p.ID_CLIENTE, c.NOME, p.ANO, p.MES
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_intervalo = BigQueryInsertJobOperator(
        task_id    = "gold_intervalo",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.intervalo_medio_compras`
                    PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
                    CLUSTER BY ID_CLIENTE
                    OPTIONS (description = 'Intervalo médio em dias entre compras por cliente por ano')
                    AS
                    WITH datas_distintas AS (
                        SELECT DISTINCT ID_CLIENTE, ANO, DATA_PEDIDO
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                        WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
                    ),
                    com_lag AS (
                        SELECT
                            ID_CLIENTE, ANO, DATA_PEDIDO,
                            LAG(DATA_PEDIDO) OVER (PARTITION BY ID_CLIENTE ORDER BY DATA_PEDIDO) AS DATA_COMPRA_ANTERIOR
                        FROM datas_distintas
                    ),
                    intervalos AS (
                        SELECT ID_CLIENTE, ANO,
                               DATE_DIFF(DATA_PEDIDO, DATA_COMPRA_ANTERIOR, DAY) AS DIAS_ENTRE_COMPRAS
                        FROM com_lag
                        WHERE DATA_COMPRA_ANTERIOR IS NOT NULL
                    )
                    SELECT
                        i.ID_CLIENTE, c.NOME, i.ANO,
                        COUNT(*)                             AS QTD_INTERVALOS,
                        ROUND(AVG(i.DIAS_ENTRE_COMPRAS), 1) AS INTERVALO_MEDIO_DIAS,
                        MIN(i.DIAS_ENTRE_COMPRAS)            AS MENOR_INTERVALO_DIAS,
                        MAX(i.DIAS_ENTRE_COMPRAS)            AS MAIOR_INTERVALO_DIAS
                    FROM intervalos i
                    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = i.ID_CLIENTE
                    GROUP BY i.ID_CLIENTE, c.NOME, i.ANO
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_tiers = BigQueryInsertJobOperator(
        task_id    = "gold_tiers",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.tiers_clientes`
                    PARTITION BY RANGE_BUCKET(ANO, GENERATE_ARRAY(2019, 2025, 1))
                    CLUSTER BY ID_CLIENTE, TIER
                    OPTIONS (description = 'Tier mensal de cada cliente por valor de compras')
                    AS
                    WITH valor_mensal AS (
                        SELECT
                            ID_CLIENTE, ANO, MES,
                            FORMAT('%04d-%02d', ANO, MES) AS ANO_MES,
                            ROUND(SUM(VALOR_TOTAL), 2)    AS VALOR_MES
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                        WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
                        GROUP BY ID_CLIENTE, ANO, MES
                    )
                    SELECT
                        vm.ID_CLIENTE, c.NOME, vm.ANO, vm.MES, vm.ANO_MES, vm.VALOR_MES,
                        CASE
                            WHEN vm.VALOR_MES <  1000 THEN 'Básico'
                            WHEN vm.VALOR_MES <  2000 THEN 'Prata'
                            WHEN vm.VALOR_MES <  5000 THEN 'Ouro'
                            ELSE                           'Super'
                        END AS TIER
                    FROM valor_mensal vm
                    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = vm.ID_CLIENTE
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_som_papelaria = BigQueryInsertJobOperator(
        task_id    = "gold_som_papelaria",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.comparativo_som_papelaria`
                    OPTIONS (description = 'Comparativo % SOM e PAPELARIA 2019 x 2020')
                    AS
                    WITH base AS (
                        SELECT DEPARTAMENTO, ANO, ROUND(SUM(VALOR_TOTAL), 2) AS TOTAL_VENDAS
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                        WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
                          AND DEPARTAMENTO IN ('SOM', 'PAPELARIA')
                          AND ANO IN (2019, 2020)
                        GROUP BY DEPARTAMENTO, ANO
                    ),
                    pivot AS (
                        SELECT
                            DEPARTAMENTO,
                            SUM(CASE WHEN ANO = 2019 THEN TOTAL_VENDAS ELSE 0 END) AS TOTAL_2019,
                            SUM(CASE WHEN ANO = 2020 THEN TOTAL_VENDAS ELSE 0 END) AS TOTAL_2020
                        FROM base GROUP BY DEPARTAMENTO
                    )
                    SELECT
                        DEPARTAMENTO, TOTAL_2019, TOTAL_2020,
                        ROUND(TOTAL_2020 - TOTAL_2019, 2)                                    AS VARIACAO_ABSOLUTA,
                        ROUND(SAFE_DIVIDE(TOTAL_2020 - TOTAL_2019, TOTAL_2019) * 100, 2)     AS VARIACAO_PERCENTUAL
                    FROM pivot
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_cancelamento = BigQueryInsertJobOperator(
        task_id    = "gold_cancelamento",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.cancelamento_por_departamento`
                    OPTIONS (description = 'Taxa de cancelamento por departamento')
                    AS
                    SELECT
                        DEPARTAMENTO,
                        COUNT(*)                                                      AS TOTAL_PEDIDOS,
                        COUNTIF(STATUS_PAGAMENTO = 'CONFIRMADO')                      AS CONFIRMADOS,
                        COUNTIF(STATUS_PAGAMENTO = 'CANCELADO')                       AS CANCELADOS,
                        ROUND(100 * COUNTIF(STATUS_PAGAMENTO = 'CANCELADO') / COUNT(*), 1) AS PCT_CANCELAMENTO,
                        ROUND(SUM(CASE WHEN STATUS_PAGAMENTO = 'CONFIRMADO' THEN VALOR_TOTAL ELSE 0 END), 2) AS RECEITA_CONFIRMADA,
                        ROUND(SUM(CASE WHEN STATUS_PAGAMENTO = 'CANCELADO'  THEN VALOR_TOTAL ELSE 0 END), 2) AS RECEITA_PERDIDA
                    FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                    GROUP BY DEPARTAMENTO
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_dia_semana = BigQueryInsertJobOperator(
        task_id    = "gold_dia_semana",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.pedidos_por_dia_semana`
                    OPTIONS (description = 'Distribuição de pedidos por dia da semana')
                    AS
                    SELECT
                        FORMAT_DATE('%A', DATA_PEDIDO)      AS DIA_SEMANA,
                        EXTRACT(DAYOFWEEK FROM DATA_PEDIDO) AS NUM_DIA,
                        COUNT(DISTINCT ID_PEDIDO)           AS QTD_PEDIDOS,
                        ROUND(SUM(VALOR_TOTAL), 2)          AS RECEITA_TOTAL,
                        ROUND(AVG(VALOR_TOTAL), 2)          AS TICKET_MEDIO,
                        ROUND(100 * COUNT(DISTINCT ID_PEDIDO) / SUM(COUNT(DISTINCT ID_PEDIDO)) OVER(), 1) AS PCT_DO_TOTAL
                    FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                    WHERE STATUS_PAGAMENTO = 'CONFIRMADO'
                    GROUP BY DIA_SEMANA, NUM_DIA
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_email = BigQueryInsertJobOperator(
        task_id    = "gold_email",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.impacto_permissao_email`
                    OPTIONS (description = 'Impacto da permissão de e-mail no ticket médio')
                    AS
                    SELECT
                        CASE WHEN c.PERMISSAO_RECEBE_EMAIL THEN 'Aceita e-mail' ELSE 'Não aceita e-mail' END AS PERMISSAO_EMAIL,
                        COUNT(DISTINCT p.ID_CLIENTE)  AS QTD_CLIENTES,
                        COUNT(DISTINCT p.ID_PEDIDO)   AS QTD_PEDIDOS,
                        ROUND(SUM(p.VALOR_TOTAL), 2)  AS RECEITA_TOTAL,
                        ROUND(AVG(p.VALOR_TOTAL), 2)  AS TICKET_MEDIO,
                        ROUND(100 * AVG(p.VALOR_TOTAL) / SUM(AVG(p.VALOR_TOTAL)) OVER(), 1) AS PCT_TICKET_RELATIVO
                    FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido` p
                    INNER JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = p.ID_CLIENTE
                    WHERE p.STATUS_PAGAMENTO = 'CONFIRMADO'
                    GROUP BY PERMISSAO_EMAIL
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_retencao = BigQueryInsertJobOperator(
        task_id    = "gold_retencao",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.retencao_clientes`
                    OPTIONS (description = 'Retenção de clientes entre 2019 e 2020')
                    AS
                    WITH anos_por_cliente AS (
                        SELECT
                            ID_CLIENTE,
                            COUNTIF(ANO = 2019) > 0 AS comprou_2019,
                            COUNTIF(ANO = 2020) > 0 AS comprou_2020
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.pedido`
                        WHERE STATUS_PAGAMENTO = 'CONFIRMADO' AND ANO IN (2019, 2020)
                        GROUP BY ID_CLIENTE
                    )
                    SELECT
                        c.NOME, a.ID_CLIENTE,
                        CASE
                            WHEN comprou_2019 AND comprou_2020 THEN 'Recorrente (2019 e 2020)'
                            WHEN comprou_2019                  THEN 'Apenas 2019'
                            WHEN comprou_2020                  THEN 'Apenas 2020'
                        END AS PERFIL_RETENCAO,
                        comprou_2019 AS COMPROU_2019,
                        comprou_2020 AS COMPROU_2020
                    FROM anos_por_cliente a
                    LEFT JOIN `{PROJECT_ID}.{DATASET_SILVER}.cadastro` c ON c.ID_CLIENTE = a.ID_CLIENTE
                """,
                "useLegacySql": False,
            }
        },
    )

    gold_crescimento = BigQueryInsertJobOperator(
        task_id    = "gold_crescimento",
        project_id = PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.crescimento_base_clientes`
                    OPTIONS (description = 'Novos clientes cadastrados por ano e mês')
                    AS
                    WITH base AS (
                        SELECT
                            EXTRACT(YEAR  FROM DATA_CADASTRO) AS ANO,
                            EXTRACT(MONTH FROM DATA_CADASTRO) AS MES,
                            FORMAT('%04d-%02d', EXTRACT(YEAR FROM DATA_CADASTRO), EXTRACT(MONTH FROM DATA_CADASTRO)) AS ANO_MES
                        FROM `{PROJECT_ID}.{DATASET_SILVER}.cadastro`
                        WHERE DATA_CADASTRO IS NOT NULL
                    ),
                    agrupado AS (
                        SELECT ANO, MES, ANO_MES, COUNT(*) AS NOVOS_CLIENTES
                        FROM base
                        GROUP BY ANO, MES, ANO_MES
                    )
                    SELECT
                        ANO, MES, ANO_MES, NOVOS_CLIENTES,
                        SUM(NOVOS_CLIENTES) OVER (ORDER BY ANO, MES) AS TOTAL_ACUMULADO
                    FROM agrupado
                """,
                "useLegacySql": False,
            }
        },
    )

    # ================================================================
    # LOG GOLD — Registra a carga de cada tabela Gold
    # ================================================================

    def make_log_gold(table_name):
        return BigQueryInsertJobOperator(
            task_id    = f"log_gold_{table_name}",
            project_id = PROJECT_ID,
            configuration={
                "query": {
                    "query": f"""
                        INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`
                        SELECT
                            (SELECT IFNULL(MAX(ID_RODADA), 0) + 1 FROM `{PROJECT_ID}.{DATASET_RAW}.log_rodadas`),
                            CURRENT_TIMESTAMP(),
                            'gold.{table_name}',
                            (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_GOLD}.{table_name}`),
                            'pmweb_silver',
                            'SUCESSO',
                            NULL
                    """,
                    "useLegacySql": False,
                }
            },
        )

    log_gold_pedidos_semestre  = make_log_gold("pedidos_por_cliente_semestre")
    log_gold_ticket_medio      = make_log_gold("ticket_medio_cliente_ano_mes")
    log_gold_intervalo         = make_log_gold("intervalo_medio_compras")
    log_gold_tiers             = make_log_gold("tiers_clientes")
    log_gold_som_papelaria     = make_log_gold("comparativo_som_papelaria")
    log_gold_cancelamento      = make_log_gold("cancelamento_por_departamento")
    log_gold_dia_semana        = make_log_gold("pedidos_por_dia_semana")
    log_gold_email             = make_log_gold("impacto_permissao_email")
    log_gold_retencao          = make_log_gold("retencao_clientes")
    log_gold_crescimento       = make_log_gold("crescimento_base_clientes")

    # ================================================================
    # DEPENDÊNCIAS — Ordem de execução
    # ================================================================

    # Stage (paralelo)
    [load_stage_cadastro, load_stage_pedido]

    # Raw (cada um após seu stage)
    load_stage_cadastro >> load_raw_cadastro >> log_cadastro
    load_stage_pedido   >> load_raw_pedido   >> log_pedido

    # Silver (após log de ambos)
    [log_cadastro, log_pedido] >> load_silver_cadastro
    [log_cadastro, log_pedido] >> load_silver_pedido

    # Gold + Log Gold (todas após silver completa)
    [load_silver_cadastro, load_silver_pedido] >> gold_pedidos_semestre >> log_gold_pedidos_semestre
    [load_silver_cadastro, load_silver_pedido] >> gold_ticket_medio     >> log_gold_ticket_medio
    [load_silver_cadastro, load_silver_pedido] >> gold_intervalo        >> log_gold_intervalo
    [load_silver_cadastro, load_silver_pedido] >> gold_tiers            >> log_gold_tiers
    [load_silver_cadastro, load_silver_pedido] >> gold_som_papelaria    >> log_gold_som_papelaria
    [load_silver_cadastro, load_silver_pedido] >> gold_cancelamento     >> log_gold_cancelamento
    [load_silver_cadastro, load_silver_pedido] >> gold_dia_semana       >> log_gold_dia_semana
    [load_silver_cadastro, load_silver_pedido] >> gold_email            >> log_gold_email
    [load_silver_cadastro, load_silver_pedido] >> gold_retencao         >> log_gold_retencao
    [load_silver_cadastro, load_silver_pedido] >> gold_crescimento      >> log_gold_crescimento
