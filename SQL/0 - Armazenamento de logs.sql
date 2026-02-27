-- ================================================================
-- 0. LOG DE RODADAS
-- Registra cada execução do pipeline com contadores
-- ================================================================

CREATE OR REPLACE TABLE `acoes-378306.pmweb_raw.log_rodadas` (
  ID_RODADA    INT64       OPTIONS(description='Sequencial da rodada'),
  DATA_RODADA  TIMESTAMP   OPTIONS(description='Timestamp da execução'),
  TABELA       STRING      OPTIONS(description='Tabela carregada'),
  QTD_INCLUIDO INT64       OPTIONS(description='Registros inseridos no append'),
  ORIGEM       STRING      OPTIONS(description='Arquivo de origem'),
  STATUS       STRING      OPTIONS(description='SUCESSO ou ERRO'),
  OBSERVACAO   STRING      OPTIONS(description='Detalhes adicionais')
)
OPTIONS (description = 'Log de todas as rodadas de integração');
