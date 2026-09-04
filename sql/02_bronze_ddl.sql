-- =====================================================================
-- Camada Bronze — DDL
--
-- Duas tabelas, e só duas: o inventário dos objetos ingeridos e os
-- registros como a fonte os entregou. Nenhum campo é extraído aqui —
-- todo o parse acontece em 03_bronze_to_silver.sql, à vista.
--
-- Regra anti-swamp: nada entra em bronze.registro sem uma linha
-- correspondente em bronze.arquivo. A FK abaixo é o que declara isso.
--
-- Nota de portabilidade: em PostgreSQL os CHECK ficam dentro do
-- CREATE TABLE (ver ddl_v3_1_mvp.sql). O Databricks só aceita PRIMARY
-- KEY e FOREIGN KEY na definição da tabela — os CHECK entram por
-- ALTER TABLE, depois. A regra imposta é a mesma; muda a sintaxe.
-- =====================================================================

USE CATALOG scapegoat;

-- ---------------------------------------------------------------------
-- bronze.arquivo — um objeto do Volume, com sua proveniência
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.arquivo (
  arquivo_id      BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
                  COMMENT 'identidade do objeto ingerido',
  caso_slug       STRING NOT NULL
                  COMMENT 'episódio a que o arquivo pertence',
  fonte           STRING NOT NULL
                  COMMENT 'coletor que produziu o arquivo: graphql | consolidado',
  uri             STRING NOT NULL
                  COMMENT 'caminho completo no Volume, no momento da ingestão',
  formato         STRING NOT NULL
                  COMMENT 'json | jsonl',
  sha256          STRING NOT NULL
                  COMMENT 'hash do conteúdo — é o que torna a ingestão idempotente',
  bytes           BIGINT NOT NULL,
  consulta        STRING
                  COMMENT 'consulta literal da coleta; NULL quando não recuperável (ver §7.3)',
  periodo_inicio  DATE,
  periodo_fim     DATE,
  ingerido_em     TIMESTAMP NOT NULL,

  CONSTRAINT arquivo_pk PRIMARY KEY (arquivo_id)
)
COMMENT 'Inventário dos objetos brutos ingeridos. Uma linha por arquivo do Volume.';

-- Regras de domínio. Impostas pelo Delta na escrita — um sha256
-- malformado ou um período invertido fazem a gravação falhar.
ALTER TABLE bronze.arquivo ADD CONSTRAINT arquivo_bytes_positivo
  CHECK (bytes > 0);

ALTER TABLE bronze.arquivo ADD CONSTRAINT arquivo_sha256_formato
  CHECK (sha256 RLIKE '^[0-9a-f]{64}$');

ALTER TABLE bronze.arquivo ADD CONSTRAINT arquivo_formato_conhecido
  CHECK (formato IN ('json', 'jsonl'));

ALTER TABLE bronze.arquivo ADD CONSTRAINT arquivo_periodo_ordenado
  CHECK (periodo_inicio IS NULL OR periodo_fim IS NULL
         OR periodo_inicio <= periodo_fim);

-- ---------------------------------------------------------------------
-- bronze.registro — uma postagem, exatamente como a fonte entregou
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.registro (
  arquivo_id   BIGINT NOT NULL
               COMMENT 'de qual arquivo este registro veio',
  linha        INT NOT NULL
               COMMENT 'posição dentro do arquivo, começando em 1',
  payload      STRING NOT NULL
               COMMENT 'JSON da postagem, sem seleção de campos e sem renomeação',
  ingerido_em  TIMESTAMP NOT NULL,

  CONSTRAINT registro_arquivo_fk
      FOREIGN KEY (arquivo_id) REFERENCES bronze.arquivo
)
COMMENT 'Registros brutos, fiéis à fonte. Todo parse acontece a jusante, na Silver.';

ALTER TABLE bronze.registro ADD CONSTRAINT registro_linha_positiva
  CHECK (linha >= 1);

ALTER TABLE bronze.registro ADD CONSTRAINT registro_payload_e_json
  CHECK (payload LIKE '{%');

-- ---------------------------------------------------------------------
-- Nota sobre unicidade
--
-- Delta não impõe UNIQUE. A unicidade de sha256 é garantida pelo
-- notebook 01_ingestao_bronze, que compara o hash de cada arquivo com os
-- já registrados antes de gravar. Esta verificação prova a propriedade a
-- qualquer momento — tem de retornar zero linhas:
--
--   SELECT sha256, count(*) FROM bronze.arquivo
--    GROUP BY sha256 HAVING count(*) > 1;
-- ---------------------------------------------------------------------
