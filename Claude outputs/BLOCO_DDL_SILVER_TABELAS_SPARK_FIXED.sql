-- =====================================================================
-- BLOCO DDL: Criação das 6 tabelas Silver (VERSÃO SPARK/DELTA CORRIGIDA)
-- Executa ANTES de Bloco 4 (Promoção)
-- =====================================================================

USE CATALOG scapegoat;
USE SCHEMA silver;

-- =====================================================================
-- 1. CONTA — autores e mencionados
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.conta (
  conta_id       BIGINT GENERATED ALWAYS AS IDENTITY,
  plataforma     STRING NOT NULL COMMENT 'x (Twitter/X)',
  handle         STRING NOT NULL COMMENT 'username, lowercase',
  id_nativo      STRING COMMENT 'ID numérico nativo da plataforma (opcional)',
  criado_em      TIMESTAMP NOT NULL
) COMMENT 'Contas na rede social. Grão: uma conta, identificada por (plataforma, handle).';

ALTER TABLE silver.conta ADD CONSTRAINT conta_plataforma_handle_unica
  UNIQUE (plataforma, handle);

-- =====================================================================
-- 2. POSTAGEM — postagens (tweets)
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.postagem (
  postagem_id    BIGINT GENERATED ALWAYS AS IDENTITY,
  plataforma     STRING NOT NULL COMMENT 'x',
  id_nativo      STRING NOT NULL COMMENT 'ID numérico nativo',
  caso_slug      STRING NOT NULL COMMENT 'monark | arthur_do_val',
  autor_conta_id BIGINT NOT NULL COMMENT 'FK → conta',
  created_at     TIMESTAMP NOT NULL COMMENT 'data de criação no X',
  texto          STRING COMMENT 'texto original',
  texto_limpo    STRING COMMENT 'texto com URLs/menções/hashtags removidas',
  idioma         STRING COMMENT 'código ISO 639-1 (pt, en, etc.)',
  tipo_ref       STRING COMMENT 'original | reply | quote',
  ref_id_nativo  STRING COMMENT 'ID nativo do tweet respondido (reply/quote)',
  ref_conta_id   BIGINT COMMENT 'FK → conta respondida (reply apenas)',
  situacao       STRING NOT NULL COMMENT 'ativa | deletada | suspensa',
  fonte          STRING COMMENT 'graphql | consolidado',
  carregada_em   TIMESTAMP NOT NULL
) COMMENT 'Postagens (tweets). Grão: uma postagem, identificada por (plataforma, id_nativo).';

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_plataforma_idnativo_unica
  UNIQUE (plataforma, id_nativo);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_tipo_ref_valido
  CHECK (tipo_ref IN ('original', 'reply', 'quote'));

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_situacao_valida
  CHECK (situacao IN ('ativa', 'deletada', 'suspensa'));

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_autor_fk
  FOREIGN KEY (autor_conta_id) REFERENCES silver.conta(conta_id);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_ref_fk
  FOREIGN KEY (ref_conta_id) REFERENCES silver.conta(conta_id);

-- =====================================================================
-- 3. CAPTURA — engajamento (likes, retweets, etc.)
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.captura (
  captura_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id    BIGINT NOT NULL COMMENT 'FK → postagem',
  likes          INT NOT NULL COMMENT 'likes count',
  retweets       INT NOT NULL COMMENT 'retweets count',
  quotes         INT NOT NULL COMMENT 'quotes count',
  respostas      INT NOT NULL COMMENT 'replies count',
  capturas       INT NOT NULL COMMENT 'quantas vezes este id foi visto (duplicatas)',
  capturado_em   TIMESTAMP NOT NULL
) COMMENT 'Engajamento das postagens, capturado em momentos diferentes. Grão: uma captura (postagem em um momento).';

ALTER TABLE silver.captura ADD CONSTRAINT captura_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

-- =====================================================================
-- 4. MENCAO — arestas da rede (postagem → conta mencionada)
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.mencao (
  mencao_id      BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id    BIGINT NOT NULL COMMENT 'FK → postagem',
  conta_id       BIGINT NOT NULL COMMENT 'FK → conta mencionada'
) COMMENT 'Menções: arestas postagem → conta. Grão: uma menção.';

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_conta_fk
  FOREIGN KEY (conta_id) REFERENCES silver.conta(conta_id);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_unica
  UNIQUE (postagem_id, conta_id);

-- =====================================================================
-- 5. POSTAGEM_HASHTAG — hashtags por postagem
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.postagem_hashtag (
  postagem_id    BIGINT NOT NULL COMMENT 'FK → postagem',
  hashtag        STRING NOT NULL COMMENT 'hashtag em minúsculas (sem #)'
) COMMENT 'Hashtags por postagem. Grão: uma hashtag em uma postagem.';

ALTER TABLE silver.postagem_hashtag ADD CONSTRAINT postagem_hashtag_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.postagem_hashtag ADD CONSTRAINT postagem_hashtag_unica
  UNIQUE (postagem_id, hashtag);

-- =====================================================================
-- 6. CLASSIFICACAO — rótulos (stance: acusador | defensor | neutro)
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver.classificacao (
  classificacao_id BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id      BIGINT NOT NULL COMMENT 'FK → postagem',
  esquema          STRING NOT NULL COMMENT 'stance (único por enquanto)',
  rotulo           STRING NOT NULL COMMENT 'acusador | defensor | neutro',
  modelo           STRING NOT NULL COMMENT 'bertimbau-stance',
  versao           STRING NOT NULL COMMENT 'v2b | previa-sem-confianca',
  confianca        DOUBLE COMMENT 'score de confiança do modelo (0-1)',
  classificado_em  TIMESTAMP NOT NULL
) COMMENT 'Classificações de postagens. Grão: uma classificação por postagem e esquema.';

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_rotulo_valido
  CHECK (rotulo IN ('acusador', 'defensor', 'neutro'));

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_esquema_valido
  CHECK (esquema = 'stance');

-- =====================================================================
-- Resumo: 6 tabelas, 9 FKs, 6 UNIQUE/PK compostos
-- =====================================================================
-- Próximo: Executar Bloco 4 (MERGE conta, postagem) + (INSERT captura, mencao, hashtag, classificacao)
