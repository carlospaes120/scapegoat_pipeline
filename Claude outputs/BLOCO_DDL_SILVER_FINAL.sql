USE CATALOG scapegoat;
USE SCHEMA silver;

-- Tabela 1: CONTA
CREATE TABLE IF NOT EXISTS silver.conta (
  conta_id       BIGINT GENERATED ALWAYS AS IDENTITY,
  plataforma     STRING NOT NULL,
  handle         STRING NOT NULL,
  id_nativo      STRING,
  criado_em      TIMESTAMP NOT NULL
);

ALTER TABLE silver.conta ADD CONSTRAINT conta_pk PRIMARY KEY (conta_id);

ALTER TABLE silver.conta ADD CONSTRAINT conta_unica
  UNIQUE (plataforma, handle);

-- Tabela 2: POSTAGEM
CREATE TABLE IF NOT EXISTS silver.postagem (
  postagem_id    BIGINT GENERATED ALWAYS AS IDENTITY,
  plataforma     STRING NOT NULL,
  id_nativo      STRING NOT NULL,
  caso_slug      STRING NOT NULL,
  autor_conta_id BIGINT NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  texto          STRING,
  texto_limpo    STRING,
  idioma         STRING,
  tipo_ref       STRING,
  ref_id_nativo  STRING,
  ref_conta_id   BIGINT,
  situacao       STRING NOT NULL,
  fonte          STRING,
  carregada_em   TIMESTAMP NOT NULL
);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_pk PRIMARY KEY (postagem_id);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_unica
  UNIQUE (plataforma, id_nativo);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_tipo_ref_ck
  CHECK (tipo_ref IN ('original', 'reply', 'quote'));

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_situacao_ck
  CHECK (situacao IN ('ativa', 'deletada', 'suspensa'));

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_autor_fk
  FOREIGN KEY (autor_conta_id) REFERENCES silver.conta(conta_id);

ALTER TABLE silver.postagem ADD CONSTRAINT postagem_ref_fk
  FOREIGN KEY (ref_conta_id) REFERENCES silver.conta(conta_id);

-- Tabela 3: CAPTURA
CREATE TABLE IF NOT EXISTS silver.captura (
  captura_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id    BIGINT NOT NULL,
  likes          INT NOT NULL,
  retweets       INT NOT NULL,
  quotes         INT NOT NULL,
  respostas      INT NOT NULL,
  capturas       INT NOT NULL,
  capturado_em   TIMESTAMP NOT NULL
);

ALTER TABLE silver.captura ADD CONSTRAINT captura_pk PRIMARY KEY (captura_id);

ALTER TABLE silver.captura ADD CONSTRAINT captura_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

-- Tabela 4: MENCAO
CREATE TABLE IF NOT EXISTS silver.mencao (
  mencao_id      BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id    BIGINT NOT NULL,
  conta_id       BIGINT NOT NULL
);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_pk PRIMARY KEY (mencao_id);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_conta_fk
  FOREIGN KEY (conta_id) REFERENCES silver.conta(conta_id);

ALTER TABLE silver.mencao ADD CONSTRAINT mencao_unica
  UNIQUE (postagem_id, conta_id);

-- Tabela 5: POSTAGEM_HASHTAG
CREATE TABLE IF NOT EXISTS silver.postagem_hashtag (
  postagem_id    BIGINT NOT NULL,
  hashtag        STRING NOT NULL
);

ALTER TABLE silver.postagem_hashtag ADD CONSTRAINT postagem_hashtag_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.postagem_hashtag ADD CONSTRAINT postagem_hashtag_unica
  UNIQUE (postagem_id, hashtag);

-- Tabela 6: CLASSIFICACAO
CREATE TABLE IF NOT EXISTS silver.classificacao (
  classificacao_id BIGINT GENERATED ALWAYS AS IDENTITY,
  postagem_id      BIGINT NOT NULL,
  esquema          STRING NOT NULL,
  rotulo           STRING NOT NULL,
  modelo           STRING NOT NULL,
  versao           STRING NOT NULL,
  confianca        DOUBLE,
  classificado_em  TIMESTAMP NOT NULL
);

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_pk PRIMARY KEY (classificacao_id);

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_postagem_fk
  FOREIGN KEY (postagem_id) REFERENCES silver.postagem(postagem_id);

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_rotulo_ck
  CHECK (rotulo IN ('acusador', 'defensor', 'neutro'));

ALTER TABLE silver.classificacao ADD CONSTRAINT classificacao_esquema_ck
  CHECK (esquema = 'stance');
