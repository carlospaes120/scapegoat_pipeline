USE CATALOG scapegoat;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS silver.conta (
  conta_id BIGINT,
  plataforma STRING,
  handle STRING,
  id_nativo STRING,
  criado_em TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.postagem (
  postagem_id BIGINT,
  plataforma STRING,
  id_nativo STRING,
  caso_slug STRING,
  autor_conta_id BIGINT,
  created_at TIMESTAMP,
  texto STRING,
  texto_limpo STRING,
  idioma STRING,
  tipo_ref STRING,
  ref_id_nativo STRING,
  ref_conta_id BIGINT,
  situacao STRING,
  fonte STRING,
  carregada_em TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.captura (
  captura_id BIGINT,
  postagem_id BIGINT,
  likes INT,
  retweets INT,
  quotes INT,
  respostas INT,
  capturas INT,
  capturado_em TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.mencao (
  mencao_id BIGINT,
  postagem_id BIGINT,
  conta_id BIGINT
);

CREATE TABLE IF NOT EXISTS silver.postagem_hashtag (
  postagem_id BIGINT,
  hashtag STRING
);

CREATE TABLE IF NOT EXISTS silver.classificacao (
  classificacao_id BIGINT,
  postagem_id BIGINT,
  esquema STRING,
  rotulo STRING,
  modelo STRING,
  versao STRING,
  confianca DOUBLE,
  classificado_em TIMESTAMP
);
