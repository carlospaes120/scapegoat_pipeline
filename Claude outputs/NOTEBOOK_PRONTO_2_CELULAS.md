# Databricks Notebook — 2 Células Prontas para Copiar e Colar

---

## CÉLULA 1 (Python) — Criar Widgets

Cole isto na **primeira célula** do seu notebook e execute:

```python
dbutils.widgets.text("caso", "monark", "Caso")
dbutils.widgets.text("versao", "v0.2.0-dev", "Versão")
```

Depois de executar, aparecerão dois campos no topo do notebook onde você pode digitar/selecionar:
- **Caso**: monark (ou arthur_do_val)
- **Versão**: v0.2.0-dev (ou outra tag)

---

## CÉLULA 2 (SQL) — Pipeline Completo (Bronze → Silver)

Cole isto na **segunda célula** do seu notebook. Mude o tipo de célula para **SQL** (não Python):

```sql
USE CATALOG scapegoat;
USE SCHEMA silver;

-- =====================================================================
-- 2. NORMALIZAÇÃO — une as duas fontes de ingestão num único formato
-- =====================================================================

CREATE OR REPLACE VIEW silver.v_normalizado
COMMENT 'Une as duas fontes de ingestão num único formato. É o normalizador do pipeline.'
AS
WITH graphql AS (
  SELECT
    a.arquivo_id, a.caso_slug, r.linha,
    get_json_object(r.payload,'$.legacy.id_str')                                AS id_nativo,
    get_json_object(r.payload,'$.core.user_results.result.core.screen_name')    AS autor_handle,
    get_json_object(r.payload,'$.core.user_results.result.rest_id')             AS autor_id_nativo,
    to_timestamp(get_json_object(r.payload,'$.legacy.created_at'),
                 'EEE MMM dd HH:mm:ss Z yyyy')                                  AS created_at,
    get_json_object(r.payload,'$.legacy.full_text')                             AS texto,
    get_json_object(r.payload,'$.legacy.lang')                                  AS idioma,
    get_json_object(r.payload,'$.legacy.in_reply_to_status_id_str')             AS ref_status_id,
    get_json_object(r.payload,'$.legacy.in_reply_to_screen_name')               AS ref_handle,
    CAST(get_json_object(r.payload,'$.legacy.is_quote_status') AS BOOLEAN)      AS eh_quote,
    CAST(get_json_object(r.payload,'$.legacy.favorite_count') AS INT)           AS likes,
    CAST(get_json_object(r.payload,'$.legacy.retweet_count')  AS INT)           AS retweets,
    CAST(get_json_object(r.payload,'$.legacy.quote_count')    AS INT)           AS quotes,
    CAST(get_json_object(r.payload,'$.legacy.reply_count')    AS INT)           AS respostas,
    transform(
      from_json(get_json_object(r.payload,'$.legacy.entities.user_mentions'),
                'array<struct<id_str:string, screen_name:string>>'),
      m -> named_struct('id_nativo', m.id_str, 'handle', lower(m.screen_name))
    )                                                                           AS mencoes,
    transform(
      from_json(get_json_object(r.payload,'$.legacy.entities.hashtags'),
                'array<struct<text:string>>'),
      h -> lower(h.text)
    )                                                                           AS hashtags,
    CAST(NULL AS STRING)                                                        AS stance_previa,
    'graphql'                                                                   AS fonte
  FROM bronze.registro r
  JOIN bronze.arquivo  a USING (arquivo_id)
  WHERE a.fonte = 'graphql'
),
consolidado AS (
  SELECT
    a.arquivo_id, a.caso_slug, r.linha,
    get_json_object(r.payload,'$.id')                                           AS id_nativo,
    get_json_object(r.payload,'$.user')                                         AS autor_handle,
    CAST(NULL AS STRING)                                                        AS autor_id_nativo,
    to_timestamp(get_json_object(r.payload,'$.created_at_iso'))                 AS created_at,
    get_json_object(r.payload,'$.text')                                         AS texto,
    CAST(NULL AS STRING)                                                        AS idioma,
    nullif(get_json_object(r.payload,'$.in_reply_to_status_id'),'')             AS ref_status_id,
    nullif(get_json_object(r.payload,'$.in_reply_to_user'),'')                  AS ref_handle,
    CAST(get_json_object(r.payload,'$.is_quote') AS BOOLEAN)                    AS eh_quote,
    CAST(get_json_object(r.payload,'$.like_count')    AS INT)                   AS likes,
    CAST(get_json_object(r.payload,'$.retweet_count') AS INT)                   AS retweets,
    CAST(get_json_object(r.payload,'$.quote_count')   AS INT)                   AS quotes,
    CAST(get_json_object(r.payload,'$.reply_count')   AS INT)                   AS respostas,
    transform(
      from_json(get_json_object(r.payload,'$.mentions'),
                'array<struct<id_str:string, username:string>>'),
      m -> named_struct('id_nativo', m.id_str, 'handle', lower(m.username))
    )                                                                           AS mencoes,
    transform(
      from_json(get_json_object(r.payload,'$.hashtags'), 'array<string>'),
      h -> lower(h)
    )                                                                           AS hashtags,
    nullif(get_json_object(r.payload,'$.stance'),'')                            AS stance_previa,
    'consolidado'                                                               AS fonte
  FROM bronze.registro r
  JOIN bronze.arquivo  a USING (arquivo_id)
  WHERE a.fonte = 'consolidado'
),
uniao AS (SELECT * FROM graphql UNION ALL SELECT * FROM consolidado)
SELECT
  *,
  CASE
    WHEN ref_status_id IS NOT NULL OR ref_handle IS NOT NULL THEN 'reply'
    WHEN eh_quote                                            THEN 'quote'
    ELSE 'original'
  END AS tipo_ref
FROM uniao;

-- =====================================================================
-- 2.1 Deduplicação
-- =====================================================================

CREATE OR REPLACE VIEW silver.v_deduplicado
COMMENT 'Uma linha por postagem. Contadores = máximo observado entre as capturas do mesmo id.'
AS
SELECT
  caso_slug, id_nativo,
  min_by(autor_handle,    linha) AS autor_handle,
  min_by(autor_id_nativo, linha) AS autor_id_nativo,
  min_by(created_at,      linha) AS created_at,
  min_by(texto,           linha) AS texto,
  min_by(idioma,          linha) AS idioma,
  min_by(ref_status_id,   linha) AS ref_status_id,
  min_by(ref_handle,      linha) AS ref_handle,
  min_by(tipo_ref,        linha) AS tipo_ref,
  min_by(mencoes,         linha) AS mencoes,
  min_by(hashtags,        linha) AS hashtags,
  min_by(stance_previa,   linha) AS stance_previa,
  min_by(fonte,           linha) AS fonte,
  max(likes)     AS likes,
  max(retweets)  AS retweets,
  max(quotes)    AS quotes,
  max(respostas) AS respostas,
  count(*)       AS capturas
FROM silver.v_normalizado
GROUP BY caso_slug, id_nativo;

-- =====================================================================
-- 3. QC BLOQUEANTE
-- =====================================================================

DROP TABLE IF EXISTS silver.qc_resultado;

CREATE TABLE IF NOT EXISTS silver.qc_resultado (
  caso_slug       STRING NOT NULL,
  indicador       STRING NOT NULL,
  valor           DOUBLE,
  severidade      STRING NOT NULL,
  aprovado        BOOLEAN,
  detalhe         STRING,
  versao_pipeline STRING NOT NULL,
  verificado_em   TIMESTAMP
);

ALTER TABLE silver.qc_resultado ADD CONSTRAINT qc_severidade_valida
  CHECK (severidade IN ('bloqueia','alerta','informa'));

-- 3.1 A bateria de QC
INSERT INTO silver.qc_resultado (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline)
WITH base AS (SELECT * FROM silver.v_normalizado WHERE caso_slug = :caso),
     dedup AS (SELECT * FROM silver.v_deduplicado WHERE caso_slug = :caso),
     arq   AS (SELECT * FROM bronze.arquivo      WHERE caso_slug = :caso)
SELECT * FROM (
  SELECT :caso, 'qc_linhas_bronze',        CAST(count(*) AS DOUBLE), 'informa',  true,
         'registros lidos da camada Bronze', :versao FROM base
  UNION ALL
  SELECT :caso, 'qc_postagens_unicas',     CAST(count(*) AS DOUBLE), 'informa',  true,
         NULL, :versao FROM dedup
  UNION ALL
  SELECT :caso, 'qc_duplicatas_removidas',
         CAST((SELECT count(*) FROM base) - (SELECT count(*) FROM dedup) AS DOUBLE),
         'informa', true, 'colapsadas por (caso, id_nativo)', :versao
  UNION ALL
  SELECT :caso, 'qc_ids_duplicados', CAST(count(*) AS DOUBLE), 'informa', true,
         'ids que aparecem mais de uma vez no bruto', :versao
  FROM dedup WHERE capturas > 1
  UNION ALL
  SELECT :caso, 'qc_duplicatas_divergentes', CAST(count(*) AS DOUBLE), 'alerta',
         count(*) = 0,
         'mesmo id com contadores diferentes: capturas em momentos distintos da raspagem',
         :versao
  FROM (
    SELECT caso_slug, id_nativo FROM silver.v_normalizado WHERE caso_slug = :caso
    GROUP BY caso_slug, id_nativo
    HAVING count(DISTINCT concat_ws('|', likes, retweets, quotes, respostas)) > 1
  )
  UNION ALL
  SELECT :caso, 'qc_id_fora_do_padrao', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0,
         'id_nativo deve ser só dígitos, 15 a 20',
         :versao
  FROM dedup WHERE NOT (id_nativo RLIKE '^[0-9]{15,20}$')
  UNION ALL
  SELECT :caso, 'qc_descartadas_sem_autor', CAST(count(*) AS DOUBLE), 'alerta',
         count(*) = 0,
         'postagens sem handle de autor, descartadas na promoção',
         :versao
  FROM dedup WHERE autor_handle IS NULL OR trim(autor_handle) = ''
  UNION ALL
  SELECT :caso, 'qc_pct_sem_autor',
         CAST(100.0 * sum(CASE WHEN autor_handle IS NULL OR trim(autor_handle)='' THEN 1 ELSE 0 END)
              / count(*) AS DOUBLE),
         'bloqueia',
         100.0 * sum(CASE WHEN autor_handle IS NULL OR trim(autor_handle)='' THEN 1 ELSE 0 END)
              / count(*) <= 1.0,
         'acima de 1% sem autor indica falha de coleta',
         :versao
  FROM dedup
  UNION ALL
  SELECT :caso, 'qc_data_nao_parseada', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0, 'created_at nulo após conversão', :versao
  FROM dedup WHERE created_at IS NULL
  UNION ALL
  SELECT :caso, 'qc_fora_da_janela', CAST(count(*) AS DOUBLE), 'alerta',
         count(*) = 0, 'postagem fora do período declarado na coleta', :versao
  FROM dedup d WHERE NOT EXISTS (
    SELECT 1 FROM arq a
    WHERE date(d.created_at) BETWEEN a.periodo_inicio AND a.periodo_fim)
  UNION ALL
  SELECT :caso, 'qc_data_no_futuro', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0, NULL, :versao
  FROM dedup WHERE created_at > current_timestamp()
  UNION ALL
  SELECT :caso, 'qc_reply_sem_destino', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0,
         'reply precisa de ref_handle',
         :versao
  FROM dedup WHERE tipo_ref = 'reply' AND (ref_handle IS NULL OR trim(ref_handle) = '')
  UNION ALL
  SELECT :caso, 'qc_replies_reclassificados', CAST(count(*) AS DOUBLE), 'informa', true,
         'respostas que o corpus rotulava como original e o pipeline corrigiu',
         :versao
  FROM dedup WHERE tipo_ref = 'reply'
  UNION ALL
  SELECT :caso, 'qc_contador_negativo', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0, NULL, :versao
  FROM dedup WHERE likes < 0 OR retweets < 0 OR quotes < 0 OR respostas < 0
  UNION ALL
  SELECT :caso, 'qc_texto_vazio', CAST(count(*) AS DOUBLE), 'alerta',
         count(*) = 0, NULL, :versao
  FROM dedup WHERE texto IS NULL OR trim(texto) = ''
  UNION ALL
  SELECT :caso, 'qc_idioma_inesperado', CAST(count(*) AS DOUBLE), 'alerta',
         count(*) = 0,
         'postagem em idioma diferente do declarado apesar do filtro lang',
         :versao
  FROM dedup WHERE idioma IS NOT NULL AND idioma <> 'pt'
  UNION ALL
  SELECT :caso, 'qc_mencoes_totais', CAST(sum(size(coalesce(mencoes, array()))) AS DOUBLE),
         'informa', true, 'arestas do grafo de menções', :versao
  FROM dedup
  UNION ALL
  SELECT :caso, 'qc_pct_mencoes_com_id',
         CAST(100.0 * sum(CASE WHEN m.id_nativo IS NOT NULL THEN 1 ELSE 0 END) / count(*) AS DOUBLE),
         'alerta',
         sum(CASE WHEN m.id_nativo IS NULL THEN 1 ELSE 0 END) = 0,
         'menções com id nativo resolvem CONTA sem heurística de handle',
         :versao
  FROM dedup LATERAL VIEW explode(mencoes) t AS m
  UNION ALL
  SELECT :caso, 'qc_cv_volume_diario', CAST(stddev_samp(n)/avg(n) AS DOUBLE), 'informa', true,
         'coeficiente de variação do volume diário',
         :versao
  FROM (SELECT date(created_at) AS d, count(*) AS n FROM dedup GROUP BY 1)
  UNION ALL
  SELECT :caso, 'qc_com_stance_previa', CAST(count(*) AS DOUBLE), 'informa', true,
         'rótulos de versão anterior, sem confiança registrada',
         :versao
  FROM dedup WHERE stance_previa IS NOT NULL
  UNION ALL
  SELECT :caso, 'qc_stance_previa_fora_do_dominio', CAST(count(*) AS DOUBLE), 'bloqueia',
         count(*) = 0, 'stance ∈ {acusador, defensor, neutro}', :versao
  FROM dedup WHERE stance_previa IS NOT NULL
    AND stance_previa NOT IN ('acusador','defensor','neutro')
);

-- 3.2 O portão: pode promover?
CREATE OR REPLACE VIEW silver.v_qc_portao
COMMENT 'Um caso só pode ser promovido quando pode_promover = true.'
AS
SELECT caso_slug, versao_pipeline,
       sum(CASE WHEN severidade = 'bloqueia' AND NOT aprovado THEN 1 ELSE 0 END) AS bloqueios,
       sum(CASE WHEN severidade = 'alerta'   AND NOT aprovado THEN 1 ELSE 0 END) AS alertas,
       sum(CASE WHEN severidade = 'bloqueia' AND NOT aprovado THEN 1 ELSE 0 END) = 0 AS pode_promover
FROM silver.qc_resultado
GROUP BY caso_slug, versao_pipeline;

-- =====================================================================
-- 4. PROMOÇÃO PARA AS TABELAS DEFINITIVAS
-- =====================================================================

-- 4.0 O que efetivamente é promovido
CREATE OR REPLACE VIEW silver.v_promovivel
COMMENT 'Subconjunto de v_deduplicado que pode virar POSTAGEM.'
AS
SELECT * FROM silver.v_deduplicado
WHERE autor_handle IS NOT NULL AND trim(autor_handle) <> '';

-- 4.1 CONTA — autores e mencionados
MERGE INTO silver.conta AS alvo
USING (
  SELECT handle, max(id_nativo) AS id_nativo FROM (
    SELECT lower(autor_handle) AS handle, autor_id_nativo AS id_nativo
      FROM silver.v_promovivel WHERE caso_slug = :caso
    UNION ALL
    SELECT m.handle, m.id_nativo
      FROM silver.v_promovivel LATERAL VIEW explode(mencoes) t AS m
     WHERE caso_slug = :caso AND m.handle IS NOT NULL
    UNION ALL
    SELECT lower(ref_handle), NULL
      FROM silver.v_promovivel WHERE caso_slug = :caso AND ref_handle IS NOT NULL
  ) GROUP BY handle
) AS origem
ON alvo.plataforma = 'x' AND alvo.handle = origem.handle
WHEN MATCHED AND alvo.id_nativo IS NULL AND origem.id_nativo IS NOT NULL
  THEN UPDATE SET alvo.id_nativo = origem.id_nativo
WHEN NOT MATCHED
  THEN INSERT (plataforma, handle, id_nativo, criado_em)
       VALUES ('x', origem.handle, origem.id_nativo, current_timestamp());

-- 4.2 POSTAGEM
MERGE INTO silver.postagem AS alvo
USING (
  SELECT d.id_nativo, d.caso_slug, c.conta_id AS autor_conta_id, d.created_at,
         d.texto, d.idioma, d.tipo_ref, d.ref_status_id,
         cr.conta_id AS ref_conta_id, d.fonte
    FROM silver.v_promovivel d
    JOIN silver.conta c  ON c.plataforma='x'  AND c.handle = lower(d.autor_handle)
    LEFT JOIN silver.conta cr ON cr.plataforma='x' AND cr.handle = lower(d.ref_handle)
   WHERE d.caso_slug = :caso
) AS origem
ON alvo.plataforma = 'x' AND alvo.id_nativo = origem.id_nativo
WHEN NOT MATCHED THEN INSERT (
  plataforma, id_nativo, caso_slug, autor_conta_id, created_at, texto, idioma,
  tipo_ref, ref_id_nativo, ref_conta_id, situacao, fonte, carregada_em)
VALUES (
  'x', origem.id_nativo, origem.caso_slug, origem.autor_conta_id, origem.created_at,
  origem.texto, origem.idioma, origem.tipo_ref, origem.ref_status_id,
  origem.ref_conta_id, 'ativa', origem.fonte, current_timestamp());

-- 4.3 CAPTURA — engajamento é fotografia datada
INSERT INTO silver.captura (postagem_id, likes, retweets, quotes, respostas, capturas, capturado_em)
SELECT p.postagem_id, d.likes, d.retweets, d.quotes, d.respostas, d.capturas, current_timestamp()
  FROM silver.v_promovivel d
  JOIN silver.postagem p ON p.plataforma='x' AND p.id_nativo = d.id_nativo
 WHERE d.caso_slug = :caso;

-- 4.4 MENCAO — as arestas
INSERT INTO silver.mencao (postagem_id, conta_id)
SELECT DISTINCT p.postagem_id, c.conta_id
  FROM silver.v_promovivel d
  LATERAL VIEW explode(d.mencoes) t AS m
  JOIN silver.postagem p ON p.plataforma='x' AND p.id_nativo = d.id_nativo
  JOIN silver.conta    c ON c.plataforma='x' AND c.handle = m.handle
 WHERE d.caso_slug = :caso;

-- 4.5 POSTAGEM_HASHTAG
INSERT INTO silver.postagem_hashtag (postagem_id, hashtag)
SELECT DISTINCT p.postagem_id, h
  FROM silver.v_promovivel d
  LATERAL VIEW explode(d.hashtags) t AS h
  JOIN silver.postagem p ON p.plataforma='x' AND p.id_nativo = d.id_nativo
 WHERE d.caso_slug = :caso AND h IS NOT NULL AND trim(h) <> '';

-- 4.6 CLASSIFICACAO — rótulos que vieram no arquivo
INSERT INTO silver.classificacao (postagem_id, esquema, rotulo, modelo, versao, confianca, classificado_em)
SELECT p.postagem_id, 'stance', d.stance_previa,
       'bertimbau-stance', 'previa-sem-confianca', NULL, current_timestamp()
  FROM silver.v_promovivel d
  JOIN silver.postagem p ON p.plataforma='x' AND p.id_nativo = d.id_nativo
 WHERE d.caso_slug = :caso AND d.stance_previa IS NOT NULL;

-- =====================================================================
-- FIM
-- =====================================================================
```

---

## Como usar:

1. **Abra seu notebook Databricks**
2. **Célula 1**: Cole o código Python (widgets) e execute
3. **Célula 2**: Crie uma **nova célula**, mude para **SQL**, e cole todo o código acima
4. **Execute**: Clique em "Run All" ou execute célula por célula

O Databricks vai pedir os valores:
- Caso: **monark**
- Versão: **v0.2.0-dev**

Depois execute a Célula 2. O QC vai rodar primeiro, depois o Bloco 4 (MERGE e INSERT) vai popular as tabelas.

---

Pronto! Está tudo aqui.