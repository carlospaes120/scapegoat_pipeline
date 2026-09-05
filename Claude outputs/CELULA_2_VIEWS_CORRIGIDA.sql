USE CATALOG scapegoat;
USE SCHEMA silver;

-- =====================================================================
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
    to_timestamp(substring(get_json_object(r.payload,'$.legacy.created_at'), 5),
                 'MMM dd HH:mm:ss Z yyyy')                                  AS created_at,
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

