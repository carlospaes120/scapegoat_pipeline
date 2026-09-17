-- definicao de silver.v_promovivel em 16/09/2026, antes do plano C (passo 1.1); guardada por regra do projeto (nada e apagado sem copia)
-- obtida com: SHOW CREATE TABLE scapegoat.silver.v_promovivel
-- NAO executar: a view atual (reescrita em 16/09) aplica o portao de QC e calcula texto_limpo; esta nao fazia nenhum dos dois.
CREATE VIEW silver.v_promovivel (
  caso_slug COMMENT 'episódio a que o arquivo pertence',
  id_nativo,
  autor_handle,
  autor_id_nativo,
  created_at,
  texto,
  idioma,
  ref_status_id,
  ref_handle,
  tipo_ref,
  mencoes,
  hashtags,
  stance_previa,
  fonte,
  likes,
  retweets,
  quotes,
  respostas,
  capturas)
COMMENT 'VIEW: linhas de v_deduplicado que passam o QC (silver.v_qc_portao.pode_promover) e ja trazem texto_limpo calculado. Entrada dos MERGE/INSERT da Silver.'
WITH SCHEMA COMPENSATION
AS SELECT * FROM silver.v_deduplicado
WHERE autor_handle IS NOT NULL AND trim(autor_handle) <> ''
