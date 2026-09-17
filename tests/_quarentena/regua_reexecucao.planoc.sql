-- tests/regua_reexecucao.sql
-- Regua do plano C: contagens, somas, distribuicoes e dominios que nao podem mudar
-- apos a reexecucao do pipeline. Rodar antes (16/09/2026) e apos cada execucao do job.
-- Saida: tabela, metrica, chave, valor (texto). Comparar linha a linha.

WITH linhas AS (
  SELECT 'silver.conta' AS tabela, 'linhas' AS metrica, '-' AS chave, CAST(COUNT(*) AS STRING) AS valor FROM scapegoat.silver.conta
  UNION ALL SELECT 'silver.postagem',         'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.postagem
  UNION ALL SELECT 'silver.captura',          'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.captura
  UNION ALL SELECT 'silver.mencao',           'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.mencao
  UNION ALL SELECT 'silver.postagem_hashtag', 'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.postagem_hashtag
  UNION ALL SELECT 'silver.classificacao',    'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.classificacao
  UNION ALL SELECT 'silver.qc_resultado',     'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.silver.qc_resultado
  UNION ALL SELECT 'gold.calendario_caso',    'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.calendario_caso
  UNION ALL SELECT 'gold.dim_conta_papel',    'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.dim_conta_papel
  UNION ALL SELECT 'gold.fato_atividade',     'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_rede',          'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.grafo_arestas',      'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.grafo_arestas
  UNION ALL SELECT 'gold.fato_referencia',    'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_referencia
  UNION ALL SELECT 'gold.papel_narrativo_v0', 'linhas', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.papel_narrativo_v0
),
somas AS (
  SELECT 'gold.fato_atividade', 'soma_n_postagens', caso, CAST(SUM(n_postagens) AS STRING) FROM scapegoat.gold.fato_atividade GROUP BY caso
  UNION ALL SELECT 'gold.fato_referencia', 'soma_n_postagens', caso, CAST(SUM(n_postagens) AS STRING) FROM scapegoat.gold.fato_referencia GROUP BY caso
  UNION ALL SELECT 'gold.fato_rede', 'soma_n_mencoes', caso, CAST(SUM(n_mencoes) AS STRING) FROM scapegoat.gold.fato_rede GROUP BY caso
  UNION ALL SELECT 'gold.fato_rede', 'soma_n_arestas', caso, CAST(SUM(n_arestas) AS STRING) FROM scapegoat.gold.fato_rede GROUP BY caso
  UNION ALL SELECT 'gold.fato_rede', 'linhas_por_caso', caso, CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_rede GROUP BY caso
  UNION ALL SELECT 'gold.grafo_arestas', 'soma_peso', caso, CAST(SUM(peso) AS STRING) FROM scapegoat.gold.grafo_arestas GROUP BY caso
  UNION ALL SELECT 'gold.grafo_arestas', 'pares_linhas', caso, CAST(COUNT(*) AS STRING) FROM scapegoat.gold.grafo_arestas GROUP BY caso
  UNION ALL SELECT 'gold.grafo_arestas', 'alvo_como_origem', '-', CAST(COUNT(*) AS STRING) FROM scapegoat.gold.grafo_arestas WHERE papel_origem = 'alvo'
  UNION ALL SELECT 'silver.mencao', 'linhas_por_caso', p.caso_slug, CAST(COUNT(*) AS STRING)
    FROM scapegoat.silver.mencao m JOIN scapegoat.silver.postagem p ON m.postagem_id = p.postagem_id GROUP BY p.caso_slug
  UNION ALL SELECT 'silver.captura', 'linhas_por_caso', p.caso_slug, CAST(COUNT(*) AS STRING)
    FROM scapegoat.silver.captura c JOIN scapegoat.silver.postagem p ON c.postagem_id = p.postagem_id GROUP BY p.caso_slug
  UNION ALL SELECT 'silver.postagem', 'linhas_por_caso', caso_slug, CAST(COUNT(*) AS STRING) FROM scapegoat.silver.postagem GROUP BY caso_slug
),
classif AS (
  SELECT 'silver.classificacao', 'rotulo_por_caso', CONCAT(p.caso_slug, ' / ', c.rotulo), CAST(COUNT(*) AS STRING)
    FROM scapegoat.silver.classificacao c JOIN scapegoat.silver.postagem p ON c.postagem_id = p.postagem_id
    GROUP BY p.caso_slug, c.rotulo
  UNION ALL SELECT 'silver.classificacao', 'linhas_por_versao', versao, CAST(COUNT(*) AS STRING) FROM scapegoat.silver.classificacao GROUP BY versao
),
versoes AS (
  SELECT 'gold.fato_rede', 'linhas_por_versao_pipeline', versao_pipeline, CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_rede GROUP BY versao_pipeline
  UNION ALL SELECT 'gold.fato_atividade', 'linhas_por_versao_classificacao', versao_classificacao, CAST(COUNT(*) AS STRING) FROM scapegoat.gold.fato_atividade GROUP BY versao_classificacao
  UNION ALL SELECT 'silver.qc_resultado', 'linhas_por_caso_versao', CONCAT(caso_slug, ' / ', versao_pipeline), CAST(COUNT(*) AS STRING) FROM scapegoat.silver.qc_resultado GROUP BY caso_slug, versao_pipeline
  UNION ALL SELECT 'silver.v_qc_portao', 'bloqueios|alertas|pode_promover', CONCAT(caso_slug, ' / ', versao_pipeline),
    CONCAT(CAST(bloqueios AS STRING), ' | ', CAST(alertas AS STRING), ' | ', CAST(pode_promover AS STRING)) FROM scapegoat.silver.v_qc_portao
),
dominios AS (
  SELECT 'gold.calendario_caso', 'min|max', 'data', CONCAT(CAST(MIN(data) AS STRING), ' | ', CAST(MAX(data) AS STRING)) FROM scapegoat.gold.calendario_caso
  UNION ALL SELECT 'gold.calendario_caso', 'min|max', 'dias_desde_estopim', CONCAT(CAST(MIN(dias_desde_estopim) AS STRING), ' | ', CAST(MAX(dias_desde_estopim) AS STRING)) FROM scapegoat.gold.calendario_caso
  UNION ALL SELECT 'gold.calendario_caso', 'min|max', 'volume_postagens', CONCAT(CAST(MIN(volume_postagens) AS STRING), ' | ', CAST(MAX(volume_postagens) AS STRING)) FROM scapegoat.gold.calendario_caso
  UNION ALL SELECT 'gold.dim_conta_papel', 'min|max', 'n_mencoes_recebidas_caso', CONCAT(CAST(MIN(n_mencoes_recebidas_caso) AS STRING), ' | ', CAST(MAX(n_mencoes_recebidas_caso) AS STRING)) FROM scapegoat.gold.dim_conta_papel
  UNION ALL SELECT 'gold.dim_conta_papel', 'min|max', 'n_postagens_caso', CONCAT(CAST(MIN(n_postagens_caso) AS STRING), ' | ', CAST(MAX(n_postagens_caso) AS STRING)) FROM scapegoat.gold.dim_conta_papel
  UNION ALL SELECT 'gold.dim_conta_papel', 'min|max', 'primeiro_dia', CONCAT(CAST(MIN(primeiro_dia) AS STRING), ' | ', CAST(MAX(primeiro_dia) AS STRING)) FROM scapegoat.gold.dim_conta_papel
  UNION ALL SELECT 'gold.dim_conta_papel', 'min|max', 'ultimo_dia', CONCAT(CAST(MIN(ultimo_dia) AS STRING), ' | ', CAST(MAX(ultimo_dia) AS STRING)) FROM scapegoat.gold.dim_conta_papel
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'likes', CONCAT(CAST(MIN(likes) AS STRING), ' | ', CAST(MAX(likes) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'n_autores_distintos', CONCAT(CAST(MIN(n_autores_distintos) AS STRING), ' | ', CAST(MAX(n_autores_distintos) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'n_postagens', CONCAT(CAST(MIN(n_postagens) AS STRING), ' | ', CAST(MAX(n_postagens) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'quotes', CONCAT(CAST(MIN(quotes) AS STRING), ' | ', CAST(MAX(quotes) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'respostas', CONCAT(CAST(MIN(respostas) AS STRING), ' | ', CAST(MAX(respostas) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_atividade', 'min|max', 'retweets', CONCAT(CAST(MIN(retweets) AS STRING), ' | ', CAST(MAX(retweets) AS STRING)) FROM scapegoat.gold.fato_atividade
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'assortatividade_stance', CONCAT(COALESCE(CAST(ROUND(MIN(assortatividade_stance), 6) AS STRING), 'NULL'), ' | ', COALESCE(CAST(ROUND(MAX(assortatividade_stance), 6) AS STRING), 'NULL')) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'centralizacao_grau_entrada', CONCAT(CAST(ROUND(MIN(centralizacao_grau_entrada), 6) AS STRING), ' | ', CAST(ROUND(MAX(centralizacao_grau_entrada), 6) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'densidade', CONCAT(CAST(ROUND(MIN(densidade), 6) AS STRING), ' | ', CAST(ROUND(MAX(densidade), 6) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'gini_mencoes_recebidas', CONCAT(CAST(ROUND(MIN(gini_mencoes_recebidas), 6) AS STRING), ' | ', CAST(ROUND(MAX(gini_mencoes_recebidas), 6) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'hhi_mencoes_recebidas', CONCAT(CAST(ROUND(MIN(hhi_mencoes_recebidas), 6) AS STRING), ' | ', CAST(ROUND(MAX(hhi_mencoes_recebidas), 6) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'isolamento_alvo', CONCAT(CAST(ROUND(MIN(isolamento_alvo), 6) AS STRING), ' | ', CAST(ROUND(MAX(isolamento_alvo), 6) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'n_arestas', CONCAT(CAST(MIN(n_arestas) AS STRING), ' | ', CAST(MAX(n_arestas) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'n_mencoes', CONCAT(CAST(MIN(n_mencoes) AS STRING), ' | ', CAST(MAX(n_mencoes) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_rede', 'min|max', 'n_nos', CONCAT(CAST(MIN(n_nos) AS STRING), ' | ', CAST(MAX(n_nos) AS STRING)) FROM scapegoat.gold.fato_rede
  UNION ALL SELECT 'gold.fato_referencia', 'min|max', 'n_autores_distintos', CONCAT(CAST(MIN(n_autores_distintos) AS STRING), ' | ', CAST(MAX(n_autores_distintos) AS STRING)) FROM scapegoat.gold.fato_referencia
  UNION ALL SELECT 'gold.fato_referencia', 'min|max', 'n_postagens', CONCAT(CAST(MIN(n_postagens) AS STRING), ' | ', CAST(MAX(n_postagens) AS STRING)) FROM scapegoat.gold.fato_referencia
  UNION ALL SELECT 'gold.grafo_arestas', 'min|max', 'peso', CONCAT(CAST(MIN(peso) AS STRING), ' | ', CAST(MAX(peso) AS STRING)) FROM scapegoat.gold.grafo_arestas
  UNION ALL SELECT 'gold.papel_narrativo_v0', 'min|max', 'decidido_em', CONCAT(CAST(MIN(decidido_em) AS STRING), ' | ', CAST(MAX(decidido_em) AS STRING)) FROM scapegoat.gold.papel_narrativo_v0
  UNION ALL SELECT 'gold.papel_narrativo_v0', 'min|max', 'dia_virada', CONCAT(CAST(MIN(dia_virada) AS STRING), ' | ', CAST(MAX(dia_virada) AS STRING)) FROM scapegoat.gold.papel_narrativo_v0
)
SELECT * FROM linhas
UNION ALL SELECT * FROM somas
UNION ALL SELECT * FROM classif
UNION ALL SELECT * FROM versoes
UNION ALL SELECT * FROM dominios
ORDER BY tabela, metrica, chave;