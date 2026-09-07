-- ============================================================
-- Catálogo — complemento: gold.grafo_arestas e camada pub
-- Rodar depois de gold_catalogo_comments.sql
-- ============================================================
USE CATALOG scapegoat;

-- ------------------------------------------------------------
-- gold.grafo_arestas
-- ------------------------------------------------------------
COMMENT ON TABLE gold.grafo_arestas IS
'PURPOSE: lista de arestas do grafo de menções de cada caso no grão mais fino, para exportação (Gephi, site) e para qualquer recorte de janela (caso inteiro, fase, dia, intervalo de dias_desde_estopim) por WHERE + SUM(peso). É a fonte das figuras de grafo; fato_rede guarda as métricas, esta tabela guarda a estrutura.
GRAIN: 1 linha por caso × data × origem_conta_id → destino_conta_id (aresta dirigida do autor para a conta mencionada).
LINEAGE: silver.mencao ⋈ silver.postagem (arestas) + silver.classificacao (stance da postagem) + gold.dim_conta_papel (papéis) + gold.calendario_caso (fase).
PRIVACIDADE: só conta_id. A pseudonimização acontece em pub.v_grafo_arestas / pub.v_grafo_nos.
CONFERÊNCIA: Σ peso = 5.868 (monark) / 22.954 (arthur_do_val) = silver.mencao; nº de pares = n_arestas de fato_rede; o alvo nunca é origem.';

ALTER TABLE gold.grafo_arestas ALTER COLUMN caso COMMENT 'Slug do caso.';
ALTER TABLE gold.grafo_arestas ALTER COLUMN data COMMENT 'Dia da(s) postagem(ns) que geraram a aresta.';
ALTER TABLE gold.grafo_arestas ALTER COLUMN dias_desde_estopim COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE gold.grafo_arestas ALTER COLUMN fase COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE gold.grafo_arestas ALTER COLUMN origem_conta_id COMMENT 'Autor da postagem (silver.postagem.autor_conta_id).';
ALTER TABLE gold.grafo_arestas ALTER COLUMN destino_conta_id COMMENT 'Conta mencionada (silver.mencao.conta_id).';
ALTER TABLE gold.grafo_arestas ALTER COLUMN peso COMMENT 'Número de menções origem→destino no dia. Aditiva: somar ao agregar janelas.';
ALTER TABLE gold.grafo_arestas ALTER COLUMN stance_origem COMMENT 'Stance mais frequente entre as postagens que geraram a aresta no dia (acusador | defensor | neutro | sem_rotulo).';
ALTER TABLE gold.grafo_arestas ALTER COLUMN papel_origem COMMENT 'Papel da origem em dim_conta_papel (alvo | demais).';
ALTER TABLE gold.grafo_arestas ALTER COLUMN papel_destino COMMENT 'Papel do destino em dim_conta_papel (alvo | demais). Filtrar destino = alvo dá o subgrafo de ataque/defesa à vítima.';

ALTER TABLE gold.grafo_arestas SET TAGS ('layer' = 'gold', 'classification' = 'pseudonimizado', 'owner' = 'carlos', 'domain' = 'scapegoating', 'refresh' = 'manual');

-- ------------------------------------------------------------
-- pub — views de publicação
-- ------------------------------------------------------------
COMMENT ON VIEW pub.v_grafo_arestas IS
'PURPOSE: arestas prontas para exportação (Gephi: source, target, weight; site: GEXF/JSON), com conta_id substituído por pseudônimo estável.
PSEUDONIMIZAÇÃO: n + 10 hex de SHA-256(sal || conta_id). Mesma conta → mesmo pseudônimo em qualquer caso, dia ou export. Irreversível sem silver.conta (acesso restrito). Nomes públicos, quando decididos, entrarão por tabela de rótulos (gold.rotulo_publico, v2) sem alterar esta view.
GRAIN: idêntico a gold.grafo_arestas. Agregar por (source, target) com SUM(weight) para obter o grafo de uma janela.';

COMMENT ON VIEW pub.v_grafo_nos IS
'PURPOSE: tabela de nós para exportação, com atributos para colorir (papel, stance_modal) e dimensionar (n_mencoes_recebidas_caso, n_postagens_caso).
GRAIN: 1 linha por caso × conta; id = pseudônimo (o mesmo de v_grafo_arestas). 16.932 linhas, 16.399 pseudônimos distintos: 533 contas participam dos dois casos.
LINEAGE: gold.dim_conta_papel + stance modal do autor em silver.classificacao. stance_modal = nao_autor para contas só mencionadas (ex.: o alvo).';

-- Conferência
SELECT table_schema, table_name, table_type, comment
FROM system.information_schema.tables
WHERE table_catalog = 'scapegoat' AND table_schema IN ('gold', 'pub')
ORDER BY table_schema, table_name;
