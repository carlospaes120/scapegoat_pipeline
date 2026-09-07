-- ============================================================
-- BLOCO 3 — Catálogo da Gold (Unity Catalog)
-- Comentários de tabela (PURPOSE / GRAIN / LINEAGE) e de coluna,
-- mais tags de governança. Rodar inteiro no SQL Editor.
-- ============================================================
USE CATALOG scapegoat;
USE SCHEMA gold;

-- ------------------------------------------------------------
-- 1. calendario_caso
-- ------------------------------------------------------------
COMMENT ON TABLE calendario_caso IS
'PURPOSE: eixo temporal normalizado de cada caso — traduz a data-calendário em dias desde o estopim e em fase da crise, para permitir comparação entre casos sem usar volume absoluto.
GRAIN: 1 linha por caso × data com pelo menos 1 postagem coletada.
LINEAGE: silver.postagem (volume diário) + parâmetros declarados (estopim por caso: monark 2022-02-08, arthur_do_val 2022-03-04).
REGRAS: pico = dia de maior volume a partir do estopim; declinio = volume >= 25% do pico; pos_rito = abaixo de 25%; pre_crise = dias anteriores ao estopim (mantidos, rotulados). Regra avaliada dia a dia, não monotônica.
VERSAO: 2026-09-07 (correção: pico e limiar restritos à janela >= estopim).';

ALTER TABLE calendario_caso ALTER COLUMN caso COMMENT 'Slug do caso (monark | arthur_do_val). Chave de negócio, igual a silver.postagem.caso_slug.';
ALTER TABLE calendario_caso ALTER COLUMN data COMMENT 'Data-calendário (UTC) das postagens.';
ALTER TABLE calendario_caso ALTER COLUMN dias_desde_estopim COMMENT 'data − data_estopim, em dias. 0 = estopim; negativo = pré-crise. Eixo de comparação entre casos.';
ALTER TABLE calendario_caso ALTER COLUMN fase COMMENT 'pre_crise | estopim | escalada | pico | declinio | pos_rito. Derivada do volume diário (ver comentário da tabela).';
ALTER TABLE calendario_caso ALTER COLUMN volume_postagens COMMENT 'Número de postagens do caso no dia (silver.postagem). Medida aditiva.';

ALTER TABLE calendario_caso SET TAGS ('layer' = 'gold', 'classification' = 'anonimizado', 'owner' = 'carlos', 'domain' = 'scapegoating', 'refresh' = 'manual');

-- ------------------------------------------------------------
-- 2. dim_conta_papel
-- ------------------------------------------------------------
COMMENT ON TABLE dim_conta_papel IS
'PURPOSE: papel de cada conta dentro de cada caso (alvo do bode expiatório vs demais), com contadores de participação. Base para isolamento_alvo em fato_rede e para os papéis narrativos futuros.
GRAIN: 1 linha por caso × conta_id. Contas de um caso = autores de postagens do caso ∪ contas mencionadas em postagens do caso.
LINEAGE: silver.postagem (autoria) + silver.mencao ⋈ silver.postagem (menções) + lista declarada de alvos (monark = conta 5238; arthur_do_val = conta 5046).
PRIVACIDADE: não carrega handle nem texto — apenas conta_id (chave surrogate da Silver).
NOTA: os alvos não postam no corpus (n_postagens_caso = 0); existem no grafo apenas como mencionados.';

ALTER TABLE dim_conta_papel ALTER COLUMN caso COMMENT 'Slug do caso (monark | arthur_do_val).';
ALTER TABLE dim_conta_papel ALTER COLUMN conta_id COMMENT 'Chave surrogate da conta em silver.conta. Sem handle na Gold.';
ALTER TABLE dim_conta_papel ALTER COLUMN papel_principal COMMENT 'alvo | demais no MVP. Reservado para v2: lider_acusacao, vitima_secundaria, instituicao_legitimadora.';
ALTER TABLE dim_conta_papel ALTER COLUMN n_postagens_caso COMMENT 'Postagens da conta como autora, dentro do caso.';
ALTER TABLE dim_conta_papel ALTER COLUMN n_mencoes_recebidas_caso COMMENT 'Menções recebidas pela conta em postagens do caso (silver.mencao).';
ALTER TABLE dim_conta_papel ALTER COLUMN primeiro_dia COMMENT 'Primeira data em que a conta aparece no caso (como autora ou mencionada).';
ALTER TABLE dim_conta_papel ALTER COLUMN ultimo_dia COMMENT 'Última data em que a conta aparece no caso.';

ALTER TABLE dim_conta_papel SET TAGS ('layer' = 'gold', 'classification' = 'pseudonimizado', 'owner' = 'carlos', 'domain' = 'scapegoating', 'refresh' = 'manual');

-- ------------------------------------------------------------
-- 3. fato_atividade
-- ------------------------------------------------------------
COMMENT ON TABLE fato_atividade IS
'PURPOSE: atividade diária de cada caso decomposta por posição (stance) e por mecanismo de referência. Responde P1–P4 (volume por fase, proporção acusador/defensor/neutro, uso de reply/quote, engajamento).
GRAIN: 1 linha por caso × data × stance × tipo_ref × versao_classificacao.
LINEAGE: silver.postagem ⋈ silver.captura (contadores) ⋈ silver.classificacao (rótulo de stance, modelo bertimbau-base-stance) ⋈ gold.calendario_caso (dias_desde_estopim, fase).
MEDIDAS: n_postagens, likes, retweets, quotes, respostas são aditivas. n_autores_distintos é semi-aditiva (não somar entre linhas).
VERSIONAMENTO: reclassificar não sobrescreve — nova versao_classificacao gera novas linhas.';

ALTER TABLE fato_atividade ALTER COLUMN caso COMMENT 'Slug do caso.';
ALTER TABLE fato_atividade ALTER COLUMN data COMMENT 'Data-calendário das postagens.';
ALTER TABLE fato_atividade ALTER COLUMN dias_desde_estopim COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE fato_atividade ALTER COLUMN fase COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE fato_atividade ALTER COLUMN stance COMMENT 'acusador | defensor | neutro (silver.classificacao.rotulo). sem_rotulo se a postagem não foi classificada.';
ALTER TABLE fato_atividade ALTER COLUMN tipo_ref COMMENT 'Mecanismo da postagem: original | reply | quote (silver.postagem.tipo_ref).';
ALTER TABLE fato_atividade ALTER COLUMN versao_classificacao COMMENT 'Commit/versão do classificador que produziu o rótulo (silver.classificacao.versao).';
ALTER TABLE fato_atividade ALTER COLUMN n_postagens COMMENT 'Número de postagens no grão. Aditiva.';
ALTER TABLE fato_atividade ALTER COLUMN n_autores_distintos COMMENT 'Autores distintos no grão. SEMI-ADITIVA: não somar entre stances/dias.';
ALTER TABLE fato_atividade ALTER COLUMN likes COMMENT 'Soma de likes capturados (máximo por postagem entre capturas). Aditiva.';
ALTER TABLE fato_atividade ALTER COLUMN retweets COMMENT 'Soma de retweets. Aditiva.';
ALTER TABLE fato_atividade ALTER COLUMN quotes COMMENT 'Soma de quotes recebidos. Aditiva.';
ALTER TABLE fato_atividade ALTER COLUMN respostas COMMENT 'Soma de respostas recebidas. Aditiva.';

ALTER TABLE fato_atividade SET TAGS ('layer' = 'gold', 'classification' = 'anonimizado', 'owner' = 'carlos', 'domain' = 'scapegoating', 'refresh' = 'manual');

-- ------------------------------------------------------------
-- 4. fato_rede
-- ------------------------------------------------------------
COMMENT ON TABLE fato_rede IS
'PURPOSE: snapshot diário da estrutura do grafo de menções de cada caso — concentração, centralização e isolamento do alvo. Responde P5–P9.
GRAIN: 1 linha por caso × data × versao_pipeline. Grafo do dia: nós = autores ∪ mencionados nas postagens do dia; arestas = pares autor→mencionado, peso = nº de menções.
LINEAGE: silver.mencao ⋈ silver.postagem (arestas) + gold.dim_conta_papel (alvo) + gold.calendario_caso (fase).
NATUREZA: tabela de snapshot — medidas NÃO aditivas; nunca somar entre dias. Comparar entre casos apenas por dias_desde_estopim e por medida normalizada.
CUIDADO: dias com poucos nós (ex.: monark 2022-02-07, 2 nós) produzem métricas degeneradas; recomenda-se mínimo de ~30 nós na análise.
LIMITAÇÃO MVP: assortatividade_stance é NULL (exige stance de nós não-autores). Modularidade/clustering ficam para v2 (NetworkX).';

ALTER TABLE fato_rede ALTER COLUMN caso COMMENT 'Slug do caso.';
ALTER TABLE fato_rede ALTER COLUMN data COMMENT 'Dia do snapshot do grafo.';
ALTER TABLE fato_rede ALTER COLUMN dias_desde_estopim COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE fato_rede ALTER COLUMN fase COMMENT 'Copiado de gold.calendario_caso.';
ALTER TABLE fato_rede ALTER COLUMN versao_pipeline COMMENT 'Versão do cálculo de rede. Recalcular não sobrescreve: gera nova versão.';
ALTER TABLE fato_rede ALTER COLUMN n_nos COMMENT 'Contas no grafo do dia (autores ∪ mencionados).';
ALTER TABLE fato_rede ALTER COLUMN n_arestas COMMENT 'Pares distintos autor→mencionado no dia.';
ALTER TABLE fato_rede ALTER COLUMN n_mencoes COMMENT 'Soma dos pesos das arestas (= menções do dia em silver.mencao).';
ALTER TABLE fato_rede ALTER COLUMN densidade COMMENT 'n_arestas / (n_nos·(n_nos−1)). Grafo dirigido.';
ALTER TABLE fato_rede ALTER COLUMN gini_mencoes_recebidas COMMENT 'Gini da distribuição de menções recebidas entre os nós do dia (0 = igualdade, 1 = tudo em um nó).';
ALTER TABLE fato_rede ALTER COLUMN hhi_mencoes_recebidas COMMENT 'Herfindahl: soma dos quadrados das participações de cada nó nas menções recebidas (1/n_nos … 1).';
ALTER TABLE fato_rede ALTER COLUMN centralizacao_grau_entrada COMMENT 'Centralização de Freeman por grau de entrada não ponderado: Σ(grau_max − grau_i) / (n−1)². 1 = estrela perfeita.';
ALTER TABLE fato_rede ALTER COLUMN isolamento_alvo COMMENT 'Menções ao alvo (dim_conta_papel.papel_principal = alvo) / menções do dia. Proxy do foco da multidão na vítima.';
ALTER TABLE fato_rede ALTER COLUMN assortatividade_stance COMMENT 'NULL no MVP. Reservado para homofilia de stance nas arestas (v2).';

ALTER TABLE fato_rede SET TAGS ('layer' = 'gold', 'classification' = 'anonimizado', 'owner' = 'carlos', 'domain' = 'scapegoating', 'refresh' = 'manual');

-- ------------------------------------------------------------
-- 5. Conferência: os comentários entraram?
-- ------------------------------------------------------------
SELECT table_name, comment
FROM system.information_schema.tables
WHERE table_catalog = 'scapegoat' AND table_schema = 'gold'
ORDER BY table_name;

SELECT table_name, column_name, comment
FROM system.information_schema.columns
WHERE table_catalog = 'scapegoat' AND table_schema = 'gold'
ORDER BY table_name, ordinal_position;
