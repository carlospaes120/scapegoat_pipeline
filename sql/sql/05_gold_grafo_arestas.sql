-- ============================================================
-- BLOCO 3 (complemento) — gold.grafo_arestas + camada pub
--
-- grafo_arestas: lista de arestas do grafo de menções no grão mais fino
--   (caso × dia × origem → destino). Qualquer janela (caso inteiro, fase,
--   dia, intervalo de dias_desde_estopim) é um WHERE + SUM(peso) sobre ela.
-- pub.v_grafo_arestas / pub.v_grafo_nos: as mesmas informações com
--   conta_id trocado por pseudônimo estável (hash). É o que sai para
--   Gephi (CSV) e para o site (GEXF/JSON).
--
-- Rodar inteiro no SQL Editor.
-- ============================================================
USE CATALOG scapegoat;

-- ------------------------------------------------------------
-- 1. Tabela Gold (só conta_id, sem handle)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.grafo_arestas (
    caso STRING NOT NULL,
    data DATE NOT NULL,
    dias_desde_estopim INT,
    fase STRING,
    origem_conta_id BIGINT NOT NULL,     -- autor da postagem
    destino_conta_id BIGINT NOT NULL,    -- conta mencionada
    peso INT,                            -- nº de menções origem→destino no dia
    stance_origem STRING,                -- stance modal das postagens que geraram a aresta
    papel_origem STRING,                 -- de dim_conta_papel
    papel_destino STRING,                -- de dim_conta_papel (alvo | demais)
    CONSTRAINT pk_grafo_arestas PRIMARY KEY (caso, data, origem_conta_id, destino_conta_id)
)
USING DELTA;

INSERT OVERWRITE gold.grafo_arestas
WITH cls AS (
    SELECT postagem_id, rotulo
    FROM silver.classificacao
    WHERE esquema = (SELECT MAX(esquema) FROM silver.classificacao)
),
mencoes AS (
    SELECT p.caso_slug AS caso, DATE(p.created_at) AS data,
           p.autor_conta_id AS origem, m.conta_id AS destino,
           COALESCE(c.rotulo, 'sem_rotulo') AS stance
    FROM silver.mencao m
    JOIN silver.postagem p ON p.postagem_id = m.postagem_id
    LEFT JOIN cls c ON c.postagem_id = p.postagem_id
    WHERE p.caso_slug IN ('monark', 'arthur_do_val')
      AND p.autor_conta_id IS NOT NULL AND m.conta_id IS NOT NULL
),
por_stance AS (
    SELECT caso, data, origem, destino, stance, COUNT(*) AS n
    FROM mencoes GROUP BY 1, 2, 3, 4, 5
),
modal AS (
    SELECT caso, data, origem, destino, stance AS stance_origem,
           SUM(n) OVER (PARTITION BY caso, data, origem, destino) AS peso,
           ROW_NUMBER() OVER (PARTITION BY caso, data, origem, destino
                              ORDER BY n DESC, stance) AS rn
    FROM por_stance
)
SELECT
    md.caso, md.data,
    cal.dias_desde_estopim, cal.fase,
    md.origem  AS origem_conta_id,
    md.destino AS destino_conta_id,
    CAST(md.peso AS INT) AS peso,
    md.stance_origem,
    COALESCE(po.papel_principal, 'demais') AS papel_origem,
    COALESCE(pd.papel_principal, 'demais') AS papel_destino
FROM modal md
LEFT JOIN gold.calendario_caso cal ON cal.caso = md.caso AND cal.data = md.data
LEFT JOIN gold.dim_conta_papel po ON po.caso = md.caso AND po.conta_id = md.origem
LEFT JOIN gold.dim_conta_papel pd ON pd.caso = md.caso AND pd.conta_id = md.destino
WHERE md.rn = 1;

-- ------------------------------------------------------------
-- 2. Camada pub — pseudonimização estável por hash
--    O pseudônimo é o mesmo para a mesma conta em qualquer caso, dia ou
--    export. Não é reversível sem silver.conta (acesso restrito).
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS pub;

CREATE OR REPLACE VIEW pub.v_grafo_arestas AS
SELECT
    caso, data, dias_desde_estopim, fase,
    CONCAT('n', SUBSTR(SHA2(CONCAT('scapegoat-pub-v1|', origem_conta_id), 256), 1, 10))  AS source,
    CONCAT('n', SUBSTR(SHA2(CONCAT('scapegoat-pub-v1|', destino_conta_id), 256), 1, 10)) AS target,
    peso AS weight,
    stance_origem, papel_origem, papel_destino
FROM gold.grafo_arestas;

-- Nós: um por caso × conta, com atributos para colorir/dimensionar no Gephi
CREATE OR REPLACE VIEW pub.v_grafo_nos AS
WITH stance_autor AS (
    SELECT p.caso_slug AS caso, p.autor_conta_id AS conta_id, c.rotulo,
           ROW_NUMBER() OVER (PARTITION BY p.caso_slug, p.autor_conta_id
                              ORDER BY COUNT(*) DESC, c.rotulo) AS rn
    FROM silver.postagem p
    JOIN silver.classificacao c ON c.postagem_id = p.postagem_id
    WHERE p.caso_slug IN ('monark', 'arthur_do_val')
    GROUP BY 1, 2, 3
)
SELECT
    d.caso,
    CONCAT('n', SUBSTR(SHA2(CONCAT('scapegoat-pub-v1|', d.conta_id), 256), 1, 10)) AS id,
    d.papel_principal AS papel,
    COALESCE(sa.rotulo, 'nao_autor') AS stance_modal,   -- nao_autor = só mencionado (ex.: o alvo)
    d.n_postagens_caso,
    d.n_mencoes_recebidas_caso,
    d.primeiro_dia, d.ultimo_dia
FROM gold.dim_conta_papel d
LEFT JOIN stance_autor sa ON sa.caso = d.caso AND sa.conta_id = d.conta_id AND sa.rn = 1;

-- ------------------------------------------------------------
-- 3. Validação
-- ------------------------------------------------------------
-- (a) soma dos pesos = menções da Silver (5.868 / 22.954); pares = arestas da fato_rede (4.899 / 17.585)
SELECT caso, SUM(peso) AS mencoes, COUNT(*) AS pares
FROM gold.grafo_arestas GROUP BY 1;

-- (b) o alvo só aparece como destino, nunca como origem
SELECT caso,
       SUM(CASE WHEN papel_origem  = 'alvo' THEN 1 ELSE 0 END) AS alvo_como_origem,
       SUM(CASE WHEN papel_destino = 'alvo' THEN peso ELSE 0 END) AS mencoes_ao_alvo
FROM gold.grafo_arestas GROUP BY 1;

-- (c) pseudônimo é único por conta
SELECT COUNT(*) AS nos, COUNT(DISTINCT id) AS ids_distintos FROM pub.v_grafo_nos;

-- ------------------------------------------------------------
-- 4. Exemplos de exportação (baixar o resultado como CSV no botão ↓)
-- ------------------------------------------------------------
-- Grafo de uma fase (Gephi: Source, Target, Weight + atributos)
-- SELECT source, target, SUM(weight) AS weight,
--        MAX(stance_origem) AS stance_origem, MAX(papel_destino) AS papel_destino
-- FROM pub.v_grafo_arestas
-- WHERE caso = 'monark' AND fase = 'pico'
-- GROUP BY 1, 2;

-- Grafo dinâmico do caso inteiro (Gephi: importar 'data' como Timestamp)
-- SELECT source, target, weight, data AS timestamp, stance_origem, papel_destino
-- FROM pub.v_grafo_arestas
-- WHERE caso = 'arthur_do_val';

-- Tabela de nós do caso (Gephi: Id + atributos)
-- SELECT id, papel, stance_modal, n_postagens_caso, n_mencoes_recebidas_caso
-- FROM pub.v_grafo_nos WHERE caso = 'monark';
