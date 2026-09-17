-- ============================================================
-- Gold — fato_rede (07/09/2026)
-- Grão: caso × data × versao_pipeline. Snapshot diário do grafo de
-- menções (nós = autores ∪ mencionados; arestas = autor→mencionado).
-- Responde P5–P9. Medidas NÃO aditivas.
-- ============================================================
USE CATALOG scapegoat;

CREATE TABLE IF NOT EXISTS gold.fato_rede (
    caso STRING NOT NULL,
    data DATE NOT NULL,
    dias_desde_estopim INT,
    fase STRING,
    versao_pipeline STRING NOT NULL,
    n_nos INT,
    n_arestas INT,
    n_mencoes INT,
    densidade DOUBLE,
    gini_mencoes_recebidas DOUBLE,
    hhi_mencoes_recebidas DOUBLE,
    centralizacao_grau_entrada DOUBLE,
    isolamento_alvo DOUBLE,
    assortatividade_stance DOUBLE,        -- NULL no MVP
    CONSTRAINT pk_fato_rede PRIMARY KEY (caso, data, versao_pipeline)
)
USING DELTA;

INSERT OVERWRITE gold.fato_rede
WITH arestas AS (
    SELECT p.caso_slug AS caso, DATE(p.created_at) AS data,
           p.autor_conta_id AS origem, m.conta_id AS destino,
           COUNT(*) AS peso
    FROM silver.mencao m
    JOIN silver.postagem p ON p.postagem_id = m.postagem_id
    WHERE p.caso_slug IN ('monark', 'arthur_do_val')
      AND p.autor_conta_id IS NOT NULL AND m.conta_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
nos AS (
    SELECT caso, data, origem AS conta_id FROM arestas
    UNION
    SELECT caso, data, destino FROM arestas
),
grau AS (
    SELECT n.caso, n.data, n.conta_id,
           COALESCE(SUM(a.peso), 0) AS mencoes_recebidas,
           COUNT(a.origem)          AS grau_entrada
    FROM nos n
    LEFT JOIN arestas a ON a.caso = n.caso AND a.data = n.data AND a.destino = n.conta_id
    GROUP BY 1, 2, 3
),
ranqueado AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY caso, data ORDER BY mencoes_recebidas, conta_id) AS i,
           COUNT(*)  OVER (PARTITION BY caso, data) AS n,
           SUM(mencoes_recebidas) OVER (PARTITION BY caso, data) AS total,
           MAX(grau_entrada) OVER (PARTITION BY caso, data) AS grau_max
    FROM grau
),
metricas_nos AS (
    SELECT caso, data,
           MAX(n)     AS n_nos,
           MAX(total) AS n_mencoes,
           (2.0 * SUM(i * mencoes_recebidas)) / (MAX(n) * MAX(total)) - (MAX(n) + 1.0) / MAX(n) AS gini,
           SUM(POWER(mencoes_recebidas / total, 2)) AS hhi,
           SUM(grau_max - grau_entrada) / POWER(MAX(n) - 1, 2) AS centralizacao
    FROM ranqueado
    GROUP BY 1, 2
),
metricas_arestas AS (
    SELECT caso, data, COUNT(*) AS n_arestas FROM arestas GROUP BY 1, 2
),
alvo AS (
    SELECT a.caso, a.data, SUM(a.peso) AS mencoes_ao_alvo
    FROM arestas a
    JOIN gold.dim_conta_papel d
      ON d.caso = a.caso AND d.conta_id = a.destino AND d.papel_principal = 'alvo'
    GROUP BY 1, 2
)
SELECT
    mn.caso, mn.data,
    cal.dias_desde_estopim, cal.fase,
    'gold-v1' AS versao_pipeline,
    mn.n_nos, ma.n_arestas, mn.n_mencoes,
    ma.n_arestas / (mn.n_nos * (mn.n_nos - 1.0)) AS densidade,
    mn.gini AS gini_mencoes_recebidas,
    mn.hhi  AS hhi_mencoes_recebidas,
    mn.centralizacao AS centralizacao_grau_entrada,
    COALESCE(al.mencoes_ao_alvo, 0) / mn.n_mencoes AS isolamento_alvo,
    CAST(NULL AS DOUBLE) AS assortatividade_stance
FROM metricas_nos mn
JOIN metricas_arestas ma ON ma.caso = mn.caso AND ma.data = mn.data
LEFT JOIN alvo al        ON al.caso = mn.caso AND al.data = mn.data
LEFT JOIN gold.calendario_caso cal ON cal.caso = mn.caso AND cal.data = mn.data;

-- Validação: Σ n_mencoes = silver.mencao (5.868 / 22.954)
SELECT caso, SUM(n_mencoes) AS mencoes, SUM(n_arestas) AS arestas, MAX(n_nos) AS max_nos
FROM gold.fato_rede GROUP BY 1;

SELECT caso, data, dias_desde_estopim, fase, n_nos, n_arestas,
       ROUND(densidade, 5) AS dens, ROUND(gini_mencoes_recebidas, 3) AS gini,
       ROUND(hhi_mencoes_recebidas, 3) AS hhi, ROUND(centralizacao_grau_entrada, 3) AS centr,
       ROUND(isolamento_alvo, 3) AS isol_alvo
FROM gold.fato_rede ORDER BY caso, data;
