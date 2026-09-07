-- ============================================================
-- Gold — dim_conta_papel (07/09/2026)
-- Grão: caso × conta_id. Contas de um caso = autores ∪ mencionados.
-- Sem handle. Alvos declarados por conta_id (identificados pelo handle
-- em silver.conta; não postam no corpus).
-- ============================================================
USE CATALOG scapegoat;

CREATE TABLE IF NOT EXISTS gold.dim_conta_papel (
    caso STRING NOT NULL,
    conta_id BIGINT NOT NULL,
    papel_principal STRING NOT NULL,   -- alvo | demais (v2: lider_acusacao, vitima_secundaria, instituicao_legitimadora)
    n_postagens_caso INT,
    n_mencoes_recebidas_caso INT,
    primeiro_dia DATE,
    ultimo_dia DATE,
    CONSTRAINT pk_dim_conta_papel PRIMARY KEY (caso, conta_id)
)
USING DELTA;

INSERT OVERWRITE gold.dim_conta_papel
WITH alvos AS (
    SELECT 'monark' AS caso, 5238 AS conta_id
    UNION ALL
    SELECT 'arthur_do_val', 5046
),
autoria AS (
    SELECT caso_slug AS caso, autor_conta_id AS conta_id,
           COUNT(*) AS n_postagens,
           MIN(DATE(created_at)) AS primeiro_dia,
           MAX(DATE(created_at)) AS ultimo_dia
    FROM silver.postagem
    WHERE caso_slug IN ('monark', 'arthur_do_val')
    GROUP BY 1, 2
),
mencionados AS (
    SELECT p.caso_slug AS caso, m.conta_id,
           COUNT(*) AS n_mencoes,
           MIN(DATE(p.created_at)) AS primeiro_dia,
           MAX(DATE(p.created_at)) AS ultimo_dia
    FROM silver.mencao m
    JOIN silver.postagem p ON p.postagem_id = m.postagem_id
    WHERE p.caso_slug IN ('monark', 'arthur_do_val')
    GROUP BY 1, 2
),
contas_caso AS (
    SELECT caso, conta_id FROM autoria
    UNION
    SELECT caso, conta_id FROM mencionados
)
SELECT
    cc.caso,
    cc.conta_id,
    CASE WHEN a.conta_id IS NOT NULL THEN 'alvo' ELSE 'demais' END AS papel_principal,
    COALESCE(au.n_postagens, 0) AS n_postagens_caso,
    COALESCE(me.n_mencoes, 0) AS n_mencoes_recebidas_caso,
    LEAST(COALESCE(au.primeiro_dia, me.primeiro_dia), COALESCE(me.primeiro_dia, au.primeiro_dia)) AS primeiro_dia,
    GREATEST(COALESCE(au.ultimo_dia, me.ultimo_dia), COALESCE(me.ultimo_dia, au.ultimo_dia)) AS ultimo_dia
FROM contas_caso cc
LEFT JOIN alvos a        ON a.caso  = cc.caso AND a.conta_id  = cc.conta_id
LEFT JOIN autoria au     ON au.caso = cc.caso AND au.conta_id = cc.conta_id
LEFT JOIN mencionados me ON me.caso = cc.caso AND me.conta_id = cc.conta_id;

-- Validação: 1 alvo por caso, com 0 postagens como autor
SELECT caso, papel_principal, COUNT(*) AS n_contas,
       SUM(n_postagens_caso) AS postagens, SUM(n_mencoes_recebidas_caso) AS mencoes
FROM gold.dim_conta_papel GROUP BY 1, 2 ORDER BY 1, 2;

SELECT * FROM gold.dim_conta_papel WHERE papel_principal = 'alvo';
