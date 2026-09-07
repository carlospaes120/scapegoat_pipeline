-- ============================================================
-- Gold — fato_atividade (07/09/2026)
-- Grão: caso × data × stance × tipo_ref × versao_classificacao.
-- Responde P1–P4. Recarregar sempre que calendario_caso mudar.
-- ============================================================
USE CATALOG scapegoat;

CREATE TABLE IF NOT EXISTS gold.fato_atividade (
    caso STRING NOT NULL,
    data DATE NOT NULL,
    dias_desde_estopim INT,
    fase STRING,
    stance STRING NOT NULL,
    tipo_ref STRING NOT NULL,
    versao_classificacao STRING NOT NULL,
    n_postagens INT,
    n_autores_distintos INT,             -- semi-aditiva
    likes BIGINT,
    retweets BIGINT,
    quotes BIGINT,
    respostas BIGINT,
    CONSTRAINT pk_fato_atividade PRIMARY KEY (caso, data, stance, tipo_ref, versao_classificacao)
)
USING DELTA;

INSERT OVERWRITE gold.fato_atividade
WITH cap AS (
    SELECT postagem_id,
           MAX(likes) AS likes, MAX(retweets) AS retweets,
           MAX(quotes) AS quotes, MAX(respostas) AS respostas
    FROM silver.captura
    GROUP BY postagem_id
),
cls AS (
    SELECT postagem_id, rotulo, versao
    FROM silver.classificacao
    WHERE esquema = (SELECT MAX(esquema) FROM silver.classificacao)
)
SELECT
    p.caso_slug AS caso,
    DATE(p.created_at) AS data,
    cal.dias_desde_estopim,
    cal.fase,
    COALESCE(c.rotulo, 'sem_rotulo') AS stance,
    COALESCE(p.tipo_ref, 'desconhecido') AS tipo_ref,
    COALESCE(c.versao, 'nenhuma') AS versao_classificacao,
    COUNT(*) AS n_postagens,
    COUNT(DISTINCT p.autor_conta_id) AS n_autores_distintos,
    SUM(COALESCE(cap.likes, 0)) AS likes,
    SUM(COALESCE(cap.retweets, 0)) AS retweets,
    SUM(COALESCE(cap.quotes, 0)) AS quotes,
    SUM(COALESCE(cap.respostas, 0)) AS respostas
FROM silver.postagem p
LEFT JOIN cap ON cap.postagem_id = p.postagem_id
LEFT JOIN cls c ON c.postagem_id = p.postagem_id
LEFT JOIN gold.calendario_caso cal
       ON cal.caso = p.caso_slug AND cal.data = DATE(p.created_at)
WHERE p.caso_slug IN ('monark', 'arthur_do_val')
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Validação: soma = Silver (4.803 + 13.893); sem sem_rotulo, sem fase nula
SELECT caso, SUM(n_postagens) AS postagens,
       SUM(CASE WHEN stance = 'sem_rotulo' THEN n_postagens END) AS sem_rotulo,
       SUM(CASE WHEN tipo_ref = 'desconhecido' THEN n_postagens END) AS tipo_desconhecido,
       SUM(CASE WHEN fase IS NULL THEN n_postagens END) AS sem_fase
FROM gold.fato_atividade GROUP BY 1;

SELECT caso, fase, stance, SUM(n_postagens) AS n
FROM gold.fato_atividade
GROUP BY 1, 2, 3 ORDER BY 1, MIN(dias_desde_estopim), 3;
