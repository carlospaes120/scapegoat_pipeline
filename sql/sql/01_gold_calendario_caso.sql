-- ============================================================
-- Gold — calendario_caso (versão final, 07/09/2026)
-- Grão: caso × data. Pico e limiar de declínio calculados SÓ sobre
-- dias >= estopim (decisão de 07/09: o máximo global do Arthur, 28/02,
-- é outra polêmica e não pode ancorar o pico).
-- ============================================================
USE CATALOG scapegoat;

CREATE TABLE IF NOT EXISTS gold.calendario_caso (
    caso STRING NOT NULL,
    data DATE NOT NULL,
    dias_desde_estopim INT,
    fase STRING,
    volume_postagens INT,
    CONSTRAINT pk_calendario_caso PRIMARY KEY (caso, data)
)
USING DELTA;

INSERT OVERWRITE gold.calendario_caso
WITH volume_diario AS (
    SELECT LOWER(caso_slug) AS caso, DATE(created_at) AS data, COUNT(*) AS n_postagens
    FROM silver.postagem
    WHERE LOWER(caso_slug) IN ('monark', 'arthur_do_val')
    GROUP BY 1, 2
),
estopim_por_caso AS (
    SELECT 'monark' AS caso, DATE('2022-02-08') AS data_estopim
    UNION ALL
    SELECT 'arthur_do_val', DATE('2022-03-04')
),
stats_por_caso AS (
    SELECT vd.caso, MAX(vd.n_postagens) AS pico_volume
    FROM volume_diario vd
    JOIN estopim_por_caso e ON e.caso = vd.caso
    WHERE vd.data >= e.data_estopim
    GROUP BY vd.caso
),
dia_pico_por_caso AS (
    SELECT vd.caso, MIN(vd.data) AS dia_pico
    FROM volume_diario vd
    JOIN estopim_por_caso e ON e.caso = vd.caso
    JOIN stats_por_caso s ON s.caso = vd.caso AND s.pico_volume = vd.n_postagens
    WHERE vd.data >= e.data_estopim
    GROUP BY vd.caso
)
SELECT
    vd.caso,
    vd.data,
    DATEDIFF(vd.data, e.data_estopim) AS dias_desde_estopim,
    CASE
        WHEN vd.data <  e.data_estopim THEN 'pre_crise'
        WHEN vd.data =  e.data_estopim THEN 'estopim'
        WHEN vd.data <  dp.dia_pico    THEN 'escalada'
        WHEN vd.data =  dp.dia_pico    THEN 'pico'
        WHEN vd.n_postagens >= CAST(s.pico_volume * 0.25 AS INT) THEN 'declinio'
        ELSE 'pos_rito'
    END AS fase,
    vd.n_postagens AS volume_postagens
FROM volume_diario vd
JOIN estopim_por_caso e   ON e.caso  = vd.caso
JOIN stats_por_caso s     ON s.caso  = vd.caso
JOIN dia_pico_por_caso dp ON dp.caso = vd.caso;

-- Validação
SELECT caso, fase, COUNT(*) AS dias, SUM(volume_postagens) AS postagens
FROM gold.calendario_caso GROUP BY 1, 2 ORDER BY 1, MIN(dias_desde_estopim);
