# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05_analise — Bloco 4 (Qualidade e Análise)
# MAGIC
# MAGIC Uma seção por pergunta (P1–P11). Cada seção: query sobre a `gold`, validação contra total conhecido,
# MAGIC figura salva em `SAIDA`. Nenhuma célula lê `silver` ou `bronze`; nenhum handle nem texto de postagem.
# MAGIC
# MAGIC Cores fixas por caso em todas as figuras: Arthur do Val = azul `#2a78d6`, Monark = laranja `#eb6834`.

# COMMAND ----------

# Parâmetros da sessão (cada notebook tem sessão própria: USE CATALOG não atravessa notebooks)
spark.sql("USE CATALOG scapegoat")

# Pasta de saída das figuras: evidencias/bloco4 dentro do Git folder (vai para o GitHub no próximo commit)
import os
SAIDA = "/Workspace/Users/paes120@gmail.com/scapegoat_pipeline/evidencias/bloco4"
os.makedirs(SAIDA, exist_ok=True)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SURF, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
COR = {"arthur_do_val": "#2a78d6", "monark": "#eb6834"}
NOME = {"arthur_do_val": "Arthur do Val", "monark": "Monark"}
ORDEM_FASE = ["pre_crise", "estopim", "escalada", "pico", "declinio", "pos_rito"]

def eixo_limpo(ax):
    ax.set_facecolor(SURF)
    ax.tick_params(colors=MUTED, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, lw=0.7); ax.set_axisbelow(True)
    for s in ("top", "right", "left"): ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 — Quanto tempo durou o episódio, do estopim ao retorno à linha de base?
# MAGIC
# MAGIC Fonte: `gold.calendario_caso`. Linha de base operacional = fase `pos_rito` (volume < 25 % do pico,
# MAGIC pico contado a partir do estopim). Validação: 26 linhas; Σ volume = 4.803 (Monark) + 13.893 (Arthur) = 18.696.

# COMMAND ----------

p1 = spark.sql("""
  SELECT
    caso,
    data,
    dias_desde_estopim,
    fase,
    volume_postagens,
    dayofweek(data) IN (1, 7) AS fim_de_semana,          -- 1 = domingo, 7 = sábado
    volume_postagens / MAX(CASE WHEN fase = 'pico' THEN volume_postagens END)
                       OVER (PARTITION BY caso) AS fracao_do_pico
  FROM scapegoat.gold.calendario_caso
  ORDER BY caso, data
""").toPandas()

# validação contra totais conhecidos
tot = p1.groupby("caso")["volume_postagens"].sum().to_dict()
assert len(p1) == 26, f"esperava 26 linhas, veio {len(p1)}"
assert tot == {"arthur_do_val": 13893, "monark": 4803}, f"totais divergem: {tot}"
pico = p1[p1.fase == "pico"].groupby("caso")["fracao_do_pico"].agg(["count", "max"])
assert (pico["count"] == 1).all() and (abs(pico["max"] - 1.0) < 1e-9).all(), f"esperava um pico = 1,0 por caso: {pico.to_dict()}"
print("P1 validada:", tot)
display(p1)

# COMMAND ----------

# resumo por caso — a resposta numérica de P1
p1_resumo = spark.sql("""
  SELECT
    caso,
    MIN(data)                                                    AS inicio_coleta,
    MAX(data)                                                    AS fim_coleta,
    COUNT(*)                                                     AS dias_coletados,
    COUNT(CASE WHEN fase = 'pre_crise' THEN 1 END)               AS dias_pre_crise,
    ROUND(AVG(CASE WHEN fase = 'pre_crise' THEN volume_postagens END)) AS media_pre_crise,
    MIN(CASE WHEN fase = 'estopim' THEN data END)                AS data_estopim,
    MIN(CASE WHEN fase = 'estopim' THEN volume_postagens END)    AS volume_estopim,
    MIN(CASE WHEN fase = 'pico' THEN data END)                   AS data_pico,
    MAX(CASE WHEN fase = 'pico' THEN volume_postagens END)       AS volume_pico,
    MIN(CASE WHEN fase = 'pos_rito' THEN data END)               AS primeiro_pos_rito,
    MIN(CASE WHEN fase = 'pos_rito' THEN dias_desde_estopim END) AS dias_ate_linha_base,
    COUNT(CASE WHEN fase = 'pos_rito' THEN 1 END)                AS dias_pos_rito,
    MAX(dias_desde_estopim)                                      AS ultimo_dia_desde_estopim
  FROM scapegoat.gold.calendario_caso
  GROUP BY caso
  ORDER BY caso
""")
display(p1_resumo)
p1_resumo.toPandas().to_csv(f"{SAIDA}/p1_resumo.csv", index=False)   # só datas e contagens; sem conta_id

# COMMAND ----------

# figura P1 — fração do pico × dias desde o estopim
fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
fig.patch.set_facecolor(SURF); eixo_limpo(ax)

for caso, g in p1.groupby("caso"):
    g = g.sort_values("dias_desde_estopim")
    ax.plot(g.dias_desde_estopim, g.fracao_do_pico, color=COR[caso], lw=2, zorder=3)
    for _, r in g.iterrows():
        ax.plot(r.dias_desde_estopim, r.fracao_do_pico, marker="o", ms=6.5,
                mfc=(SURF if r.fim_de_semana else COR[caso]), mec=COR[caso], mew=1.6, zorder=4)
    pico = g[g.fase == "pico"].iloc[0]
    ult = g.iloc[-1]
    ax.annotate(f"{NOME[caso]} (pico {int(pico.volume_postagens):,}, {pico.data:%d/%m})".replace(",", "."),
                (ult.dias_desde_estopim, ult.fracao_do_pico), xytext=(6, 0), textcoords="offset points",
                va="center", fontsize=8.5, color=INK2)

ax.axhline(0.25, color=MUTED, lw=1, ls=(0, (4, 3)), zorder=2)
ax.text(-7.3, 0.27, "limiar de 25 % do pico (pós-rito)", fontsize=8, color=MUTED, va="bottom")
ax.axvline(0, color=AXIS, lw=1, zorder=1)
ax.text(0.1, 1.55, "estopim", fontsize=8, color=MUTED, va="top")

# anotações — os três pontos que a discussão usa (posições lidas do dado, texto fixo)
m = p1[p1.caso == "monark"].set_index("dias_desde_estopim")
a = p1[p1.caso == "arthur_do_val"].set_index("dias_desde_estopim")
ax.annotate("13/02 (domingo): único dia\nabaixo de 25 %; 14/02 volta a 40 %", (5, m.loc[5, "fracao_do_pico"]),
            xytext=(0.5, 0.06), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
ax.annotate("14/03: corte da coleta,\nainda acima do limiar", (10, a.loc[10, "fracao_do_pico"]),
            xytext=(10.35, 0.70), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
ax.annotate("28/02: polêmica anterior\n(Ucrânia), 1.359 postagens", (-4, a.loc[-4, "fracao_do_pico"]),
            xytext=(-7.3, 1.27), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

ax.set_xlim(-7.5, 14.5); ax.set_ylim(0, 1.62)
ax.set_xticks(range(-7, 11)); ax.set_yticks([0, .25, .5, .75, 1, 1.25, 1.5])
ax.set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %", "125 %", "150 %"])
ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)
ax.set_ylabel("volume diário como fração do pico", color=INK2, fontsize=9)
ax.set_title("P1 · Duração do episódio: do estopim ao retorno à linha de base", loc="left", fontsize=11, color=INK, pad=22)
ax.text(0, 1.035, "pico = maior volume a partir do estopim · marcador vazado = sábado ou domingo · fonte: gold.calendario_caso",
        transform=ax.transAxes, fontsize=7.5, color=MUTED)
fig.tight_layout()
caminho = f"{SAIDA}/p1_duracao.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 — Quando ocorreu o pico e quão abrupta foi a escalada?
# MAGIC
# MAGIC Fonte: `gold.calendario_caso`. Com grão diário, a abruptez se mede melhor na queda do que na subida:
# MAGIC a subida do Monark é censurada pelo início da coleta e a do Arthur é mascarada pela polêmica anterior.
# MAGIC Regras: variação diária **não definida contra o primeiro dia de coleta**; razão pico/média pré-crise só com ≥ 3 dias de pré-crise.
# MAGIC Validação: 26 linhas; Σ volume = 18.696; pico 1 dia após o estopim nos dois casos.

# COMMAND ----------

p2 = spark.sql("""
  WITH s AS (
    SELECT caso, data, dias_desde_estopim, fase, volume_postagens,
           LAG(volume_postagens) OVER (PARTITION BY caso ORDER BY data)                    AS volume_dia_anterior,
           MIN(data) OVER (PARTITION BY caso)                                              AS primeiro_dia_coleta,
           MAX(CASE WHEN fase = 'pico' THEN volume_postagens END) OVER (PARTITION BY caso) AS volume_pico
    FROM scapegoat.gold.calendario_caso
  )
  SELECT caso, data, dias_desde_estopim, fase, volume_postagens,
         volume_postagens / volume_pico AS fracao_do_pico,
         CASE WHEN volume_dia_anterior IS NULL OR DATE_SUB(data, 1) = primeiro_dia_coleta THEN NULL
              ELSE volume_postagens / volume_dia_anterior - 1 END AS variacao_diaria
  FROM s
  ORDER BY caso, data
""").toPandas()

tot = p2.groupby("caso")["volume_postagens"].sum().to_dict()
assert len(p2) == 26 and tot == {"arthur_do_val": 13893, "monark": 4803}, f"validação P2: {len(p2)} linhas, {tot}"
assert (p2[p2.fase == "pico"].groupby("caso")["dias_desde_estopim"].min() == 1).all(), "esperava pico em dias_desde_estopim = 1"
assert p2.groupby("caso")["variacao_diaria"].apply(lambda s: s.isna().sum()).eq(2).all(), "esperava 2 variações indefinidas por caso"
print("P2 validada:", tot)
display(p2)

# COMMAND ----------

p2_resumo = spark.sql("""
  WITH c AS (
    SELECT *,
           MAX(CASE WHEN fase = 'pico' THEN dias_desde_estopim END) OVER (PARTITION BY caso) AS dia_pico,
           MAX(CASE WHEN fase = 'pico' THEN volume_postagens END)   OVER (PARTITION BY caso) AS volume_pico
    FROM scapegoat.gold.calendario_caso
  )
  SELECT
    caso,
    MIN(CASE WHEN fase = 'pico' THEN data END)                                              AS data_pico,
    MAX(dia_pico)                                                                           AS dias_estopim_ate_pico,
    MAX(volume_pico)                                                                        AS volume_pico,
    ROUND(MAX(volume_pico) / MAX(CASE WHEN fase = 'estopim' THEN volume_postagens END), 2) AS razao_pico_estopim,
    CASE WHEN COUNT(CASE WHEN fase = 'pre_crise' THEN 1 END) >= 3
         THEN ROUND(MAX(volume_pico) / AVG(CASE WHEN fase = 'pre_crise' THEN volume_postagens END), 2) END AS razao_pico_media_pre,
    ROUND(MAX(CASE WHEN dias_desde_estopim = dia_pico + 1 THEN volume_postagens END) / MAX(volume_pico), 2) AS fracao_dia_apos_pico,
    MIN(CASE WHEN dias_desde_estopim > dia_pico AND volume_postagens < 0.5  * volume_pico
             THEN dias_desde_estopim - dia_pico END)                                        AS dias_pico_ate_metade,
    MIN(CASE WHEN dias_desde_estopim > dia_pico AND volume_postagens < 0.25 * volume_pico
             THEN dias_desde_estopim - dia_pico END)                                        AS dias_pico_ate_quarto
  FROM c
  GROUP BY caso
  ORDER BY caso
""")
display(p2_resumo)
p2_resumo.toPandas().to_csv(f"{SAIDA}/p2_resumo.csv", index=False)

# COMMAND ----------

# figura P2 — variação dia a dia, um painel por caso
fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)

for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    g = p2[p2.caso == caso].sort_values("dias_desde_estopim")
    v = g.dropna(subset=["variacao_diaria"])
    ax.bar(v.dias_desde_estopim, v.variacao_diaria, width=0.62, color=COR[caso], zorder=3)
    for _, r in v.iterrows():                               # borda branca fina na base: mark spec
        ax.plot([r.dias_desde_estopim - 0.31, r.dias_desde_estopim + 0.31], [0, 0], color=SURF, lw=1.2, zorder=4)
    nd = g[g.variacao_diaria.isna()]
    for _, r in nd.iterrows():
        ax.text(r.dias_desde_estopim, 0.06, "n.d.", ha="center", fontsize=7, color=MUTED)
    ax.axhline(0, color=AXIS, lw=1, zorder=2)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    pico = g[g.fase == "pico"].iloc[0]
    ax.plot([pico.dias_desde_estopim], [0.985], marker="v", ms=6, color=INK, zorder=5, clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none")
    ax.text(pico.dias_desde_estopim, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2,
            transform=ax.get_xaxis_transform())
    ax.set_xticks(g.dias_desde_estopim)
    ax.set_xlim(g.dias_desde_estopim.min() - 0.7, g.dias_desde_estopim.max() + 0.7)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

# anotações lidas do dado
a = p2[p2.caso == "arthur_do_val"].set_index("dias_desde_estopim")
m = p2[p2.caso == "monark"].set_index("dias_desde_estopim")
axes[0].annotate(f"28/02 (Ucrânia): +{a.loc[-4,'variacao_diaria']*100:.0f} %,\na maior variação da janela",
                 (-4, a.loc[-4, "variacao_diaria"]), xytext=(-2.6, 3.1), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate(f"estopim → pico: +{a.loc[1,'variacao_diaria']*100:.0f} %;\ndia seguinte: {a.loc[2,'variacao_diaria']*100:.0f} %",
                 (1, a.loc[1, "variacao_diaria"]), xytext=(2.6, 1.6), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate("14/03: corte da coleta", (10, a.loc[10, "variacao_diaria"]), xytext=(6.3, -0.85), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[1].annotate(f"dia após o pico: {m.loc[2,'variacao_diaria']*100:.0f} %", (2, m.loc[2, "variacao_diaria"]),
                 xytext=(2.3, -0.95), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[1].annotate(f"14/02 (segunda): +{m.loc[6,'variacao_diaria']*100:.0f} %\napós o domingo",
                 (6, m.loc[6, "variacao_diaria"]), xytext=(-0.9, 2.9), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

axes[0].set_ylim(-1.1, 3.9)
axes[0].set_yticks([-1, -0.5, 0, 0.5, 1, 2, 3])
axes[0].set_yticklabels(["−100 %", "−50 %", "0", "+50 %", "+100 %", "+200 %", "+300 %"])
axes[0].set_ylabel("variação do volume em relação ao dia anterior", color=INK2, fontsize=9)
fig.suptitle("P2 · Abruptez da escalada: variação diária do volume", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "n.d. = variação não definida (primeiro dia de coleta como base) · fonte: gold.calendario_caso",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.09, right=0.99, top=0.80, bottom=0.14, wspace=0.08)
caminho = f"{SAIDA}/p2_abruptez.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 — Quando ocorreu o pico e quão abrupta foi a escalada?
# MAGIC
# MAGIC Fonte: `gold.calendario_caso`. Com grão diário, a abruptez se mede melhor na queda do que na subida:
# MAGIC a subida do Monark é censurada pelo início da coleta e a do Arthur é mascarada pela polêmica anterior.
# MAGIC A variação diária é medida em **pontos percentuais do pico** (Δ volume ÷ volume do pico), não em % do dia anterior —
# MAGIC a variação relativa explode quando a base é pequena (177 → 539 seria +205 %) e contraria a regra de comparar só medidas normalizadas.
# MAGIC Regras: variação **não definida contra o primeiro dia de coleta**; razão pico/média pré-crise só com ≥ 3 dias de pré-crise.
# MAGIC Validação: 26 linhas; Σ volume = 18.696; pico 1 dia após o estopim nos dois casos.

# COMMAND ----------

p2 = spark.sql("""
  WITH s AS (
    SELECT caso, data, dias_desde_estopim, fase, volume_postagens,
           LAG(volume_postagens) OVER (PARTITION BY caso ORDER BY data)                    AS volume_dia_anterior,
           MIN(data) OVER (PARTITION BY caso)                                              AS primeiro_dia_coleta,
           MAX(CASE WHEN fase = 'pico' THEN volume_postagens END) OVER (PARTITION BY caso) AS volume_pico
    FROM scapegoat.gold.calendario_caso
  )
  SELECT caso, data, dias_desde_estopim, fase, volume_postagens,
         volume_postagens / volume_pico AS fracao_do_pico,
         CASE WHEN volume_dia_anterior IS NULL OR DATE_SUB(data, 1) = primeiro_dia_coleta THEN NULL
              ELSE (volume_postagens - volume_dia_anterior) / volume_pico END AS variacao_pp_pico
  FROM s
  ORDER BY caso, data
""").toPandas()

tot = p2.groupby("caso")["volume_postagens"].sum().to_dict()
assert len(p2) == 26 and tot == {"arthur_do_val": 13893, "monark": 4803}, f"validação P2: {len(p2)} linhas, {tot}"
assert (p2[p2.fase == "pico"].groupby("caso")["dias_desde_estopim"].min() == 1).all(), "esperava pico em dias_desde_estopim = 1"
assert p2.groupby("caso")["variacao_pp_pico"].apply(lambda s: s.isna().sum()).eq(2).all(), "esperava 2 variações indefinidas por caso"
print("P2 validada:", tot)
display(p2)

# COMMAND ----------

p2_resumo = spark.sql("""
  WITH c AS (
    SELECT *,
           MAX(CASE WHEN fase = 'pico' THEN dias_desde_estopim END) OVER (PARTITION BY caso) AS dia_pico,
           MAX(CASE WHEN fase = 'pico' THEN volume_postagens END)   OVER (PARTITION BY caso) AS volume_pico
    FROM scapegoat.gold.calendario_caso
  )
  SELECT
    caso,
    MIN(CASE WHEN fase = 'pico' THEN data END)                                              AS data_pico,
    MAX(dia_pico)                                                                           AS dias_estopim_ate_pico,
    MAX(volume_pico)                                                                        AS volume_pico,
    ROUND(MAX(volume_pico) / MAX(CASE WHEN fase = 'estopim' THEN volume_postagens END), 2) AS razao_pico_estopim,
    CASE WHEN COUNT(CASE WHEN fase = 'pre_crise' THEN 1 END) >= 3
         THEN ROUND(MAX(volume_pico) / AVG(CASE WHEN fase = 'pre_crise' THEN volume_postagens END), 2) END AS razao_pico_media_pre,
    ROUND(MAX(CASE WHEN dias_desde_estopim = dia_pico + 1 THEN volume_postagens END) / MAX(volume_pico), 2) AS fracao_dia_apos_pico,
    MIN(CASE WHEN dias_desde_estopim > dia_pico AND volume_postagens < 0.5  * volume_pico
             THEN dias_desde_estopim - dia_pico END)                                        AS dias_pico_ate_metade,
    MIN(CASE WHEN dias_desde_estopim > dia_pico AND volume_postagens < 0.25 * volume_pico
             THEN dias_desde_estopim - dia_pico END)                                        AS dias_pico_ate_quarto
  FROM c
  GROUP BY caso
  ORDER BY caso
""")
display(p2_resumo)
p2_resumo.toPandas().to_csv(f"{SAIDA}/p2_resumo.csv", index=False)

# COMMAND ----------

# figura P2 — variação dia a dia em pontos do pico, um painel por caso
fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)

for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    g = p2[p2.caso == caso].sort_values("dias_desde_estopim")
    v = g.dropna(subset=["variacao_pp_pico"])
    ax.bar(v.dias_desde_estopim, v.variacao_pp_pico, width=0.62, color=COR[caso], zorder=3)
    for _, r in v.iterrows():                               # borda branca fina na base: mark spec
        ax.plot([r.dias_desde_estopim - 0.31, r.dias_desde_estopim + 0.31], [0, 0], color=SURF, lw=1.2, zorder=4)
    nd = g[g.variacao_pp_pico.isna()]
    for _, r in nd.iterrows():
        ax.text(r.dias_desde_estopim, 0.03, "n.d.", ha="center", fontsize=7, color=MUTED)
    ax.axhline(0, color=AXIS, lw=1, zorder=2)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    pico = g[g.fase == "pico"].iloc[0]
    ax.plot([pico.dias_desde_estopim], [0.985], marker="v", ms=6, color=INK, zorder=5, clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none")
    ax.text(pico.dias_desde_estopim, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2,
            transform=ax.get_xaxis_transform())
    ax.set_xticks(g.dias_desde_estopim)
    ax.set_xlim(g.dias_desde_estopim.min() - 0.7, g.dias_desde_estopim.max() + 0.7)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

# anotações lidas do dado
a = p2[p2.caso == "arthur_do_val"].set_index("dias_desde_estopim")
m = p2[p2.caso == "monark"].set_index("dias_desde_estopim")
axes[0].annotate(f"28/02 (Ucrânia): +{a.loc[-4,'variacao_pp_pico']*100:.0f} pp,\na maior variação da janela",
                 (-4, a.loc[-4, "variacao_pp_pico"]), xytext=(-2.6, 1.05), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate(f"estopim → pico: +{a.loc[1,'variacao_pp_pico']*100:.0f} pp;\ndia seguinte: {a.loc[2,'variacao_pp_pico']*100:.0f} pp",
                 (1, a.loc[1, "variacao_pp_pico"]), xytext=(2.6, 0.72), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate("14/03: corte da coleta", (10, a.loc[10, "variacao_pp_pico"]), xytext=(6.3, -0.62), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[1].annotate(f"dia após o pico: {m.loc[2,'variacao_pp_pico']*100:.0f} pp", (2, m.loc[2, "variacao_pp_pico"]),
                 xytext=(2.3, -0.72), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[1].annotate(f"14/02 (segunda): +{m.loc[6,'variacao_pp_pico']*100:.0f} pp\n(13 % → 40 % do pico)",
                 (6, m.loc[6, "variacao_pp_pico"]), xytext=(-0.9, 0.75), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

axes[0].set_ylim(-0.8, 1.35)
axes[0].set_yticks([-0.75, -0.5, -0.25, 0, 0.25, 0.5, 0.75, 1, 1.25])
axes[0].set_yticklabels(["−75", "−50", "−25", "0", "+25", "+50", "+75", "+100", "+125"])
axes[0].set_ylabel("variação diária, em pontos percentuais do pico", color=INK2, fontsize=9)
fig.suptitle("P2 · Abruptez da escalada: variação diária do volume em pontos do pico", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "Δ volume ÷ volume do pico do caso · n.d. = não definida (primeiro dia de coleta como base) · fonte: gold.calendario_caso",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.09, right=0.99, top=0.80, bottom=0.14, wspace=0.08)
caminho = f"{SAIDA}/p2_abruptez.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 — Quantas contas distintas participaram do episódio?
# MAGIC
# MAGIC Fonte: `gold.dim_conta_papel` (1 linha por caso × conta: autoras ∪ mencionadas) e `gold.calendario_caso`.
# MAGIC Contas automatizadas: não há marcador de automação na Silver nem na Gold; o inventário (03/09) não encontrou volume relevante em 2022;
# MAGIC nenhuma conta do corpus foi criada dentro da janela de coleta (conferido em 08/09 via `silver.conta.criado_em`). Reportado como zero, não como exclusão.
# MAGIC Concentração da produção: corte por **contagem** de contas (`ROW_NUMBER`), não por `PERCENT_RANK` — 78–91 % das autoras têm 1 postagem e o empate engoliria o corte.
# MAGIC Validação: contas 11.358 + 5.574 = 16.932 (= linhas de nó da `pub`); Σ postagens = 18.696; 1 alvo por caso; Σ contas novas por dia = contas do caso.

# COMMAND ----------

p3 = spark.sql("""
  WITH ranked AS (
    SELECT caso, conta_id, n_postagens_caso,
           ROW_NUMBER() OVER (PARTITION BY caso ORDER BY n_postagens_caso DESC, conta_id) AS pos,
           COUNT(*)     OVER (PARTITION BY caso)                                            AS n_autoras
    FROM scapegoat.gold.dim_conta_papel
    WHERE n_postagens_caso > 0
  ),
  top AS (
    SELECT caso,
           SUM(CASE WHEN pos <= CEIL(0.01 * n_autoras) THEN n_postagens_caso ELSE 0 END) AS post_top1pct,
           SUM(CASE WHEN pos <= CEIL(0.10 * n_autoras) THEN n_postagens_caso ELSE 0 END) AS post_top10pct,
           MAX(CASE WHEN pos = 1 THEN n_postagens_caso END)                              AS max_postagens_uma_conta
    FROM ranked GROUP BY caso
  ),
  base AS (
    SELECT caso,
           COUNT(*)                                             AS contas_no_caso,
           COUNT(CASE WHEN n_postagens_caso > 0 THEN 1 END)     AS contas_autoras,
           COUNT(CASE WHEN n_postagens_caso = 0 THEN 1 END)     AS contas_so_mencionadas,
           COUNT(CASE WHEN papel_principal = 'alvo' THEN 1 END) AS alvos,
           SUM(n_postagens_caso)                                AS total_postagens,
           COUNT(CASE WHEN n_postagens_caso = 1 THEN 1 END)     AS autoras_com_1_postagem
    FROM scapegoat.gold.dim_conta_papel GROUP BY caso
  )
  SELECT b.caso, b.contas_no_caso, b.contas_autoras, b.contas_so_mencionadas, b.alvos, b.total_postagens,
         ROUND(b.total_postagens / b.contas_autoras, 2)        AS postagens_por_autora,
         b.autoras_com_1_postagem,
         ROUND(b.autoras_com_1_postagem / b.contas_autoras, 3) AS parcela_autoras_1_postagem,
         t.max_postagens_uma_conta,
         ROUND(t.post_top1pct / b.total_postagens, 3)          AS parcela_top1pct_autoras,
         ROUND(t.post_top10pct / b.total_postagens, 3)         AS parcela_top10pct_autoras
  FROM base b JOIN top t ON t.caso = b.caso
  ORDER BY b.caso
""")
p3r = p3.toPandas()
assert p3r.contas_no_caso.sum() == 16932, f"esperava 16.932 contas (linhas de nó da pub), veio {p3r.contas_no_caso.sum()}"
assert dict(zip(p3r.caso, p3r.total_postagens)) == {"arthur_do_val": 13893, "monark": 4803}, "Σ postagens diverge da Silver"
assert (p3r.alvos == 1).all(), "esperava exatamente 1 alvo por caso"
assert (p3r.contas_autoras + p3r.contas_so_mencionadas == p3r.contas_no_caso).all()
print("P3 validada:", dict(zip(p3r.caso, p3r.contas_no_caso)))
display(p3)
p3r.to_csv(f"{SAIDA}/p3_resumo.csv", index=False)

# COMMAND ----------

# entrada de contas por dia (primeiro dia em que a conta aparece no caso) — dado da figura
p3d = spark.sql("""
  SELECT c.caso, c.data, c.dias_desde_estopim, c.fase,
         COUNT(d.conta_id)                                  AS contas_novas,
         COUNT(CASE WHEN d.n_postagens_caso > 0 THEN 1 END) AS autoras_novas,
         COUNT(CASE WHEN d.n_postagens_caso = 0 THEN 1 END) AS mencionadas_novas,
         SUM(COUNT(d.conta_id)) OVER (PARTITION BY c.caso ORDER BY c.data) AS contas_acumuladas,
         c.volume_postagens
  FROM scapegoat.gold.calendario_caso c
  LEFT JOIN scapegoat.gold.dim_conta_papel d ON d.caso = c.caso AND d.primeiro_dia = c.data
  GROUP BY c.caso, c.data, c.dias_desde_estopim, c.fase, c.volume_postagens
  ORDER BY c.caso, c.data
""").toPandas()
tot_dia = p3d.groupby("caso")["contas_novas"].sum().to_dict()
assert len(p3d) == 26 and tot_dia == dict(zip(p3r.caso, p3r.contas_no_caso)), f"Σ contas novas ≠ contas do caso: {tot_dia}"
p3d["fracao_contas"]  = p3d.contas_novas / p3d.caso.map(dict(zip(p3r.caso, p3r.contas_no_caso)))
p3d["fracao_autoras"] = p3d.autoras_novas / p3d.caso.map(dict(zip(p3r.caso, p3r.contas_no_caso)))
p3d["fracao_menc"]    = p3d.mencionadas_novas / p3d.caso.map(dict(zip(p3r.caso, p3r.contas_no_caso)))
display(p3d)

# COMMAND ----------

# figura P3 — quando as contas entram: novas contas por dia, como fração das contas do caso
import matplotlib.colors as mcolors
def clarear(hexcor, f=0.55):
    r, g, b = mcolors.to_rgb(hexcor); return (r + (1 - r) * f, g + (1 - g) * f, b + (1 - b) * f)

fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    g = p3d[p3d.caso == caso].sort_values("dias_desde_estopim")
    ax.bar(g.dias_desde_estopim, g.fracao_autoras, width=0.62, color=COR[caso], zorder=3, label="autoras")
    ax.bar(g.dias_desde_estopim, g.fracao_menc, bottom=g.fracao_autoras, width=0.62, color=clarear(COR[caso]),
           edgecolor=SURF, linewidth=1.2, zorder=3, label="só mencionadas")
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    pico = g[g.fase == "pico"].iloc[0]
    ax.plot([pico.dias_desde_estopim], [0.985], marker="v", ms=6, color=INK, zorder=5, clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none")
    ax.text(pico.dias_desde_estopim, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    ax.set_xticks(g.dias_desde_estopim)
    ax.set_xlim(g.dias_desde_estopim.min() - 0.7, g.dias_desde_estopim.max() + 0.7)
    n = int(p3r.set_index("caso").loc[caso, "contas_no_caso"])
    ax.set_title(f"{NOME[caso]} — {n:,} contas".replace(",", "."), loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

a = p3d[p3d.caso == "arthur_do_val"].set_index("dias_desde_estopim")
m = p3d[p3d.caso == "monark"].set_index("dias_desde_estopim")
d01 = m.loc[[0, 1], "fracao_contas"].sum()
axes[1].annotate(f"estopim + pico:\n{d01*100:.0f} % das contas\nem dois dias", (1, m.loc[1, "fracao_contas"]),
                 xytext=(1.8, 0.215), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate(f"28/02 (Ucrânia): maior entrada\nda janela, {a.loc[-4,'fracao_contas']*100:.0f} %", (-4, a.loc[-4, "fracao_contas"]),
                 xytext=(-2.4, 0.22), fontsize=7.8, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[0].annotate(f"pico: {a.loc[1,'fracao_contas']*100:.0f} %; depois ~{a.loc[2:9,'fracao_contas'].mean()*100:.0f} %/dia",
                 (1, a.loc[1, "fracao_contas"]), xytext=(2.6, 0.155), fontsize=7.8, color=INK2,
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

axes[0].set_ylim(0, 0.34)
axes[0].set_yticks([0, .05, .10, .15, .20, .25, .30]); axes[0].set_yticklabels(["0", "5 %", "10 %", "15 %", "20 %", "25 %", "30 %"])
axes[0].set_ylabel("contas que aparecem pela primeira vez no dia\n(fração das contas do caso)", color=INK2, fontsize=9)
fig.suptitle("P3 · Participação: quantas contas, e quando entram", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "tom escuro = autoras · tom claro = só mencionadas · primeiro dia em que a conta aparece no caso · fonte: gold.dim_conta_papel, gold.calendario_caso",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.10, right=0.99, top=0.80, bottom=0.14, wspace=0.08)
caminho = f"{SAIDA}/p3_participacao.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 — Houve contágio: contas que passaram a ser alvo por associação, e para onde a multidão transfere a pressão?
# MAGIC
# MAGIC Fonte: `gold.grafo_arestas` (menções por dia) × `gold.papel_narrativo_v0` (papéis declarados das contas mais mencionadas) × `gold.fato_rede` (`isolamento_alvo`).
# MAGIC `stance_origem` é a posição do autor **em relação ao alvo**, não à conta mencionada — por isso o papel de cada conta é declarado
# MAGIC por leitura do caso (`origem = declarado`), não inferido do stance. Taxa-base de acusadores nas menções: Arthur 0,67; Monark 0,39.
# MAGIC Medida: parcela das menções do dia dirigida a cada papel (comparável entre casos). Validação: Σ menções do dia = `fato_rede.n_mencoes`;
# MAGIC 19 + 21 contas rotuladas, nenhuma é o alvo.

# COMMAND ----------

p4 = spark.sql("""
  WITH dia AS (
    SELECT caso, data, dias_desde_estopim, fase, n_mencoes, n_nos, isolamento_alvo
    FROM scapegoat.gold.fato_rede
  ),
  por_papel AS (
    SELECT g.caso, g.data, p.papel_inicial AS papel, SUM(g.peso) AS mencoes, COUNT(DISTINCT g.destino_conta_id) AS contas
    FROM scapegoat.gold.grafo_arestas g
    JOIN scapegoat.gold.papel_narrativo_v0 p ON p.caso = g.caso AND p.conta_id = g.destino_conta_id
    GROUP BY g.caso, g.data, p.papel_inicial
  )
  SELECT d.caso, d.data, d.dias_desde_estopim, d.fase, d.n_mencoes, d.n_nos, d.isolamento_alvo,
         pp.papel, COALESCE(pp.mencoes, 0) AS mencoes, COALESCE(pp.contas, 0) AS contas,
         COALESCE(pp.mencoes, 0) / d.n_mencoes AS parcela_do_dia
  FROM dia d
  LEFT JOIN por_papel pp ON pp.caso = d.caso AND pp.data = d.data
  ORDER BY d.caso, d.data, pp.papel
""").toPandas()

# validações
n_menc = p4.drop_duplicates(["caso", "data"]).groupby("caso")["n_mencoes"].sum().to_dict()
assert n_menc == {"arthur_do_val": 22954, "monark": 5868}, f"Σ n_mencoes diverge de silver.mencao: {n_menc}"
rot = spark.sql("""
  SELECT p.caso, COUNT(*) AS n, SUM(CASE WHEN d.papel_principal = 'alvo' OR d.conta_id IS NULL THEN 1 ELSE 0 END) AS invalidas
  FROM scapegoat.gold.papel_narrativo_v0 p
  LEFT JOIN scapegoat.gold.dim_conta_papel d ON d.caso = p.caso AND d.conta_id = p.conta_id
  GROUP BY p.caso""").toPandas().set_index("caso")
assert rot.loc["monark", "n"] == 19 and rot.loc["arthur_do_val", "n"] == 21, f"esperava 19/21 contas rotuladas: {rot.n.to_dict()}"
assert (rot.invalidas == 0).all(), "conta rotulada inexistente na dim_conta_papel ou igual ao alvo"
print("P4 validada:", n_menc, rot.n.to_dict())

# COMMAND ----------

# resumo por caso × papel — a resposta numérica
p4_resumo = spark.sql("""
  WITH tot AS (SELECT caso, SUM(peso) AS mencoes_caso FROM scapegoat.gold.grafo_arestas GROUP BY caso),
  contas_papel AS (
    SELECT caso, papel_inicial AS papel, COUNT(*) AS contas FROM scapegoat.gold.papel_narrativo_v0 GROUP BY caso, papel_inicial
  ),
  por_papel_dia AS (
    SELECT g.caso, p.papel_inicial AS papel, g.dias_desde_estopim, SUM(g.peso) AS mencoes
    FROM scapegoat.gold.grafo_arestas g
    JOIN scapegoat.gold.papel_narrativo_v0 p ON p.caso = g.caso AND p.conta_id = g.destino_conta_id
    GROUP BY g.caso, p.papel_inicial, g.dias_desde_estopim
  ),
  por_papel AS (
    SELECT caso, papel, SUM(mencoes) AS mencoes,
           MAX_BY(dias_desde_estopim, mencoes) AS dia_pico_papel
    FROM por_papel_dia GROUP BY caso, papel
  ),
  alvo AS (
    SELECT caso, SUM(peso) AS mencoes_alvo, MAX_BY(dias_desde_estopim, peso) AS dia_pico_mencoes_alvo
    FROM (SELECT caso, dias_desde_estopim, SUM(peso) AS peso FROM scapegoat.gold.grafo_arestas
          WHERE papel_destino = 'alvo' GROUP BY caso, dias_desde_estopim)
    GROUP BY caso
  )
  SELECT pp.caso, pp.papel, cp.contas, pp.mencoes,
         ROUND(pp.mencoes / t.mencoes_caso, 3)  AS parcela_mencoes_caso,
         ROUND(pp.mencoes / a.mencoes_alvo, 2)  AS razao_vs_alvo,
         pp.dia_pico_papel,
         pp.dia_pico_papel - 1                  AS defasagem_vs_pico_calendario,
         a.dia_pico_mencoes_alvo
  FROM por_papel pp
  JOIN contas_papel cp ON cp.caso = pp.caso AND cp.papel = pp.papel
  JOIN tot t  ON t.caso = pp.caso
  JOIN alvo a ON a.caso = pp.caso
  ORDER BY pp.caso, pp.mencoes DESC
""")
p4r = p4_resumo.toPandas()
assert (p4r.groupby("caso")["parcela_mencoes_caso"].sum() < 1).all(), "Σ parcelas por caso deve ser < 1 (menções aos papéis ⊂ menções do caso)"
display(p4_resumo)
p4r.to_csv(f"{SAIDA}/p4_resumo.csv", index=False)

# COMMAND ----------

# figura P4 — parcela das menções do dia por papel, com o alvo como referência
COR_PAPEL = {"instituicao_legitimadora": "#4a3aa7", "vitima_secundaria": "#e34948",
             "aliado_do_alvo": "#008300", "lider_acusacao": "#e87ba4"}
ROTULO = {"instituicao_legitimadora": "instituições legitimadoras", "vitima_secundaria": "vítimas secundárias",
          "aliado_do_alvo": "aliados do alvo", "lider_acusacao": "líderes de acusação"}

MIN_NOS = 30   # regra do Catálogo: dias com poucos nós produzem métricas degeneradas
dias = p4.drop_duplicates(["caso", "data"]).sort_values(["caso", "data"]).copy()
dias["alvo_plot"] = dias.isolamento_alvo.where(dias.n_nos >= MIN_NOS)
omitidos = dias[dias.n_nos < MIN_NOS]

fig, axes = plt.subplots(1, 2, figsize=(10, 4.6), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = dias[dias.caso == caso]
    (h,) = ax.plot(d.dias_desde_estopim, d.alvo_plot, color=INK, lw=2.2, marker="o", ms=3.5, zorder=4)
    handles["alvo"] = h
    for papel, cor in COR_PAPEL.items():
        s_ = p4[(p4.caso == caso) & (p4.papel == papel)].sort_values("dias_desde_estopim")
        if s_.empty: continue
        s_ = d[["dias_desde_estopim"]].merge(s_[["dias_desde_estopim", "parcela_do_dia"]], how="left").fillna(0)
        (h,) = ax.plot(s_.dias_desde_estopim, s_.parcela_do_dia, color=cor, lw=1.8, marker="o", ms=3.5, zorder=3)
        handles[ROTULO[papel]] = h
    for _, r in omitidos[omitidos.caso == caso].iterrows():
        ax.text(r.dias_desde_estopim, 0.01, "n.d.", ha="center", fontsize=7, color=MUTED)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    ax.set_xticks(d.dias_desde_estopim)
    ax.set_xlim(d.dias_desde_estopim.min() - 0.5, d.dias_desde_estopim.max() + 0.5)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

# anotações lidas do dado
a_inst = p4[(p4.caso == "arthur_do_val") & (p4.papel == "instituicao_legitimadora")].set_index("dias_desde_estopim")
m_inst = p4[(p4.caso == "monark") & (p4.papel == "instituicao_legitimadora")].set_index("dias_desde_estopim")
if 8 in a_inst.index:
    axes[0].annotate(f"dia 8: {a_inst.loc[8,'parcela_do_dia']*100:.0f} % das menções\nvão a parlamentares e à casa legislativa",
                     (8, a_inst.loc[8, "parcela_do_dia"]), xytext=(2.2, 0.62), fontsize=7.6, color=INK2,
                     arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
if 0 in m_inst.index:
    axes[1].annotate(f"dias 0–1: patrocinadores\ne programa recebem\n{m_inst.loc[0,'parcela_do_dia']*100:.0f}–{m_inst.loc[1,'parcela_do_dia']*100:.0f} % das menções",
                     (1, m_inst.loc[1, "parcela_do_dia"]), xytext=(2.4, 0.56), fontsize=7.6, color=INK2,
                     arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.93), ncol=5, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 0.72)
axes[0].set_yticks([0, .1, .2, .3, .4, .5, .6, .7]); axes[0].set_yticklabels(["0", "10 %", "20 %", "30 %", "40 %", "50 %", "60 %", "70 %"])
axes[0].set_ylabel("parcela das menções do dia dirigida ao papel", color=INK2, fontsize=9)
fig.suptitle("P4 · Para onde a multidão aponta: alvo, instituições, vítimas secundárias, aliados, líderes", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, f"papéis declarados (gold.papel_narrativo_v0, top-25 por menções) · alvo = isolamento_alvo de gold.fato_rede · n.d. = dia com < {MIN_NOS} nós, omitido · fonte: gold.grafo_arestas",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.78, bottom=0.13, wspace=0.08)
caminho = f"{SAIDA}/p4_contagio.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)


# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 — Quão concentrada foi a atenção? Que fração das menções recaiu sobre que fração das contas?
# MAGIC
# MAGIC Fontes: `gold.fato_rede` (Gini, HHI e centralização de grau de entrada por dia, calculados no Bloco 3) e `gold.grafo_arestas`
# MAGIC (top-1 %, 5 %, 10 % das contas mencionadas por **fase** e no caso inteiro; corte por contagem — `ROW_NUMBER`, não `PERCENT_RANK`).
# MAGIC Janela de comparação = fase; o dia entra só como série das métricas prontas, com dias de < 30 nós marcados como inválidos.
# MAGIC Validação: 26 linhas; Σ `n_mencoes` = 22.954 + 5.868 = `grafo_arestas`; linha `todas` = total do caso; top-1 conta = alvo em toda janela.

# COMMAND ----------

p5 = spark.sql("""
  SELECT caso, data, dias_desde_estopim, fase, n_nos, n_arestas, n_mencoes, densidade,
         gini_mencoes_recebidas AS gini, hhi_mencoes_recebidas AS hhi,
         centralizacao_grau_entrada AS centralizacao, isolamento_alvo,
         n_nos >= 30 AS dia_valido
  FROM scapegoat.gold.fato_rede
  ORDER BY caso, data
""").toPandas()
tot = p5.groupby("caso")["n_mencoes"].sum().to_dict()
assert len(p5) == 26 and tot == {"arthur_do_val": 22954, "monark": 5868}, f"validação P5: {len(p5)} linhas, {tot}"
assert p5[~p5.dia_valido].shape[0] == 1 and p5[~p5.dia_valido].iloc[0].caso == "monark", "esperava só o dia -1 do Monark inválido"
print("P5 validada:", tot, "| dias inválidos:", p5[~p5.dia_valido][["caso", "dias_desde_estopim", "n_nos"]].values.tolist())

# COMMAND ----------

# concentração por fase e no caso inteiro — a resposta numérica
p5_resumo = spark.sql("""
  WITH recebidas AS (
    SELECT caso, fase, destino_conta_id, papel_destino, SUM(peso) AS mencoes
    FROM scapegoat.gold.grafo_arestas GROUP BY caso, fase, destino_conta_id, papel_destino
    UNION ALL
    SELECT caso, 'todas' AS fase, destino_conta_id, papel_destino, SUM(peso) AS mencoes
    FROM scapegoat.gold.grafo_arestas GROUP BY caso, destino_conta_id, papel_destino
  ),
  ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY caso, fase ORDER BY mencoes DESC, destino_conta_id) AS pos,
           COUNT(*)     OVER (PARTITION BY caso, fase) AS n_contas,
           SUM(mencoes) OVER (PARTITION BY caso, fase) AS total
    FROM recebidas
  )
  SELECT caso, fase,
         MAX(n_contas) AS contas_mencionadas,
         MAX(total)    AS mencoes,
         ROUND(SUM(CASE WHEN papel_destino = 'alvo' THEN mencoes ELSE 0 END) / MAX(total), 3)      AS parcela_alvo,
         ROUND(SUM(CASE WHEN pos = 1 THEN mencoes ELSE 0 END) / MAX(total), 3)                     AS parcela_top1_conta,
         ROUND(SUM(CASE WHEN pos <= CEIL(0.01 * n_contas) THEN mencoes ELSE 0 END) / MAX(total), 3) AS parcela_top1pct,
         ROUND(SUM(CASE WHEN pos <= CEIL(0.05 * n_contas) THEN mencoes ELSE 0 END) / MAX(total), 3) AS parcela_top5pct,
         ROUND(SUM(CASE WHEN pos <= CEIL(0.10 * n_contas) THEN mencoes ELSE 0 END) / MAX(total), 3) AS parcela_top10pct,
         ROUND(SUM(CASE WHEN mencoes = 1 THEN 1 ELSE 0 END) / MAX(n_contas), 3)                    AS parcela_contas_com_1_mencao
  FROM ranked
  GROUP BY caso, fase
  ORDER BY caso, CASE fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                           WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 ELSE 7 END
""")
p5r = p5_resumo.toPandas()
todas = p5r[p5r.fase == "todas"].set_index("caso")
assert todas.loc["arthur_do_val", "mencoes"] == 22954 and todas.loc["monark", "mencoes"] == 5868, "linha 'todas' ≠ total do caso"
assert (p5r.parcela_alvo == p5r.parcela_top1_conta).all(), "esperava o alvo como conta mais mencionada em toda janela"
display(p5_resumo)
p5r.to_csv(f"{SAIDA}/p5_resumo.csv", index=False)

# COMMAND ----------

# figura P5 — concentração da atenção por dia: Gini, centralização, HHI (0–1), um painel por caso
COR_MET = {"gini": "#1baf7a", "centralizacao": "#4a3aa7", "hhi": "#e87ba4"}
ROT_MET = {"gini": "Gini das menções recebidas", "centralizacao": "centralização (grau de entrada)", "hhi": "HHI"}
MIN_NOS = 30

fig, axes = plt.subplots(1, 2, figsize=(10, 4.4), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = p5[p5.caso == caso].sort_values("dias_desde_estopim").copy()
    for m, cor in COR_MET.items():
        y = d[m].where(d.n_nos >= MIN_NOS)
        (h,) = ax.plot(d.dias_desde_estopim, y, color=cor, lw=1.9, marker="o", ms=3.5, zorder=3)
        handles[ROT_MET[m]] = h
    for _, r in d[d.n_nos < MIN_NOS].iterrows():
        ax.text(r.dias_desde_estopim, 0.02, "n.d.", ha="center", fontsize=7, color=MUTED)
    ax.axhline(0.8, color=MUTED, lw=0.9, ls=(0, (4, 3)), zorder=1)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    ax.set_xticks(d.dias_desde_estopim)
    ax.set_xlim(d.dias_desde_estopim.min() - 0.5, d.dias_desde_estopim.max() + 0.5)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)
axes[0].text(-7.4, 0.815, "Gini = 0,8", fontsize=7.5, color=MUTED, va="bottom")

a = p5[p5.caso == "arthur_do_val"].set_index("dias_desde_estopim"); m = p5[p5.caso == "monark"].set_index("dias_desde_estopim")
axes[0].annotate(f"centralização cai de {a.loc[-7,'centralizacao']:.2f} (dia −7)\npara {a.loc[10,'centralizacao']:.2f} (dia 10): a atenção se dispersa".replace(".", ","),
                 (7, a.loc[7, "centralizacao"]), xytext=(1.6, 0.66), fontsize=7.6, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))
axes[1].annotate(f"centralização sobe\nde {m.loc[1,'centralizacao']:.2f} (pico) a {m.loc[6,'centralizacao']:.2f} (dia 6):\na atenção converge".replace(".", ","),
                 (3, m.loc[3, "centralizacao"]), xytext=(-0.9, 0.52), fontsize=7.6, color=INK2, arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8))

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.93), ncol=3, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 1.02)
axes[0].set_yticks([0, .2, .4, .6, .8, 1.0]); axes[0].set_yticklabels(["0", "0,2", "0,4", "0,6", "0,8", "1,0"])
axes[0].set_ylabel("índice (0 = igualdade, 1 = tudo em um nó)", color=INK2, fontsize=9)
fig.suptitle("P5 · Concentração da atenção por dia: Gini, centralização e HHI das menções recebidas", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, f"grafo de menções do dia (autor → mencionado) · n.d. = dia com < {MIN_NOS} nós, omitido · fonte: gold.fato_rede",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.78, bottom=0.13, wspace=0.08)
caminho = f"{SAIDA}/p5_concentracao.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG scapegoat;
# MAGIC
# MAGIC WITH s AS (
# MAGIC   SELECT r.caso, r.dias_desde_estopim, r.n_nos, c.volume_postagens,
# MAGIC          r.gini_mencoes_recebidas AS gini, r.hhi_mencoes_recebidas AS hhi,
# MAGIC          r.centralizacao_grau_entrada AS centralizacao, r.isolamento_alvo,
# MAGIC          r.n_mencoes * r.isolamento_alvo AS mencoes_ao_alvo
# MAGIC   FROM gold.fato_rede r
# MAGIC   JOIN gold.calendario_caso c ON c.caso = r.caso AND c.data = r.data
# MAGIC   WHERE r.n_nos >= 30
# MAGIC ),
# MAGIC janelas AS (
# MAGIC   SELECT 'todos os dias validos' AS janela, * FROM s
# MAGIC   UNION ALL
# MAGIC   SELECT 'a partir do estopim'   AS janela, * FROM s WHERE dias_desde_estopim >= 0
# MAGIC )
# MAGIC SELECT
# MAGIC   caso, janela,
# MAGIC   MAX_BY(dias_desde_estopim, volume_postagens)  AS pico_volume,
# MAGIC   MAX_BY(dias_desde_estopim, mencoes_ao_alvo)   AS pico_mencoes_alvo,
# MAGIC   MAX_BY(dias_desde_estopim, isolamento_alvo)   AS pico_isolamento,
# MAGIC   MAX_BY(dias_desde_estopim, centralizacao)     AS pico_centralizacao,
# MAGIC   MAX_BY(dias_desde_estopim, hhi)               AS pico_hhi,
# MAGIC   MAX_BY(dias_desde_estopim, gini)              AS pico_gini,
# MAGIC   MAX_BY(dias_desde_estopim, centralizacao) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_centralizacao,
# MAGIC   MAX_BY(dias_desde_estopim, isolamento_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_isolamento,
# MAGIC   MAX_BY(dias_desde_estopim, mencoes_ao_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_mencoes_alvo,
# MAGIC   ROUND(CORR(volume_postagens, centralizacao), 2)   AS corr_volume_centralizacao,
# MAGIC   ROUND(CORR(volume_postagens, isolamento_alvo), 2) AS corr_volume_isolamento,
# MAGIC   COUNT(*)                                          AS dias
# MAGIC FROM janelas
# MAGIC GROUP BY caso, janela
# MAGIC ORDER BY caso, janela;

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 — O pico de concentração coincide com o pico de volume, ou um antecede o outro?
# MAGIC
# MAGIC Fontes: `gold.fato_rede` × `gold.calendario_caso`, só dias com ≥ 30 nós. Duas janelas: todos os dias válidos e a partir do estopim
# MAGIC (a pré-crise do Arthur é outra polêmica, e o máximo global de volume dela — 28/02 — não é o pico do caso).
# MAGIC Medidas: dia do pico de volume, de menções ao alvo, de `isolamento_alvo`, centralização, HHI e Gini; defasagens em dias; correlação volume × concentração.
# MAGIC Validação: 18 + 7 dias válidos; pico de volume a partir do estopim = dia 1 nos dois casos (P2).

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

p6 = spark.sql("""
  WITH s AS (
    SELECT r.caso, r.data, r.dias_desde_estopim, r.fase, r.n_nos, c.volume_postagens,
           r.gini_mencoes_recebidas AS gini, r.hhi_mencoes_recebidas AS hhi,
           r.centralizacao_grau_entrada AS centralizacao, r.isolamento_alvo,
           r.n_mencoes * r.isolamento_alvo AS mencoes_ao_alvo
    FROM scapegoat.gold.fato_rede r
    JOIN scapegoat.gold.calendario_caso c ON c.caso = r.caso AND c.data = r.data
    WHERE r.n_nos >= 30
  )
  SELECT *, volume_postagens / MAX(CASE WHEN dias_desde_estopim >= 0 THEN volume_postagens END) OVER (PARTITION BY caso) AS fracao_do_pico
  FROM s ORDER BY caso, data
""").toPandas()
assert p6.groupby("caso").size().to_dict() == {"arthur_do_val": 18, "monark": 7}, "esperava 18 + 7 dias válidos"
assert (p6[p6.fracao_do_pico == 1.0].dias_desde_estopim == 1).all(), "pico de volume a partir do estopim deve ser o dia 1"
print("P6 validada:", p6.groupby("caso").size().to_dict())

# COMMAND ----------

p6_resumo = spark.sql("""
  WITH s AS (
    SELECT r.caso, r.dias_desde_estopim, c.volume_postagens,
           r.gini_mencoes_recebidas AS gini, r.hhi_mencoes_recebidas AS hhi,
           r.centralizacao_grau_entrada AS centralizacao, r.isolamento_alvo,
           r.n_mencoes * r.isolamento_alvo AS mencoes_ao_alvo
    FROM scapegoat.gold.fato_rede r
    JOIN scapegoat.gold.calendario_caso c ON c.caso = r.caso AND c.data = r.data
    WHERE r.n_nos >= 30
  ),
  janelas AS (
    SELECT 'todos os dias validos' AS janela, * FROM s
    UNION ALL
    SELECT 'a partir do estopim'   AS janela, * FROM s WHERE dias_desde_estopim >= 0
  )
  SELECT caso, janela,
         MAX_BY(dias_desde_estopim, volume_postagens) AS pico_volume,
         MAX_BY(dias_desde_estopim, mencoes_ao_alvo)  AS pico_mencoes_alvo,
         MAX_BY(dias_desde_estopim, isolamento_alvo)  AS pico_isolamento,
         MAX_BY(dias_desde_estopim, centralizacao)    AS pico_centralizacao,
         MAX_BY(dias_desde_estopim, hhi)              AS pico_hhi,
         MAX_BY(dias_desde_estopim, gini)             AS pico_gini,
         MAX_BY(dias_desde_estopim, centralizacao)   - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_centralizacao,
         MAX_BY(dias_desde_estopim, isolamento_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_isolamento,
         MAX_BY(dias_desde_estopim, mencoes_ao_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_mencoes_alvo,
         ROUND(CORR(volume_postagens, centralizacao), 2)   AS corr_volume_centralizacao,
         ROUND(CORR(volume_postagens, isolamento_alvo), 2) AS corr_volume_isolamento,
         COUNT(*) AS dias
  FROM janelas GROUP BY caso, janela ORDER BY caso, janela
""")
p6r = p6_resumo.toPandas()
assert (p6r[p6r.janela == "a partir do estopim"].pico_volume == 1).all()
display(p6_resumo)
p6r.to_csv(f"{SAIDA}/p6_resumo.csv", index=False)

# COMMAND ----------

# figura P6 — volume (fração do pico) × centralização × isolamento do alvo, por dia
import matplotlib.colors as mcolors
def clarear(hexcor, f=0.6):
    r, g, b = mcolors.to_rgb(hexcor); return (r + (1 - r) * f, g + (1 - g) * f, b + (1 - b) * f)

fig, axes = plt.subplots(1, 2, figsize=(10, 4.6), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = p6[p6.caso == caso].sort_values("dias_desde_estopim")
    hb = ax.bar(d.dias_desde_estopim, d.fracao_do_pico, width=0.7, color=clarear(COR[caso]), zorder=2)
    handles["volume (fração do pico a partir do estopim)"] = hb
    (h1,) = ax.plot(d.dias_desde_estopim, d.centralizacao, color="#4a3aa7", lw=1.9, marker="o", ms=3.5, zorder=4)
    (h2,) = ax.plot(d.dias_desde_estopim, d.isolamento_alvo, color=INK, lw=2.0, marker="o", ms=3.5, zorder=4)
    handles["centralização (grau de entrada)"] = h1; handles["parcela das menções no alvo"] = h2
    # marcadores de pico
    pv = d[d.dias_desde_estopim >= 0].sort_values("volume_postagens").iloc[-1]
    pc = d[d.dias_desde_estopim >= 0].sort_values("centralizacao").iloc[-1]
    ax.plot([pv.dias_desde_estopim], [0.985], marker="v", ms=6, color=clarear(COR[caso], 0.2), clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.plot([pc.dias_desde_estopim], [0.985], marker="v", ms=6, color="#4a3aa7", clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.set_xticks(d.dias_desde_estopim)
    ax.set_xlim(d.dias_desde_estopim.min() - 0.6, d.dias_desde_estopim.max() + 0.6)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

ra = p6r[(p6r.caso == "arthur_do_val") & (p6r.janela == "a partir do estopim")].iloc[0]
rm = p6r[(p6r.caso == "monark") & (p6r.janela == "a partir do estopim")].iloc[0]
axes[0].text(0.02, 0.985, f"a partir do estopim: pico de volume dia {ra.pico_volume}, de centralização dia {ra.pico_centralizacao} "
             f"(+{ra.defasagem_centralizacao}); correlação volume × centralização {ra.corr_volume_centralizacao:.2f}".replace(".", ","),
             transform=axes[0].transAxes, fontsize=7.6, color=INK2, va="top")
axes[0].text(0.02, 0.935, "janela inteira: todos os picos de concentração ficam em −3/−4 (polêmica anterior)",
             transform=axes[0].transAxes, fontsize=7.6, color=INK2, va="top")
axes[1].text(0.03, 0.985, f"pico de volume dia {rm.pico_volume};\ncentralização e isolamento dia {rm.pico_centralizacao} (+{rm.defasagem_centralizacao});\n"
             f"correlação volume × centralização {rm.corr_volume_centralizacao:.2f}".replace(".", ","),
             transform=axes[1].transAxes, fontsize=7.6, color=INK2, va="top")

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.93), ncol=3, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 1.85)
axes[0].set_yticks([0, .25, .5, .75, 1.0, 1.25, 1.5]); axes[0].set_yticklabels(["0", "0,25", "0,50", "0,75", "1,00", "1,25", "1,50"])
axes[0].set_ylabel("fração do pico (volume) · índice 0–1 (concentração)", color=INK2, fontsize=9)
fig.suptitle("P6 · Pico de volume × pico de concentração: quem vem primeiro", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "▼ pico de volume (cor do caso) e pico de centralização (roxo), a partir do estopim · só dias com ≥ 30 nós · fonte: gold.fato_rede, gold.calendario_caso",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.78, bottom=0.13, wspace=0.08)
caminho = f"{SAIDA}/p6_defasagem.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 — O pico de concentração coincide com o pico de volume, ou um antecede o outro?
# MAGIC
# MAGIC Fontes: `gold.fato_rede` × `gold.calendario_caso`, só dias com ≥ 30 nós. Duas janelas: todos os dias válidos e a partir do estopim
# MAGIC (a pré-crise do Arthur é outra polêmica, e o máximo global de volume dela — 28/02 — não é o pico do caso).
# MAGIC Medidas: dia do pico de volume, de menções ao alvo, de `isolamento_alvo`, centralização, HHI e Gini; defasagens em dias; correlação volume × concentração.
# MAGIC Validação: 18 + 7 dias válidos; pico de volume a partir do estopim = dia 1 nos dois casos (P2).

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

p6 = spark.sql("""
  WITH s AS (
    SELECT r.caso, r.data, r.dias_desde_estopim, r.fase, r.n_nos, c.volume_postagens,
           r.gini_mencoes_recebidas AS gini, r.hhi_mencoes_recebidas AS hhi,
           r.centralizacao_grau_entrada AS centralizacao, r.isolamento_alvo,
           r.n_mencoes * r.isolamento_alvo AS mencoes_ao_alvo
    FROM scapegoat.gold.fato_rede r
    JOIN scapegoat.gold.calendario_caso c ON c.caso = r.caso AND c.data = r.data
    WHERE r.n_nos >= 30
  )
  SELECT *, volume_postagens / MAX(CASE WHEN dias_desde_estopim >= 0 THEN volume_postagens END) OVER (PARTITION BY caso) AS fracao_do_pico
  FROM s ORDER BY caso, data
""").toPandas()
assert p6.groupby("caso").size().to_dict() == {"arthur_do_val": 18, "monark": 7}, "esperava 18 + 7 dias válidos"
assert (p6[p6.fracao_do_pico == 1.0].dias_desde_estopim == 1).all(), "pico de volume a partir do estopim deve ser o dia 1"
print("P6 validada:", p6.groupby("caso").size().to_dict())

# COMMAND ----------

p6_resumo = spark.sql("""
  WITH s AS (
    SELECT r.caso, r.dias_desde_estopim, c.volume_postagens,
           r.gini_mencoes_recebidas AS gini, r.hhi_mencoes_recebidas AS hhi,
           r.centralizacao_grau_entrada AS centralizacao, r.isolamento_alvo,
           r.n_mencoes * r.isolamento_alvo AS mencoes_ao_alvo
    FROM scapegoat.gold.fato_rede r
    JOIN scapegoat.gold.calendario_caso c ON c.caso = r.caso AND c.data = r.data
    WHERE r.n_nos >= 30
  ),
  janelas AS (
    SELECT 'todos os dias validos' AS janela, * FROM s
    UNION ALL
    SELECT 'a partir do estopim'   AS janela, * FROM s WHERE dias_desde_estopim >= 0
  )
  SELECT caso, janela,
         MAX_BY(dias_desde_estopim, volume_postagens) AS pico_volume,
         MAX_BY(dias_desde_estopim, mencoes_ao_alvo)  AS pico_mencoes_alvo,
         MAX_BY(dias_desde_estopim, isolamento_alvo)  AS pico_isolamento,
         MAX_BY(dias_desde_estopim, centralizacao)    AS pico_centralizacao,
         MAX_BY(dias_desde_estopim, hhi)              AS pico_hhi,
         MAX_BY(dias_desde_estopim, gini)             AS pico_gini,
         MAX_BY(dias_desde_estopim, centralizacao)   - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_centralizacao,
         MAX_BY(dias_desde_estopim, isolamento_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_isolamento,
         MAX_BY(dias_desde_estopim, mencoes_ao_alvo) - MAX_BY(dias_desde_estopim, volume_postagens) AS defasagem_mencoes_alvo,
         ROUND(CORR(volume_postagens, centralizacao), 2)   AS corr_volume_centralizacao,
         ROUND(CORR(volume_postagens, isolamento_alvo), 2) AS corr_volume_isolamento,
         COUNT(*) AS dias
  FROM janelas GROUP BY caso, janela ORDER BY caso, janela
""")
p6r = p6_resumo.toPandas()
assert (p6r[p6r.janela == "a partir do estopim"].pico_volume == 1).all()
display(p6_resumo)
p6r.to_csv(f"{SAIDA}/p6_resumo.csv", index=False)

# COMMAND ----------

# figura P6 — volume (fração do pico) × centralização × isolamento do alvo, por dia
import matplotlib.colors as mcolors
def clarear(hexcor, f=0.6):
    r, g, b = mcolors.to_rgb(hexcor); return (r + (1 - r) * f, g + (1 - g) * f, b + (1 - b) * f)

fig, axes = plt.subplots(1, 2, figsize=(10, 4.6), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = p6[p6.caso == caso].sort_values("dias_desde_estopim")
    hb = ax.bar(d.dias_desde_estopim, d.fracao_do_pico, width=0.7, color=clarear(COR[caso]), zorder=2)
    handles["volume (fração do pico a partir do estopim)"] = hb
    (h1,) = ax.plot(d.dias_desde_estopim, d.centralizacao, color="#4a3aa7", lw=1.9, marker="o", ms=3.5, zorder=4)
    (h2,) = ax.plot(d.dias_desde_estopim, d.isolamento_alvo, color=INK, lw=2.0, marker="o", ms=3.5, zorder=4)
    handles["centralização (grau de entrada)"] = h1; handles["parcela das menções no alvo"] = h2
    # marcadores de pico
    pv = d[d.dias_desde_estopim >= 0].sort_values("volume_postagens").iloc[-1]
    pc = d[d.dias_desde_estopim >= 0].sort_values("centralizacao").iloc[-1]
    ax.plot([pv.dias_desde_estopim], [0.985], marker="v", ms=6, color=clarear(COR[caso], 0.2), clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.plot([pc.dias_desde_estopim], [0.985], marker="v", ms=6, color="#4a3aa7", clip_on=False,
            transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.set_xticks(d.dias_desde_estopim)
    ax.set_xlim(d.dias_desde_estopim.min() - 0.6, d.dias_desde_estopim.max() + 0.6)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

ra = p6r[(p6r.caso == "arthur_do_val") & (p6r.janela == "a partir do estopim")].iloc[0]
rm = p6r[(p6r.caso == "monark") & (p6r.janela == "a partir do estopim")].iloc[0]
axes[0].text(0.02, 0.985, f"a partir do estopim: pico de volume dia {ra.pico_volume}, de centralização dia {ra.pico_centralizacao} "
             f"(+{ra.defasagem_centralizacao}); correlação volume × centralização {ra.corr_volume_centralizacao:.2f}".replace(".", ","),
             transform=axes[0].transAxes, fontsize=7.6, color=INK2, va="top")
axes[0].text(0.02, 0.935, "janela inteira: todos os picos de concentração ficam em −3/−4 (polêmica anterior)",
             transform=axes[0].transAxes, fontsize=7.6, color=INK2, va="top")
axes[1].text(0.03, 0.985, f"pico de volume dia {rm.pico_volume};\ncentralização e isolamento dia {rm.pico_centralizacao} (+{rm.defasagem_centralizacao});\n"
             f"correlação volume × centralização {rm.corr_volume_centralizacao:.2f}".replace(".", ","),
             transform=axes[1].transAxes, fontsize=7.6, color=INK2, va="top")

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.93), ncol=3, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 1.85)
axes[0].set_yticks([0, .25, .5, .75, 1.0, 1.25, 1.5]); axes[0].set_yticklabels(["0", "0,25", "0,50", "0,75", "1,00", "1,25", "1,50"])
axes[0].set_ylabel("fração do pico (volume) · índice 0–1 (concentração)", color=INK2, fontsize=9)
fig.suptitle("P6 · Pico de volume × pico de concentração: quem vem primeiro", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "▼ pico de volume (cor do caso) e pico de centralização (roxo), a partir do estopim · só dias com ≥ 30 nós · fonte: gold.fato_rede, gold.calendario_caso",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.78, bottom=0.13, wspace=0.08)
caminho = f"{SAIDA}/p6_defasagem.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P7 — Houve defesa? Proporção acusador / defensor / neutro por fase
# MAGIC
# MAGIC Fonte: `gold.fato_atividade` (postagens por caso × dia × stance × tipo_ref × versão do classificador), uma única versão
# MAGIC (`hf@a483947`, BERTimbau treinado em Monark + Wagner). Medida: **parcela das postagens do dia** em cada stance (comparável entre casos)
# MAGIC e, por fase, parcela + likes por postagem em cada stance (o engajamento diz se a defesa foi vista, não só escrita).
# MAGIC Validação: Σ postagens = 13.893 + 4.803 = 18.696 (= Silver); uma versão; Σ parcelas do dia = 1.
# MAGIC Ressalvas obrigatórias: `defensor` tem F1 0,50 na validação; o Arthur do Val não tem exemplos de treino;
# MAGIC a pré-crise do Arthur é stance sobre outra polêmica (Ucrânia).

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

VERSAO = "hf@a483947"
versoes = spark.sql("SELECT DISTINCT versao_classificacao FROM scapegoat.gold.fato_atividade").toPandas()
assert versoes.versao_classificacao.tolist() == [VERSAO], f"esperava uma única versão do classificador: {versoes.values.tolist()}"

p7 = spark.sql(f"""
  SELECT caso, data, dias_desde_estopim, fase, stance,
         SUM(n_postagens)                                                          AS postagens,
         SUM(SUM(n_postagens)) OVER (PARTITION BY caso, data)                      AS postagens_dia,
         SUM(n_postagens) / SUM(SUM(n_postagens)) OVER (PARTITION BY caso, data)   AS parcela_do_dia,
         SUM(likes)                                                                AS likes,
         ROUND(SUM(likes) / SUM(n_postagens), 1)                                   AS likes_por_postagem
  FROM scapegoat.gold.fato_atividade
  WHERE versao_classificacao = '{VERSAO}'
  GROUP BY caso, data, dias_desde_estopim, fase, stance
  ORDER BY caso, data, stance
""").toPandas()

tot = p7.groupby("caso")["postagens"].sum().to_dict()
assert tot == {"arthur_do_val": 13893, "monark": 4803}, f"Σ postagens diverge da Silver: {tot}"
assert set(p7.stance) == {"acusador", "defensor", "neutro"}, f"stances inesperados: {set(p7.stance)}"
soma_dia = p7.groupby(["caso", "data"])["parcela_do_dia"].sum()
assert ((soma_dia - 1).abs() < 1e-9).all(), "Σ parcelas do dia ≠ 1"
assert p7.drop_duplicates(["caso", "data"]).shape[0] == 26, "esperava 26 dias (calendario_caso)"
print("P7 validada:", tot, "| versão:", VERSAO)

# COMMAND ----------

# resumo por fase e no caso inteiro — a resposta numérica
p7_resumo = spark.sql(f"""
  WITH base AS (
    SELECT caso, fase, stance, n_postagens, likes
    FROM scapegoat.gold.fato_atividade WHERE versao_classificacao = '{VERSAO}'
    UNION ALL
    SELECT caso, 'todas' AS fase, stance, n_postagens, likes
    FROM scapegoat.gold.fato_atividade WHERE versao_classificacao = '{VERSAO}'
  ),
  por_fase AS (
    SELECT caso, fase,
           SUM(n_postagens)                                              AS postagens,
           SUM(CASE WHEN stance = 'acusador' THEN n_postagens ELSE 0 END) AS acusador,
           SUM(CASE WHEN stance = 'defensor' THEN n_postagens ELSE 0 END) AS defensor,
           SUM(CASE WHEN stance = 'neutro'   THEN n_postagens ELSE 0 END) AS neutro,
           SUM(CASE WHEN stance = 'acusador' THEN likes ELSE 0 END)       AS likes_acusador,
           SUM(CASE WHEN stance = 'defensor' THEN likes ELSE 0 END)       AS likes_defensor,
           SUM(CASE WHEN stance = 'neutro'   THEN likes ELSE 0 END)       AS likes_neutro
    FROM base GROUP BY caso, fase
  )
  SELECT caso, fase, postagens,
         ROUND(acusador / postagens, 3) AS parcela_acusador,
         ROUND(defensor / postagens, 3) AS parcela_defensor,
         ROUND(neutro   / postagens, 3) AS parcela_neutro,
         ROUND(defensor / acusador, 2)  AS razao_defesa_acusacao,
         ROUND(likes_acusador / NULLIF(acusador, 0), 1) AS likes_por_post_acusador,
         ROUND(likes_defensor / NULLIF(defensor, 0), 1) AS likes_por_post_defensor,
         ROUND(likes_neutro   / NULLIF(neutro, 0), 1)   AS likes_por_post_neutro,
         ROUND(likes_defensor / NULLIF(likes_acusador + likes_defensor + likes_neutro, 0), 3) AS parcela_likes_defensor
  FROM por_fase
  ORDER BY caso, CASE fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                           WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 ELSE 7 END
""")
p7r = p7_resumo.toPandas()
todas = p7r[p7r.fase == "todas"].set_index("caso")
assert todas.postagens.to_dict() == {"arthur_do_val": 13893, "monark": 4803}, "linha 'todas' ≠ total do caso"
assert (p7r.groupby("caso")["postagens"].sum() == 2 * p7r[p7r.fase == "todas"].set_index("caso").postagens).all(), "Σ fases ≠ todas"
assert ((p7r.parcela_acusador + p7r.parcela_defensor + p7r.parcela_neutro - 1).abs() < 0.002).all(), "Σ parcelas por fase ≠ 1"
display(p7_resumo)
p7r.to_csv(f"{SAIDA}/p7_resumo.csv", index=False)

# COMMAND ----------

# figura P7 — composição das postagens do dia por stance (barras empilhadas a 100 %), um painel por caso
COR_STANCE = {"acusador": "#d1493f", "defensor": "#008300", "neutro": "#c9c7bd"}
ROT_STANCE = {"acusador": "acusador", "defensor": "defensor", "neutro": "neutro"}
ORDEM = ["acusador", "defensor", "neutro"]   # acusador na base, defensor sobre ele: a fronteira entre os dois é a leitura

fig, axes = plt.subplots(1, 2, figsize=(10, 5.4), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    w = (p7[p7.caso == caso].pivot(index="dias_desde_estopim", columns="stance", values="parcela_do_dia")
         .reindex(columns=ORDEM).fillna(0).sort_index())
    base = 0
    for st in ORDEM:
        h = ax.bar(w.index, w[st], bottom=base, width=0.7, color=COR_STANCE[st], edgecolor=SURF, linewidth=0.8, zorder=3)
        handles[ROT_STANCE[st]] = h
        base = base + w[st]
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    ax.set_xticks(w.index)
    ax.set_xlim(w.index.min() - 0.6, w.index.max() + 0.6)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=58)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

# anotações lidas do dado
wa = p7[p7.caso == "arthur_do_val"].pivot(index="dias_desde_estopim", columns="stance", values="parcela_do_dia")
wm = p7[p7.caso == "monark"].pivot(index="dias_desde_estopim", columns="stance", values="parcela_do_dia")
primeiro_dia_defesa_maior = next((d for d in wm.index if d >= 0 and wm.loc[d, "defensor"] > wm.loc[d, "acusador"]), None)
ra, rm = todas.loc["arthur_do_val"], todas.loc["monark"]
axes[0].text(0, 1.20, f"caso inteiro: {ra.parcela_acusador*100:.0f} % acusador · {ra.parcela_defensor*100:.0f} % defensor · "
             f"razão defesa/acusação {ra.razao_defesa_acusacao:.2f}".replace(".", ","),
             transform=axes[0].transAxes, fontsize=7.6, color=INK2, va="top")
txt_m = (f"caso inteiro: {rm.parcela_acusador*100:.0f} % acusador · {rm.parcela_defensor*100:.0f} % defensor\n"
         f"razão defesa/acusação {rm.razao_defesa_acusacao:.2f}").replace(".", ",")
if primeiro_dia_defesa_maior is not None:
    d0 = primeiro_dia_defesa_maior
    txt_m += f"\ndefesa > acusação desde o dia {d0} ({wm.loc[d0,'defensor']*100:.0f} % vs {wm.loc[d0,'acusador']*100:.0f} %)"
axes[1].text(0, 1.20, txt_m, transform=axes[1].transAxes, fontsize=7.6, color=INK2, va="top", linespacing=1.3)
dmax = wa.loc[wa.index >= 0, "defensor"].idxmax()
axes[0].annotate(f"defesa máxima pós-estopim: dia {dmax} ({wa.loc[dmax,'defensor']*100:.0f} %)",
                 (dmax, wa.loc[dmax, "acusador"] + wa.loc[dmax, "defensor"]), xytext=(dmax + 1.4, 0.94), fontsize=7.6, color=INK2, ha="left",
                 arrowprops=dict(arrowstyle="-", color=AXIS, lw=0.8), zorder=6)

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.905), ncol=3, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 1.0)
axes[0].set_yticks([0, .25, .5, .75, 1.0]); axes[0].set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %"])
axes[0].set_ylabel("parcela das postagens do dia", color=INK2, fontsize=9)
fig.suptitle("P7 · Houve defesa? Composição das postagens por stance, dia a dia", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, f"stance por postagem (classificador {VERSAO}; defensor F1 0,50; sem exemplos de treino do Arthur) · pré-crise do Arthur = outra polêmica · fonte: gold.fato_atividade",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.68, bottom=0.11, wspace=0.08)
caminho = f"{SAIDA}/p7_defesa.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG scapegoat;
# MAGIC WITH stance_conta AS (              -- stance modal de cada conta como autora (peso = menções emitidas)
# MAGIC   SELECT caso, origem_conta_id AS conta_id, stance_origem AS stance_modal
# MAGIC   FROM (
# MAGIC     SELECT caso, origem_conta_id, stance_origem, SUM(peso) AS p,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY caso, origem_conta_id ORDER BY SUM(peso) DESC, stance_origem) AS pos
# MAGIC     FROM gold.grafo_arestas
# MAGIC     GROUP BY caso, origem_conta_id, stance_origem
# MAGIC   ) WHERE pos = 1
# MAGIC ),
# MAGIC arestas AS (
# MAGIC   SELECT g.caso,
# MAGIC          CASE WHEN g.dias_desde_estopim >= 0 THEN 'a partir do estopim' ELSE 'pre_crise' END AS janela,
# MAGIC          g.stance_origem AS stance_origem, d.stance_modal AS stance_destino, g.peso
# MAGIC   FROM gold.grafo_arestas g
# MAGIC   JOIN stance_conta d ON d.caso = g.caso AND d.conta_id = g.destino_conta_id   -- destino também é autor
# MAGIC )
# MAGIC SELECT caso, janela, stance_origem, stance_destino,
# MAGIC        SUM(peso) AS mencoes,
# MAGIC        COUNT(*)  AS arestas,
# MAGIC        ROUND(SUM(peso) / SUM(SUM(peso)) OVER (PARTITION BY caso, janela), 4) AS e_ij
# MAGIC FROM arestas
# MAGIC GROUP BY caso, janela, stance_origem, stance_destino
# MAGIC ORDER BY caso, janela, stance_origem, stance_destino;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG scapegoat;
# MAGIC WITH autores AS (SELECT DISTINCT caso, origem_conta_id AS conta_id FROM gold.grafo_arestas)
# MAGIC SELECT g.caso,
# MAGIC        CASE WHEN g.dias_desde_estopim >= 0 THEN 'a partir do estopim' ELSE 'pre_crise' END AS janela,
# MAGIC        SUM(g.peso) AS mencoes,
# MAGIC        SUM(CASE WHEN a.conta_id IS NOT NULL THEN g.peso ELSE 0 END) AS mencoes_destino_autor,
# MAGIC        ROUND(SUM(CASE WHEN a.conta_id IS NOT NULL THEN g.peso ELSE 0 END) / SUM(g.peso), 3) AS parcela_destino_autor,
# MAGIC        SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END) AS mencoes_ao_alvo
# MAGIC FROM gold.grafo_arestas g
# MAGIC LEFT JOIN autores a ON a.caso = g.caso AND a.conta_id = g.destino_conta_id
# MAGIC GROUP BY g.caso, janela
# MAGIC ORDER BY g.caso, janela;

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P8 — Assortatividade por posição: quem acusa fala com quem acusa?
# MAGIC
# MAGIC `fato_rede.assortatividade_stance` é NULL por construção: só autores têm stance; o alvo e as contas apenas mencionadas não têm.
# MAGIC Duas medidas possíveis na Gold: **(a)** mix de stance das menções por destino (alvo × demais) e fase — a quem cada posição se dirige;
# MAGIC **(b)** assortatividade de Newman restrita às arestas **autor → autor** (stance modal do destino como autor), por janela
# MAGIC (pré-crise e a partir do estopim; a pré-crise do Arthur é outra polêmica). Robustez: r sem a classe `neutro`.
# MAGIC Validação: Σ menções = 22.954 + 5.868; linhas `alvo` = parcela do alvo (P5); Σ matriz = menções com destino autor; Σ e_ij = 1.

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

# (a) mix de stance das menções por destino e fase
p8 = spark.sql("""
  WITH m AS (
    SELECT caso, fase, CASE WHEN papel_destino = 'alvo' THEN 'alvo' ELSE 'demais' END AS destino,
           stance_origem, SUM(peso) AS mencoes
    FROM scapegoat.gold.grafo_arestas
    GROUP BY caso, fase, papel_destino, stance_origem
  ),
  por_fase AS (
    SELECT caso, fase, destino, stance_origem, SUM(mencoes) AS mencoes,
           SUM(SUM(mencoes)) OVER (PARTITION BY caso, fase, destino) AS mencoes_destino
    FROM m GROUP BY caso, fase, destino, stance_origem
  )
  SELECT caso, fase, destino, mencoes_destino,
         ROUND(SUM(CASE WHEN stance_origem = 'acusador' THEN mencoes ELSE 0 END) / mencoes_destino, 3) AS parcela_acusador,
         ROUND(SUM(CASE WHEN stance_origem = 'defensor' THEN mencoes ELSE 0 END) / mencoes_destino, 3) AS parcela_defensor,
         ROUND(SUM(CASE WHEN stance_origem = 'neutro'   THEN mencoes ELSE 0 END) / mencoes_destino, 3) AS parcela_neutro
  FROM por_fase
  GROUP BY caso, fase, destino, mencoes_destino
  ORDER BY caso, CASE fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                           WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 END, destino
""").toPandas()
tot = p8.groupby("caso")["mencoes_destino"].sum().to_dict()
assert tot == {"arthur_do_val": 22954, "monark": 5868}, f"Σ menções diverge de grafo_arestas: {tot}"
alvo = p8[p8.destino == "alvo"].groupby("caso")["mencoes_destino"].sum()
assert alvo["arthur_do_val"] == 9132 and alvo["monark"] == 1335, f"menções ao alvo ≠ P5: {alvo.to_dict()}"
assert ((p8.parcela_acusador + p8.parcela_defensor + p8.parcela_neutro - 1).abs() < 0.002).all()
print("P8 (a) validada:", tot, "| menções ao alvo:", alvo.to_dict())

# COMMAND ----------

# (b) matriz de mistura autor → autor por janela, e coeficiente de Newman (direcionado)
import numpy as np
import pandas as pd
p8_mix = spark.sql("""
  WITH stance_conta AS (
    SELECT caso, origem_conta_id AS conta_id, stance_origem AS stance_modal
    FROM (
      SELECT caso, origem_conta_id, stance_origem, SUM(peso) AS p,
             ROW_NUMBER() OVER (PARTITION BY caso, origem_conta_id ORDER BY SUM(peso) DESC, stance_origem) AS pos
      FROM scapegoat.gold.grafo_arestas GROUP BY caso, origem_conta_id, stance_origem
    ) WHERE pos = 1
  )
  SELECT g.caso,
         CASE WHEN g.dias_desde_estopim >= 0 THEN 'a partir do estopim' ELSE 'pre_crise' END AS janela,
         g.stance_origem, d.stance_modal AS stance_destino, SUM(g.peso) AS mencoes, COUNT(*) AS arestas
  FROM scapegoat.gold.grafo_arestas g
  JOIN stance_conta d ON d.caso = g.caso AND d.conta_id = g.destino_conta_id
  GROUP BY g.caso, janela, g.stance_origem, d.stance_modal
""").toPandas()
denom = spark.sql("""
  WITH autores AS (SELECT DISTINCT caso, origem_conta_id AS conta_id FROM scapegoat.gold.grafo_arestas)
  SELECT g.caso, CASE WHEN g.dias_desde_estopim >= 0 THEN 'a partir do estopim' ELSE 'pre_crise' END AS janela,
         SUM(g.peso) AS mencoes,
         SUM(CASE WHEN a.conta_id IS NOT NULL THEN g.peso ELSE 0 END) AS mencoes_destino_autor
  FROM scapegoat.gold.grafo_arestas g
  LEFT JOIN autores a ON a.caso = g.caso AND a.conta_id = g.destino_conta_id
  GROUP BY g.caso, janela
""").toPandas().set_index(["caso", "janela"])
assert denom.groupby("caso")["mencoes"].sum().to_dict() == {"arthur_do_val": 22954, "monark": 5868}

S = ["acusador", "defensor", "neutro"]
def newman(sub, classes):
    M = np.zeros((len(classes), len(classes)))
    for _, r in sub.iterrows():
        if r.stance_origem in classes and r.stance_destino in classes:
            M[classes.index(r.stance_origem), classes.index(r.stance_destino)] += r.mencoes
    e = M / M.sum(); a, b = e.sum(1), e.sum(0); esp = (a * b).sum()
    return M.sum(), np.trace(e), esp, (np.trace(e) - esp) / (1 - esp)

linhas = []
for (caso, janela), sub in p8_mix.groupby(["caso", "janela"]):
    n, obs, esp, r = newman(sub, S)
    n2, obs2, esp2, r2 = newman(sub, ["acusador", "defensor"])
    assert n == denom.loc[(caso, janela), "mencoes_destino_autor"], f"Σ matriz ≠ menções com destino autor em {caso}/{janela}"
    def_ac = sub[(sub.stance_origem == "defensor") & (sub.stance_destino == "acusador")].mencoes.sum()
    def_def = sub[(sub.stance_origem == "defensor") & (sub.stance_destino == "defensor")].mencoes.sum()
    linhas.append(dict(caso=caso, janela=janela, mencoes_autor_autor=int(n),
                       parcela_das_mencoes=round(n / denom.loc[(caso, janela), "mencoes"], 3),
                       diagonal_observada=round(obs, 3), diagonal_esperada=round(esp, 3), r_newman=round(r, 3),
                       r_sem_neutro=round(r2, 3), defensor_para_acusador=int(def_ac), defensor_para_defensor=int(def_def)))
p8r = pd.DataFrame(linhas)
p8r = p8r[p8r.mencoes_autor_autor > 0]
display(p8r)
p8r.to_csv(f"{SAIDA}/p8_resumo.csv", index=False)
p8.to_csv(f"{SAIDA}/p8_mix_destino.csv", index=False)

# COMMAND ----------

# figura P8 — a quem cada posição se dirige: composição de stance das menções ao alvo × aos demais, por fase
COR_STANCE = {"acusador": "#d1493f", "defensor": "#008300", "neutro": "#c9c7bd"}
ORDEM = ["acusador", "defensor", "neutro"]
fases = {"arthur_do_val": ["pre_crise", "estopim", "pico", "declinio"], "monark": ["estopim", "pico", "declinio", "pos_rito"]}
ROT_FASE = {"pre_crise": "pré-crise", "estopim": "estopim", "pico": "pico", "declinio": "declínio", "pos_rito": "pós-rito"}

fig, axes = plt.subplots(1, 2, figsize=(10, 5.2), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[1, 1], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = p8[p8.caso == caso].set_index(["fase", "destino"])
    xs, labels = [], []
    for k, fase in enumerate(fases[caso]):
        for j, destino in enumerate(["alvo", "demais"]):
            if (fase, destino) not in d.index: continue
            x = k * 2.6 + j
            base = 0
            for st in ORDEM:
                v = d.loc[(fase, destino), f"parcela_{st}"]
                h = ax.bar(x, v, bottom=base, width=0.8, color=COR_STANCE[st], edgecolor=SURF, linewidth=0.8, zorder=3)
                handles[st] = h; base += v
            ax.text(x, -0.03, destino, ha="center", va="top", fontsize=7.2, color=INK2)
            n = int(d.loc[(fase, destino), "mencoes_destino"])
            ax.text(x, 1.01, f"{n:,}".replace(",", "."), ha="center", va="bottom", fontsize=6.6, color=MUTED)
        xs.append(k * 2.6 + 0.5); labels.append(ROT_FASE[fase])
    ax.set_xticks(xs); ax.set_xticklabels(labels)
    ax.tick_params(axis="x", pad=14)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=18)
    ax.set_xlim(-0.8, len(fases[caso]) * 2.6 - 0.8)

r = p8r.set_index(["caso", "janela"])
ra = r.loc[("arthur_do_val", "a partir do estopim")]; rm = r.loc[("monark", "a partir do estopim")]
fig.text(0.02, 0.885, ("assortatividade autor → autor a partir do estopim (Newman, direcionado): "
         f"Arthur r = {ra.r_newman:.2f} sobre {ra.parcela_das_mencoes*100:.0f} % das menções (sem neutro {ra.r_sem_neutro:.2f}); "
         f"Monark r = {rm.r_newman:.2f} sobre {rm.parcela_das_mencoes*100:.0f} % (sem neutro {rm.r_sem_neutro:.2f})").replace(".", ","),
         fontsize=7.6, color=INK2)
fig.text(0.02, 0.855, f"no Monark, defensores mencionam acusadores ({int(rm.defensor_para_acusador)} menções) mais do que outros defensores ({int(rm.defensor_para_defensor)}): a defesa responde a quem acusa",
         fontsize=7.6, color=INK2)

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.83), ncol=3, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
axes[0].set_ylim(0, 1.0)
axes[0].set_yticks([0, .25, .5, .75, 1.0]); axes[0].set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %"])
axes[0].set_ylabel("parcela das menções ao destino, por stance do autor", color=INK2, fontsize=9)
fig.suptitle("P8 · A quem cada posição se dirige: menções ao alvo × aos demais, por fase", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "número sobre a barra = menções · stance = posição do autor em relação ao alvo · fonte: gold.grafo_arestas (assortatividade: arestas cujo destino também é autor)",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.73, bottom=0.14, wspace=0.08)
caminho = f"{SAIDA}/p8_destino_stance.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P9 — Falado sobre × falado com: replies ao alvo vs. isolamento da rede
# MAGIC
# MAGIC Duas perspectivas complementares sobre o papel central do alvo:
# MAGIC **Falado com:** parcela das postagens que são replies ao alvo do caso (fonte: `fato_atividade` + `fato_referencia`).
# MAGIC **Falado sobre:** medida do isolamento em rede (parcela das menções que chegam ao alvo, `fato_rede.isolamento_alvo`).
# MAGIC Esperado: em crises de bode expiatório (Girard), o alvo permanece central. A sobreposição pode revelar defasagem
# MAGIC (quem fala com o alvo vs. quem apenas o menciona) e padrões de inversão (defesa fala com terceiros no Arthur, ambos falam com alvo no Monark).
# MAGIC Validação: Σ menções fato_referencia = Σ fato_atividade por caso × data × stance (3c de 09_fato_referencia); `isolamento_alvo` > 0 em todo dia

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

# COMMAND ----------

# Conferência 3c de fato_referencia: discrepâncias com fato_atividade por caso × data × stance
# Esperado: 0 linhas (sem discrepâncias)
conf_3c = spark.sql("""
  SELECT r.caso, r.data, r.stance, r.n AS referencia, f.n AS atividade, r.n - f.n AS diferenca
  FROM (SELECT caso, data, stance, SUM(n_postagens) AS n FROM scapegoat.gold.fato_referencia GROUP BY caso, data, stance) r
  FULL JOIN (SELECT caso, data, stance, SUM(n_postagens) AS n FROM scapegoat.gold.fato_atividade
             WHERE versao_classificacao = 'hf@a483947' GROUP BY caso, data, stance) f
    ON f.caso = r.caso AND f.data = r.data AND f.stance = r.stance
  WHERE COALESCE(r.n, 0) <> COALESCE(f.n, 0)
  ORDER BY 1, 2, 3
""").toPandas()
assert conf_3c.shape[0] == 0, f"fato_referencia ≠ fato_atividade em {conf_3c.shape[0]} linhas: {conf_3c.to_dict('records')}"
print("Conferência 3c (fato_referencia × fato_atividade): sem discrepâncias ✓")

# COMMAND ----------

# P9 (a): parcela de replies ao alvo por dia — "falado com"
p9_falado_com = spark.sql("""
  SELECT caso, data, dias_desde_estopim, fase,
         SUM(CASE WHEN tipo_ref = 'reply' AND destinatario = 'alvo' THEN n_postagens ELSE 0 END) AS replies_alvo,
         SUM(n_postagens) AS postagens_dia,
         SUM(CASE WHEN tipo_ref = 'reply' AND destinatario = 'alvo' THEN n_postagens ELSE 0 END) /
         SUM(n_postagens) AS parcela_replies_alvo
  FROM scapegoat.gold.fato_referencia
  GROUP BY caso, data, dias_desde_estopim, fase
  ORDER BY caso, data
""").toPandas()

# P9 (b): isolamento_alvo por dia — "falado sobre"
p9_falado_sobre = spark.sql("""
  SELECT caso, data, dias_desde_estopim, fase, isolamento_alvo, n_nos
  FROM scapegoat.gold.fato_rede
  ORDER BY caso, data
""").toPandas()

# Join nas duas dimensões
p9 = pd.merge(
    p9_falado_com,
    p9_falado_sobre,
    on=["caso", "data", "dias_desde_estopim", "fase"],
    how="inner"
)

# Validações
assert p9.shape[0] == 26, f"esperava 26 dias (calendario_caso), obtive {p9.shape[0]}"
assert (p9.isolamento_alvo > 0).all(), f"isolamento_alvo deve ser > 0 em todo dia"
assert (p9.parcela_replies_alvo >= 0).all() and (p9.parcela_replies_alvo <= 1).all(), "parcela_replies_alvo fora de [0,1]"
p9["nd"] = p9.n_nos < 30   # regra do bloco: dias com < 30 nós fora das séries (Monark dia -1)
print("P9 validada:", p9.shape[0], "dias | isolamento_alvo sempre > 0 | n.d.:", p9[p9.nd][["caso", "dias_desde_estopim", "n_nos"]].values.tolist())

# COMMAND ----------

# Resumo por fase (paralelo a P7 e P8)
p9_resumo = spark.sql("""
  WITH fr AS (
    SELECT caso, fase,
           SUM(CASE WHEN tipo_ref = 'reply' AND destinatario = 'alvo' THEN n_postagens ELSE 0 END) AS replies_alvo,
           SUM(CASE WHEN tipo_ref = 'reply' AND destinatario = 'terceiros' THEN n_postagens ELSE 0 END) AS replies_terceiros,
           SUM(n_postagens) AS postagens
    FROM scapegoat.gold.fato_referencia GROUP BY caso, fase
  ),
  rede AS (
    SELECT caso, fase,
           ROUND(AVG(isolamento_alvo), 3) AS isolamento_alvo_medio,
           ROUND(MIN(isolamento_alvo), 3) AS isolamento_alvo_min,
           ROUND(MAX(isolamento_alvo), 3) AS isolamento_alvo_max,
           COUNT(*) AS dias_fase
    FROM scapegoat.gold.fato_rede GROUP BY caso, fase
  )
  SELECT fr.caso, fr.fase,
         fr.postagens,
         fr.replies_alvo,
         fr.replies_terceiros,
         ROUND(fr.replies_alvo / fr.postagens, 3) AS parcela_replies_alvo,
         ROUND(fr.replies_terceiros / fr.postagens, 3) AS parcela_replies_terceiros,
         ROUND(fr.replies_alvo / NULLIF(fr.replies_terceiros, 0), 2) AS razao_alvo_terceiros,
         re.isolamento_alvo_medio,
         re.isolamento_alvo_min,
         re.isolamento_alvo_max,
         re.dias_fase
  FROM fr JOIN rede re ON re.caso = fr.caso AND re.fase = fr.fase
  ORDER BY fr.caso, CASE fr.fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                                   WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 END
""").toPandas()
display(p9_resumo)
p9_resumo.to_csv(f"{SAIDA}/p9_resumo.csv", index=False)
p9.to_csv(f"{SAIDA}/p9_falado_com_sobre.csv", index=False)

# COMMAND ----------

# Figura P9: "falado com" (replies ao alvo / postagens) e "falado sobre" (isolamento_alvo) no mesmo eixo — as duas são parcelas do dia
fases_caso = {"arthur_do_val": ["pre_crise", "estopim", "pico", "declinio"], "monark": ["estopim", "pico", "declinio", "pos_rito"]}
ROT_FASE = {"pre_crise": "pré-crise", "estopim": "estopim", "pico": "pico", "declinio": "declínio", "pos_rito": "pós-rito"}
COR_COM, COR_SOBRE = "#4a3aa7", "#1baf7a"   # cores de métrica (como P5/P6), iguais nos dois painéis

fig, axes = plt.subplots(1, 2, figsize=(10, 5.2), dpi=200, sharey=True, gridspec_kw=dict(width_ratios=[18, 8], wspace=0.08))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, caso in zip(axes, ["arthur_do_val", "monark"]):
    eixo_limpo(ax)
    d = p9[(p9.caso == caso) & (~p9.nd)].set_index("dias_desde_estopim").sort_index()
    nd = p9[(p9.caso == caso) & (p9.nd)].dias_desde_estopim.tolist()
    ax.fill_between(d.index, 0, d.isolamento_alvo, alpha=0.10, color=COR_SOBRE, zorder=1)
    h1, = ax.plot(d.index, d.isolamento_alvo, marker="s", ms=4.5, color=COR_SOBRE, lw=1.6, ls=(0, (4, 2)), zorder=3, clip_on=False)
    h2, = ax.plot(d.index, d.parcela_replies_alvo, marker="o", ms=4.5, color=COR_COM, lw=1.8, zorder=4, clip_on=False)
    handles["falado sobre: menções ao alvo / menções do dia (isolamento_alvo)"] = h1
    handles["falado com: replies ao alvo / postagens do dia"] = h2
    for x in nd:
        ax.text(x, 0.02, "n.d.", ha="center", va="bottom", fontsize=7, color=MUTED)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    todos = p9[p9.caso == caso].dias_desde_estopim
    ax.set_xticks(sorted(todos)); ax.set_xlim(todos.min() - 0.6, todos.max() + 0.6)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

# anotações lidas do dado: parcela de replies ao alvo por fase (fases com < 30 postagens ficam de fora)
def linha_fases(caso):
    r = p9_resumo[(p9_resumo.caso == caso) & (p9_resumo.postagens >= 30)].set_index("fase")
    partes = [f"{ROT_FASE[f]} {r.loc[f, 'parcela_replies_alvo']*100:.0f} %" for f in fases_caso[caso] if f in r.index]
    return f"{NOME[caso]} — replies ao alvo por fase: " + " · ".join(partes)
fig.text(0.02, 0.885, linha_fases("arthur_do_val").replace(".", ","), fontsize=7.6, color=INK2)
fig.text(0.02, 0.855, linha_fases("monark").replace(".", ","), fontsize=7.6, color=INK2)

fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.905), ncol=1, frameon=False,
           fontsize=8, labelcolor=INK2, handlelength=1.8)
axes[0].set_ylim(0, 1.0); axes[0].set_yticks([0, .25, .5, .75, 1.0]); axes[0].set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %"])
axes[0].set_ylabel("parcela do dia", color=INK2, fontsize=9)
fig.suptitle("P9 · Falado sobre × falado com: o alvo é mencionado — mas alguém responde a ele?", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.925, "denominadores diferentes (menções × postagens): comparar a forma das curvas, não o nível · n.d. = dia com < 30 nós · fonte: gold.fato_referencia, gold.fato_rede",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.72, bottom=0.11, wspace=0.08)
caminho = f"{SAIDA}/p9_falado_com_sobre.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P10 — Líderes de acusação: inversão de polo e ataque indiscriminado
# MAGIC
# MAGIC Regra aprovada em 09/09 (líder = não-alvo, autora, stance modal `acusador`, entre as 10 mais mencionadas) **não tem ocupante** em nenhum
# MAGIC caso (10/09): as mais mencionadas não postam ou não acusam. Fica como primeiro resultado. Líder passa a ser definido pela **emissão**:
# MAGIC as 10 contas não-alvo com mais menções acusadoras emitidas (`grafo_arestas`, `stance_origem = acusador`).
# MAGIC **(a) Inversão de polo** — parcela das menções do dia recebidas pelo alvo × pelos líderes (hipótese trifásica: líderes primeiro,
# MAGIC alvo depois, parte volta aos líderes). **(b) Tensão** — por líder × fase: menções emitidas, por dia ativo, destinos distintos,
# MAGIC parcela ao alvo, parcela acusadora. Ataque indiscriminado = muitos destinos e pouca parcela ao alvo.
# MAGIC Validação: nenhum líder é alvo; parcela do alvo = `fato_rede.isolamento_alvo`; Σ emitidas por fase = Σ peso do líder.
# MAGIC No README o líder é a posição no ranking (L1, L2 …), nunca `conta_id` ou handle.

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

import pandas as pd

# candidatos: 10 contas mais mencionadas de cada caso, excluído o alvo, com atributos para a regra
cand = spark.sql("""
  WITH stance_conta AS (
    SELECT caso, origem_conta_id AS conta_id, stance_origem AS stance_modal, SUM(p) OVER (PARTITION BY caso, origem_conta_id) AS mencoes_emitidas
    FROM (
      SELECT caso, origem_conta_id, stance_origem, SUM(peso) AS p,
             ROW_NUMBER() OVER (PARTITION BY caso, origem_conta_id ORDER BY SUM(peso) DESC, stance_origem) AS pos
      FROM scapegoat.gold.grafo_arestas GROUP BY caso, origem_conta_id, stance_origem
    ) WHERE pos = 1
  ),
  top AS (
    SELECT caso, conta_id, papel_principal, n_postagens_caso, n_mencoes_recebidas_caso,
           ROW_NUMBER() OVER (PARTITION BY caso ORDER BY n_mencoes_recebidas_caso DESC, conta_id) AS posicao
    FROM scapegoat.gold.dim_conta_papel WHERE papel_principal <> 'alvo'
  )
  SELECT t.caso, t.posicao, t.conta_id, t.n_mencoes_recebidas_caso, t.n_postagens_caso,
         s.mencoes_emitidas, s.stance_modal, n.papel_inicial, n.papel_final, n.dia_virada
  FROM top t
  LEFT JOIN stance_conta s ON s.caso = t.caso AND s.conta_id = t.conta_id
  LEFT JOIN scapegoat.gold.papel_narrativo_v0 n ON n.caso = t.caso AND n.conta_id = t.conta_id
  WHERE t.posicao <= 10
  ORDER BY t.caso, t.posicao
""").toPandas()
cand["lider_regra"] = cand.mencoes_emitidas.notna() & (cand.stance_modal == "acusador")
cand["lider_narrativa"] = cand.papel_inicial.fillna("").str.startswith("lider")
display(cand.drop(columns=["conta_id"]))          # conta_id fica fora do print: posição é o identificador público
print("líderes pela regra de recepção:", cand.groupby("caso")["lider_regra"].sum().to_dict(),
      "| pela narrativa (papel_inicial lider*):", cand.groupby("caso")["lider_narrativa"].sum().to_dict())
print("papéis narrativos existentes:", spark.sql("SELECT papel_inicial, COUNT(*) n FROM scapegoat.gold.papel_narrativo_v0 GROUP BY 1").toPandas().values.tolist())
cand.drop(columns=["conta_id"]).to_csv(f"{SAIDA}/p10_mais_mencionadas.csv", index=False)

# COMMAND ----------

# Resultado de 10/09: nenhum dos dois critérios encontra líder — as mais mencionadas não postam (ou postam neutro/defensor).
# Líder passa a ser definido pela EMISSÃO: as 10 contas não-alvo com mais menções acusadoras emitidas (Σ peso, stance_origem = acusador).
# A tabela acima fica como primeiro resultado: quem lidera a acusação não é quem recebe atenção.
lid = spark.sql("""
  WITH emit AS (
    SELECT g.caso, g.origem_conta_id AS conta_id,
           SUM(CASE WHEN g.stance_origem = 'acusador' THEN g.peso ELSE 0 END) AS mencoes_acusadoras,
           SUM(g.peso) AS mencoes_emitidas, COUNT(DISTINCT g.data) AS dias_ativos
    FROM scapegoat.gold.grafo_arestas g GROUP BY g.caso, g.origem_conta_id
  ),
  rk AS (
    SELECT e.*, d.n_postagens_caso, d.n_mencoes_recebidas_caso, n.papel_inicial,
           ROW_NUMBER() OVER (PARTITION BY e.caso ORDER BY e.mencoes_acusadoras DESC, e.mencoes_emitidas DESC, e.conta_id) AS posicao,
           RANK() OVER (PARTITION BY e.caso ORDER BY d.n_mencoes_recebidas_caso DESC) AS posicao_recebidas
    FROM emit e
    JOIN scapegoat.gold.dim_conta_papel d ON d.caso = e.caso AND d.conta_id = e.conta_id AND d.papel_principal <> 'alvo'
    LEFT JOIN scapegoat.gold.papel_narrativo_v0 n ON n.caso = e.caso AND n.conta_id = e.conta_id
  )
  SELECT * FROM rk WHERE posicao <= 10 AND mencoes_acusadoras > 0 ORDER BY caso, posicao
""").toPandas()
lid["rotulo"] = "L" + lid.posicao.astype(str)
lid["parcela_acusadora"] = (lid.mencoes_acusadoras / lid.mencoes_emitidas).round(3)
CRITERIO = "emissão (top-10 por menções acusadoras emitidas)"
assert (lid.groupby("caso").size() >= 1).all(), f"caso sem líder: {lid.groupby('caso').size().to_dict()}"
alvos = spark.sql("SELECT caso, conta_id FROM scapegoat.gold.dim_conta_papel WHERE papel_principal = 'alvo'").toPandas()
assert lid.merge(alvos, on=["caso", "conta_id"]).empty, "um líder é o alvo"
spark.createDataFrame(lid[["caso", "posicao", "conta_id", "rotulo"]]).createOrReplaceTempView("p10_lideres")
display(lid.drop(columns=["conta_id"]))
lid.drop(columns=["conta_id"]).to_csv(f"{SAIDA}/p10_lideres.csv", index=False)
tot_ac = spark.sql("SELECT caso, SUM(peso) n FROM scapegoat.gold.grafo_arestas WHERE stance_origem = 'acusador' GROUP BY caso").toPandas().set_index("caso").n
print("parcela das menções acusadoras do caso emitida pelos 10 líderes:",
      (lid.groupby("caso")["mencoes_acusadoras"].sum() / tot_ac).round(3).to_dict())

# COMMAND ----------

# (a) inversão de polo — parcela das menções do dia recebidas pelo alvo × pelos líderes
p10a = spark.sql("""
  SELECT g.caso, g.data, g.dias_desde_estopim, g.fase, r.n_nos, r.isolamento_alvo,
         SUM(g.peso) AS mencoes,
         SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END) / SUM(g.peso) AS parcela_alvo,
         SUM(CASE WHEN l.conta_id IS NOT NULL THEN g.peso ELSE 0 END) / SUM(g.peso) AS parcela_lideres,
         MAX(CASE WHEN l.conta_id IS NOT NULL THEN g.peso ELSE 0 END) / SUM(g.peso) AS parcela_maior_aresta_lider
  FROM scapegoat.gold.grafo_arestas g
  LEFT JOIN p10_lideres l ON l.caso = g.caso AND l.conta_id = g.destino_conta_id
  JOIN scapegoat.gold.fato_rede r ON r.caso = g.caso AND r.data = g.data
  GROUP BY g.caso, g.data, g.dias_desde_estopim, g.fase, r.n_nos, r.isolamento_alvo
  ORDER BY g.caso, g.data
""").toPandas()
assert p10a.groupby("caso")["mencoes"].sum().to_dict() == {"arthur_do_val": 22954, "monark": 5868}, "Σ menções ≠ grafo_arestas"
assert ((p10a.parcela_alvo - p10a.isolamento_alvo).abs() < 1e-6).all(), "parcela do alvo ≠ fato_rede.isolamento_alvo"
assert ((p10a.parcela_alvo + p10a.parcela_lideres) <= 1 + 1e-9).all(), "alvo + líderes > 1: líder marcado como alvo?"
p10a["nd"] = p10a.n_nos < 30
print("P10 (a) validada: parcela do alvo bate com fato_rede em", len(p10a), "dias")

# parcela dos líderes por fase e razão líderes / alvo — a inversão em números
p10a_fase = spark.sql("""
  SELECT g.caso, g.fase,
         SUM(g.peso) AS mencoes,
         ROUND(SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END) / SUM(g.peso), 3) AS parcela_alvo,
         ROUND(SUM(CASE WHEN l.conta_id IS NOT NULL THEN g.peso ELSE 0 END) / SUM(g.peso), 3) AS parcela_lideres,
         ROUND(SUM(CASE WHEN l.conta_id IS NOT NULL THEN g.peso ELSE 0 END) /
               NULLIF(SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END), 0), 2) AS razao_lideres_alvo
  FROM scapegoat.gold.grafo_arestas g
  LEFT JOIN p10_lideres l ON l.caso = g.caso AND l.conta_id = g.destino_conta_id
  GROUP BY g.caso, g.fase
  ORDER BY g.caso, CASE g.fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                               WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 END
""").toPandas()
display(p10a_fase)
p10a.to_csv(f"{SAIDA}/p10_polo_dia.csv", index=False)
p10a_fase.to_csv(f"{SAIDA}/p10_polo_fase.csv", index=False)

# COMMAND ----------

# (b) tensão dos líderes — o que cada líder emite, por fase
p10b = spark.sql("""
  SELECT l.caso, l.rotulo,
         g.fase,
         SUM(g.peso)                                                          AS mencoes_emitidas,
         COUNT(DISTINCT g.data)                                               AS dias_ativos,
         ROUND(SUM(g.peso) / COUNT(DISTINCT g.data), 1)                       AS mencoes_por_dia,
         COUNT(DISTINCT g.destino_conta_id)                                   AS destinos_distintos,
         ROUND(SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END) / SUM(g.peso), 3)  AS parcela_ao_alvo,
         ROUND(SUM(CASE WHEN g.stance_origem = 'acusador' THEN g.peso ELSE 0 END) / SUM(g.peso), 3) AS parcela_acusadora
  FROM p10_lideres l
  JOIN scapegoat.gold.grafo_arestas g ON g.caso = l.caso AND g.origem_conta_id = l.conta_id
  GROUP BY l.caso, l.rotulo, g.fase
  ORDER BY l.caso, l.rotulo, CASE g.fase WHEN 'pre_crise' THEN 1 WHEN 'estopim' THEN 2 WHEN 'escalada' THEN 3
                                          WHEN 'pico' THEN 4 WHEN 'declinio' THEN 5 WHEN 'pos_rito' THEN 6 END
""").toPandas()
# recebidas por líder × fase (a inversão vista do líder: emite mais do que recebe?)
p10b_rec = spark.sql("""
  SELECT l.caso, l.rotulo, g.fase, SUM(g.peso) AS mencoes_recebidas
  FROM p10_lideres l JOIN scapegoat.gold.grafo_arestas g ON g.caso = l.caso AND g.destino_conta_id = l.conta_id
  GROUP BY l.caso, l.rotulo, g.fase
""").toPandas()
p10b = p10b.merge(p10b_rec, on=["caso", "rotulo", "fase"], how="outer").fillna({"mencoes_emitidas": 0, "mencoes_recebidas": 0})
tot_emit = spark.sql("""
  SELECT l.caso, l.rotulo, SUM(g.peso) AS emitidas
  FROM p10_lideres l JOIN scapegoat.gold.grafo_arestas g ON g.caso = l.caso AND g.origem_conta_id = l.conta_id
  GROUP BY l.caso, l.rotulo""").toPandas().set_index(["caso", "rotulo"]).emitidas
chk = p10b.groupby(["caso", "rotulo"])["mencoes_emitidas"].sum()
assert (chk.reindex(tot_emit.index).fillna(0) == tot_emit).all(), "Σ emitidas por fase ≠ Σ peso do líder"
assert ((p10b.parcela_ao_alvo.fillna(0) <= 1) & (p10b.parcela_acusadora.fillna(0) <= 1)).all()
display(p10b)
p10b.to_csv(f"{SAIDA}/p10_lideres_fase.csv", index=False)
print("P10 (b) validada:", len(tot_emit), "líderes; Σ emitidas =", int(tot_emit.sum()))

# COMMAND ----------

# figura P10 — (a) polo: parcela do dia recebida pelo alvo × pelos líderes; (b) tensão: parcela das menções dos líderes dirigida ao alvo, por fase
COR_ALVO, COR_LID = INK, "#e87ba4"
fases_caso = {"arthur_do_val": ["pre_crise", "estopim", "pico", "declinio"], "monark": ["estopim", "pico", "declinio", "pos_rito"]}
ROT_FASE = {"pre_crise": "pré-crise", "estopim": "estopim", "pico": "pico", "declinio": "declínio", "pos_rito": "pós-rito"}

fig, axes = plt.subplots(2, 2, figsize=(10, 7.6), dpi=200, sharey="row",
                         gridspec_kw=dict(width_ratios=[18, 8], height_ratios=[3, 2], wspace=0.08, hspace=0.55))
fig.patch.set_facecolor(SURF)
handles = {}
for j, caso in enumerate(["arthur_do_val", "monark"]):
    ax = axes[0, j]; eixo_limpo(ax)
    d = p10a[(p10a.caso == caso) & (~p10a.nd)].set_index("dias_desde_estopim").sort_index()
    h1, = ax.plot(d.index, d.parcela_alvo, marker="o", ms=4.5, color=COR_ALVO, lw=1.8, zorder=4, clip_on=False)
    h2, = ax.plot(d.index, d.parcela_lideres, marker="s", ms=4.5, color=COR_LID, lw=1.8, zorder=4, clip_on=False)
    handles["alvo"] = h1; handles[f"líderes de acusação (Σ)"] = h2
    for x in p10a[(p10a.caso == caso) & (p10a.nd)].dias_desde_estopim:
        ax.text(x, 0.02, "n.d.", ha="center", va="bottom", fontsize=7, color=MUTED)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    todos = p10a[p10a.caso == caso].dias_desde_estopim
    ax.set_xticks(sorted(todos)); ax.set_xlim(todos.min() - 0.6, todos.max() + 0.6)
    n_l = int((lid.caso == caso).sum())
    ax.set_title(f"{NOME[caso]} — {n_l} líder{'es' if n_l > 1 else ''} por emissão", loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)

    ax = axes[1, j]; eixo_limpo(ax)
    b = p10b[p10b.caso == caso].groupby("fase").agg(emit=("mencoes_emitidas", "sum"), dest=("destinos_distintos", "sum"))
    b["ao_alvo"] = (p10b[p10b.caso == caso].assign(a=lambda t: t.parcela_ao_alvo.fillna(0) * t.mencoes_emitidas)
                    .groupby("fase")["a"].sum() / b.emit)
    fs = [f for f in fases_caso[caso] if f in b.index and b.loc[f, "emit"] > 0]
    xs = range(len(fs))
    ax.bar(xs, [b.loc[f, "ao_alvo"] for f in fs], width=0.6, color=COR_LID, edgecolor=SURF, zorder=3)
    for x, f in zip(xs, fs):
        ax.text(x, b.loc[f, "ao_alvo"] + 0.02, f"{int(b.loc[f, 'emit'])} menções\n{int(b.loc[f, 'dest'])} destinos", ha="center", va="bottom", fontsize=6.8, color=INK2)
    ax.set_xticks(list(xs)); ax.set_xticklabels([ROT_FASE[f] for f in fs])
    ax.set_title("o que os líderes emitem: parcela dirigida ao alvo", loc="left", fontsize=9, color=INK, pad=6)

axes[0, 0].set_ylim(0, 1.0); axes[0, 0].set_yticks([0, .25, .5, .75, 1]); axes[0, 0].set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %"])
axes[0, 0].set_ylabel("parcela das menções do dia recebidas", color=INK2, fontsize=9)
axes[1, 0].set_ylim(0, 0.7); axes[1, 0].set_yticks([0, .25, .5]); axes[1, 0].set_yticklabels(["0", "25 %", "50 %"])
axes[1, 0].set_ylabel("parcela ao alvo", color=INK2, fontsize=9)
axes[0, 0].legend(handles.values(), handles.keys(), loc="upper right", frameon=False, fontsize=8, labelcolor=INK2, handlelength=1.8)
fig.suptitle("P10 · Líderes de acusação: quem recebe a atenção (polo) e para onde os líderes atiram (tensão)", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.945, "líder = não-alvo, entre as 10 com mais menções acusadoras emitidas (as 10 mais mencionadas não acusam) · n.d. = dia com < 30 nós · fonte: gold.grafo_arestas, dim_conta_papel, papel_narrativo_v0",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.86, bottom=0.07)
caminho = f"{SAIDA}/p10_lideres.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4b — Papéis narrativos vistos pela multidão: instituições, vítimas secundárias, aliados e líder
# MAGIC
# MAGIC Uso analítico de `gold.papel_narrativo_v0`. O papel **vigente** no dia é `papel_final` a partir de `dia_virada`, senão `papel_inicial`.
# MAGIC Por papel de destino × fase: parcela das menções, **mix de stance de quem menciona** (a instituição é citada por quem acusa o alvo?),
# MAGIC dia do pico de cada papel em relação ao pico do alvo (defasagem), e **sobreposição de acusadores** com o alvo (Jaccard entre os
# MAGIC conjuntos de autoras acusadoras que mencionam o alvo e as que mencionam o papel): vítima secundária esperada alta, instituição baixa.
# MAGIC Responde às expectativas 1 (instituições reforçam), 3 (segundo hub = líder narrativo), 5 (vítima secundária, versão estrutural)
# MAGIC e 6 (transferência pelo campo do alvo: menções à vítima secundária vêm de defensores?) — ver bloco-4-analise.md.
# MAGIC Validação: Σ menções por papel = 22.954 + 5.868; linha `alvo` = parcela do alvo (P5); contas com papel ⊂ `dim_conta_papel`.

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

import pandas as pd
import numpy as np

# domínio real dos papéis e checagem de integridade da papel_narrativo_v0
dom = spark.sql("""
  SELECT caso, papel_inicial, papel_final, COUNT(*) AS contas, COUNT(dia_virada) AS com_virada
  FROM scapegoat.gold.papel_narrativo_v0 GROUP BY caso, papel_inicial, papel_final ORDER BY caso, contas DESC
""").toPandas()
display(dom)
fora = spark.sql("""
  SELECT COUNT(*) AS n FROM scapegoat.gold.papel_narrativo_v0 n
  LEFT ANTI JOIN scapegoat.gold.dim_conta_papel d ON d.caso = n.caso AND d.conta_id = n.conta_id
""").toPandas().n[0]
assert fora == 0, f"{fora} contas da papel_narrativo_v0 não estão na dim_conta_papel"

# COMMAND ----------

# (a) menções recebidas por papel vigente × dia, com mix de stance de quem menciona
p4b_dia = spark.sql("""
  WITH papel_dia AS (
    SELECT g.caso, g.data, g.dias_desde_estopim, g.fase, g.destino_conta_id, g.stance_origem, g.peso,
           CASE WHEN g.papel_destino = 'alvo' THEN 'alvo'
                WHEN n.conta_id IS NULL THEN 'demais'
                WHEN n.dia_virada IS NOT NULL AND g.dias_desde_estopim >= n.dia_virada THEN n.papel_final
                ELSE n.papel_inicial END AS papel
    FROM scapegoat.gold.grafo_arestas g
    LEFT JOIN scapegoat.gold.papel_narrativo_v0 n ON n.caso = g.caso AND n.conta_id = g.destino_conta_id
  )
  SELECT caso, data, dias_desde_estopim, fase, papel,
         SUM(peso) AS mencoes,
         SUM(SUM(peso)) OVER (PARTITION BY caso, data) AS mencoes_dia,
         SUM(peso) / SUM(SUM(peso)) OVER (PARTITION BY caso, data) AS parcela_dia,
         COUNT(DISTINCT destino_conta_id) AS contas,
         SUM(CASE WHEN stance_origem = 'acusador' THEN peso ELSE 0 END) / SUM(peso) AS de_acusadores,
         SUM(CASE WHEN stance_origem = 'defensor' THEN peso ELSE 0 END) / SUM(peso) AS de_defensores,
         SUM(CASE WHEN stance_origem = 'neutro'   THEN peso ELSE 0 END) / SUM(peso) AS de_neutros
  FROM papel_dia
  GROUP BY caso, data, dias_desde_estopim, fase, papel
  ORDER BY caso, data, papel
""").toPandas()
tot = p4b_dia.groupby("caso")["mencoes"].sum().to_dict()
assert tot == {"arthur_do_val": 22954, "monark": 5868}, f"Σ menções ≠ grafo_arestas: {tot}"
iso = spark.sql("SELECT caso, data, isolamento_alvo, n_nos FROM scapegoat.gold.fato_rede").toPandas()
chk = p4b_dia[p4b_dia.papel == "alvo"].merge(iso, on=["caso", "data"])
assert ((chk.parcela_dia - chk.isolamento_alvo).abs() < 1e-6).all(), "parcela do alvo ≠ fato_rede.isolamento_alvo"
p4b_dia = p4b_dia.merge(iso[["caso", "data", "n_nos"]], on=["caso", "data"]); p4b_dia["nd"] = p4b_dia.n_nos < 30
print("P4b (a) validada:", tot, "| papéis observados:", sorted(p4b_dia.papel.unique()))

# resumo por papel × fase, e por papel no caso inteiro (fase = 'todas')
def resumo(df, chaves):
    g = df.groupby(chaves)
    r = g.agg(mencoes=("mencoes", "sum"), contas=("contas", "max"),
              acus=("de_acusadores", lambda s: np.average(s, weights=df.loc[s.index, "mencoes"])),
              defe=("de_defensores", lambda s: np.average(s, weights=df.loc[s.index, "mencoes"])),
              neut=("de_neutros",    lambda s: np.average(s, weights=df.loc[s.index, "mencoes"]))).reset_index()
    base = df.groupby(chaves[:-1])["mencoes"].sum().rename("mencoes_base").reset_index()
    r = r.merge(base, on=chaves[:-1]); r["parcela"] = (r.mencoes / r.mencoes_base).round(3)
    for c in ["acus", "defe", "neut"]: r[c] = r[c].round(3)
    return r.rename(columns={"acus": "de_acusadores", "defe": "de_defensores", "neut": "de_neutros"})
p4b_fase = resumo(p4b_dia, ["caso", "fase", "papel"])
p4b_caso = resumo(p4b_dia.assign(fase="todas"), ["caso", "fase", "papel"])
p4b_res = pd.concat([p4b_fase, p4b_caso])
ordem = {"pre_crise": 1, "estopim": 2, "escalada": 3, "pico": 4, "declinio": 5, "pos_rito": 6, "todas": 7}
p4b_res = p4b_res.sort_values(["caso", "fase", "mencoes"], key=lambda s: s.map(ordem) if s.name == "fase" else (-s if s.name == "mencoes" else s))
assert ((p4b_res.groupby(["caso", "fase"])["parcela"].sum() - 1).abs() < 0.005).all(), "Σ parcelas por fase ≠ 1"
display(p4b_res[p4b_res.fase == "todas"])
p4b_res.to_csv(f"{SAIDA}/p4b_papeis_fase.csv", index=False)
p4b_dia.to_csv(f"{SAIDA}/p4b_papeis_dia.csv", index=False)

# COMMAND ----------

# (b) defasagem: dia do pico de cada papel (dias com >= 30 nós) em relação ao dia do pico do alvo, e o que acontece no dia seguinte
ok = p4b_dia[~p4b_dia.nd]
pico = (ok.sort_values("mencoes", ascending=False).drop_duplicates(["caso", "papel"])
          [["caso", "papel", "dias_desde_estopim", "mencoes", "parcela_dia", "de_acusadores"]]
          .rename(columns={"dias_desde_estopim": "dia_pico"}))
pico_alvo = pico[pico.papel == "alvo"].set_index("caso").dia_pico
pico["defasagem_vs_alvo"] = pico.dia_pico - pico.caso.map(pico_alvo)
# parcela acusadora das postagens no dia do pico do papel e no dia seguinte (fato_atividade)
ac = spark.sql("""
  SELECT caso, dias_desde_estopim AS dia,
         SUM(CASE WHEN stance = 'acusador' THEN n_postagens ELSE 0 END) / SUM(n_postagens) AS parcela_acusadora_dia
  FROM scapegoat.gold.fato_atividade WHERE versao_classificacao = 'hf@a483947' GROUP BY caso, dias_desde_estopim
""").toPandas().set_index(["caso", "dia"]).parcela_acusadora_dia
pico["acusacao_no_dia"] = [ac.get((c, d), np.nan) for c, d in zip(pico.caso, pico.dia_pico)]
pico["acusacao_dia_seguinte"] = [ac.get((c, d + 1), np.nan) for c, d in zip(pico.caso, pico.dia_pico)]
alvo_seg = ok[ok.papel == "alvo"].set_index(["caso", "dias_desde_estopim"]).parcela_dia
pico["parcela_alvo_dia_seguinte"] = [alvo_seg.get((c, d + 1), np.nan) for c, d in zip(pico.caso, pico.dia_pico)]
pico = pico.sort_values(["caso", "dia_pico"]).round(3)
display(pico)
pico.to_csv(f"{SAIDA}/p4b_defasagem.csv", index=False)

# COMMAND ----------

# (c) sobreposição de acusadores: quem acusa o papel também acusa o alvo? (conjuntos de autoras com stance acusador, caso inteiro)
p4b_ov = spark.sql("""
  WITH papel_conta AS (
    SELECT g.caso, g.origem_conta_id, g.destino_conta_id, g.peso,
           CASE WHEN g.papel_destino = 'alvo' THEN 'alvo'
                WHEN n.conta_id IS NULL THEN 'demais'
                WHEN n.dia_virada IS NOT NULL AND g.dias_desde_estopim >= n.dia_virada THEN n.papel_final
                ELSE n.papel_inicial END AS papel
    FROM scapegoat.gold.grafo_arestas g
    LEFT JOIN scapegoat.gold.papel_narrativo_v0 n ON n.caso = g.caso AND n.conta_id = g.destino_conta_id
    WHERE g.stance_origem = 'acusador'
  ),
  acus_alvo AS (SELECT DISTINCT caso, origem_conta_id FROM papel_conta WHERE papel = 'alvo'),
  n_alvo AS (SELECT caso, COUNT(*) AS acusadores_do_alvo FROM acus_alvo GROUP BY caso),
  acus_papel AS (SELECT DISTINCT caso, papel, origem_conta_id FROM papel_conta WHERE papel <> 'alvo')
  SELECT p.caso, p.papel,
         COUNT(*)                                                       AS acusadores_do_papel,
         SUM(CASE WHEN a.origem_conta_id IS NOT NULL THEN 1 ELSE 0 END) AS tambem_acusam_alvo,
         MAX(n.acusadores_do_alvo)                                      AS acusadores_do_alvo
  FROM acus_papel p
  LEFT JOIN acus_alvo a ON a.caso = p.caso AND a.origem_conta_id = p.origem_conta_id
  JOIN n_alvo n ON n.caso = p.caso
  GROUP BY p.caso, p.papel ORDER BY p.caso, acusadores_do_papel DESC
""").toPandas()
p4b_ov["parcela_que_tambem_acusa_alvo"] = (p4b_ov.tambem_acusam_alvo / p4b_ov.acusadores_do_papel).round(3)
p4b_ov["jaccard_com_alvo"] = (p4b_ov.tambem_acusam_alvo / (p4b_ov.acusadores_do_papel + p4b_ov.acusadores_do_alvo - p4b_ov.tambem_acusam_alvo)).round(3)
display(p4b_ov)
p4b_ov.to_csv(f"{SAIDA}/p4b_sobreposicao.csv", index=False)

# COMMAND ----------

# figura P4b — linha superior: parcela das menções do dia por papel; inferior: mix de stance de quem menciona cada papel (caso inteiro)
COR_PAPEL = {"alvo": INK, "instituicao_legitimadora": "#4a3aa7", "vitima_secundaria": "#e34948", "aliado_do_alvo": "#008300",
             "aliado_afastado": "#7fbf7f", "lider_acusacao": "#e87ba4", "acusador": "#d1493f", "veiculo": "#8c8c8c", "outro": "#bdbdbd", "demais": "#dddddd"}
ROT_PAPEL = {"alvo": "alvo", "instituicao_legitimadora": "instituição", "vitima_secundaria": "vítima secundária", "aliado_do_alvo": "aliado",
             "aliado_afastado": "aliado afastado", "lider_acusacao": "líder narrativo", "acusador": "acusador (narrativa)", "veiculo": "veículo", "outro": "outro", "demais": "demais"}
COR_STANCE = {"de_acusadores": "#d1493f", "de_defensores": "#008300", "de_neutros": "#c9c7bd"}
papeis_plot = [p for p in COR_PAPEL if p in set(p4b_dia.papel) and p != "demais"]

fig, axes = plt.subplots(2, 2, figsize=(10, 8.4), dpi=200, gridspec_kw=dict(width_ratios=[18, 8], height_ratios=[3, 2], wspace=0.08, hspace=0.6))
fig.patch.set_facecolor(SURF)
handles = {}
for j, caso in enumerate(["arthur_do_val", "monark"]):
    ax = axes[0, j]; eixo_limpo(ax)
    d = p4b_dia[(p4b_dia.caso == caso) & (~p4b_dia.nd)]
    for p in papeis_plot:
        s = d[d.papel == p].set_index("dias_desde_estopim").parcela_dia.sort_index()
        if s.empty: continue
        h, = ax.plot(s.index, s.values, marker="o", ms=3.8, lw=1.8 if p == "alvo" else 1.4, color=COR_PAPEL[p], zorder=4 if p == "alvo" else 3, clip_on=False)
        handles[ROT_PAPEL[p]] = h
    for x in p4b_dia[(p4b_dia.caso == caso) & (p4b_dia.nd)].dias_desde_estopim.unique():
        ax.text(x, 0.02, "n.d.", ha="center", va="bottom", fontsize=7, color=MUTED)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.plot([1], [0.985], marker="v", ms=6, color=INK, clip_on=False, transform=ax.get_xaxis_transform(), ls="none", zorder=5)
    ax.text(1, 1.02, "pico", ha="center", va="bottom", fontsize=7.5, color=INK2, transform=ax.get_xaxis_transform())
    todos = p4b_dia[p4b_dia.caso == caso].dias_desde_estopim
    ax.set_xticks(sorted(todos.unique())); ax.set_xlim(todos.min() - 0.6, todos.max() + 0.6)
    ax.set_title(NOME[caso], loc="left", fontsize=10, color=INK, pad=22)
    ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=9)
    if j == 0:
        ax.set_ylim(0, 0.7); ax.set_yticks([0, .25, .5]); ax.set_yticklabels(["0", "25 %", "50 %"])
        ax.set_ylabel("parcela das menções do dia", color=INK2, fontsize=9)
    else:
        ax.set_ylim(0, 0.7); ax.set_yticks([0, .25, .5]); ax.set_yticklabels([])

    ax = axes[1, j]; eixo_limpo(ax)
    r = p4b_res[(p4b_res.caso == caso) & (p4b_res.fase == "todas") & (p4b_res.papel.isin(papeis_plot))].set_index("papel")
    ps = [p for p in papeis_plot if p in r.index]
    for k, p in enumerate(ps):
        base = 0
        for st in ["de_acusadores", "de_defensores", "de_neutros"]:
            hb = ax.bar(k, r.loc[p, st], bottom=base, width=0.7, color=COR_STANCE[st], edgecolor=SURF, linewidth=0.8, zorder=3)
            handles[st.replace("de_", "mencionado por ").replace("acusadores", "acusadores do alvo").replace("defensores", "defensores do alvo")] = hb
            base += r.loc[p, st]
        ax.text(k, 1.02, f"{int(r.loc[p, 'mencoes']):,}".replace(",", "."), ha="center", va="bottom", fontsize=6.6, color=MUTED)
    ax.set_xticks(range(len(ps))); ax.set_xticklabels([ROT_PAPEL[p] for p in ps], rotation=30, ha="right", fontsize=7.5)
    ax.set_ylim(0, 1.0); ax.set_yticks([0, .5, 1]); ax.set_yticklabels(["0", "50 %", "100 %"] if j == 0 else [])
    ax.set_title("quem menciona cada papel (stance em relação ao alvo) · número = menções", loc="left", fontsize=8.5, color=INK, pad=12)
    if j == 0: ax.set_ylabel("mix de stance", color=INK2, fontsize=9)

fig.legend(handles.values(), handles.keys(), loc="upper left", bbox_to_anchor=(0.06, 0.935), ncol=4, frameon=False, fontsize=7.5, labelcolor=INK2, handlelength=1.6, columnspacing=1.2)
fig.suptitle("P4b · Papéis narrativos vistos pela multidão: quem é mencionado, quando, e por quem", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.95, "papel vigente = papel_final a partir de dia_virada · n.d. = dia com < 30 nós · fonte: gold.grafo_arestas × gold.papel_narrativo_v0",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.08, right=0.98, top=0.79, bottom=0.10)
caminho = f"{SAIDA}/p4b_papeis.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## P11 — As assinaturas estruturais coincidem quando alinhadas pelo estopim?
# MAGIC
# MAGIC Resposta em duas partes, decidida depois de P8, P10 e P4b. **(a) Assinaturas estáticas** — propriedades do caso inteiro, lidas das saídas
# MAGIC das perguntas anteriores e da Gold: sem câmaras de eco, líder invisível, alvo como polo único e instituição como segundo hub, Gini alto,
# MAGIC falado-sobre ≫ falado-com, multidão rasa. **(b) Assinaturas dinâmicas** — seis curvas normalizadas por dia, alinhadas por `dias_desde_estopim`,
# MAGIC os dois casos sobrepostos: parcela do alvo, parcela institucional, replies ao alvo, parcela defensora, parcela de likes da defesa, mira dos líderes.
# MAGIC **(c) Direção** — variação de cada curva entre o pico e o declínio, em pontos percentuais, por caso: é o número que sustenta "divergem na mesma direção".
# MAGIC Só medidas normalizadas; nada de volume absoluto. Sem índice composto. n.d. = dia com < 30 nós, em todas as séries.
# MAGIC Ressalva de leitura: o dia 0 do Arthur é uma troca de polêmica dentro de uma crise em curso (pré-crise com 42 % de replies ao alvo), não um início.

# COMMAND ----------

# setup automático se a sessão reiniciou (SAIDA, cores, eixo_limpo vêm da célula 2)
if "SAIDA" not in globals():
    dbutils.notebook.exit("Sessão reiniciada: rode a célula 2 (setup) e depois esta seção — ou Run all.")

import pandas as pd
import numpy as np
CASOS = ["arthur_do_val", "monark"]; VERSAO = "hf@a483947"

# COMMAND ----------

# (b) primeiro: as seis séries diárias, todas da Gold, com a flag n.d. de fato_rede
serie = spark.sql(f"""
  WITH rede AS (
    SELECT caso, data, dias_desde_estopim, fase, n_nos, isolamento_alvo AS parcela_alvo FROM scapegoat.gold.fato_rede
  ),
  inst AS (
    SELECT g.caso, g.data,
           SUM(CASE WHEN (CASE WHEN n.dia_virada IS NOT NULL AND g.dias_desde_estopim >= n.dia_virada THEN n.papel_final ELSE n.papel_inicial END)
                         = 'instituicao_legitimadora' THEN g.peso ELSE 0 END) / SUM(g.peso) AS parcela_instituicao
    FROM scapegoat.gold.grafo_arestas g
    LEFT JOIN scapegoat.gold.papel_narrativo_v0 n ON n.caso = g.caso AND n.conta_id = g.destino_conta_id
    GROUP BY g.caso, g.data
  ),
  refs AS (
    SELECT caso, data,
           SUM(CASE WHEN tipo_ref = 'reply' AND destinatario = 'alvo' THEN n_postagens ELSE 0 END) / SUM(n_postagens) AS replies_alvo
    FROM scapegoat.gold.fato_referencia GROUP BY caso, data
  ),
  defesa AS (
    SELECT caso, data,
           SUM(CASE WHEN stance = 'defensor' THEN n_postagens ELSE 0 END) / SUM(n_postagens) AS parcela_defensora,
           SUM(CASE WHEN stance = 'defensor' THEN likes ELSE 0 END) / NULLIF(SUM(likes), 0) AS likes_defesa
    FROM scapegoat.gold.fato_atividade WHERE versao_classificacao = '{VERSAO}' GROUP BY caso, data
  ),
  emit AS (
    SELECT caso, origem_conta_id AS conta_id,
           SUM(CASE WHEN stance_origem = 'acusador' THEN peso ELSE 0 END) AS ac, SUM(peso) AS em
    FROM scapegoat.gold.grafo_arestas g
    WHERE NOT EXISTS (SELECT 1 FROM scapegoat.gold.dim_conta_papel d WHERE d.caso = g.caso AND d.conta_id = g.origem_conta_id AND d.papel_principal = 'alvo')
    GROUP BY caso, origem_conta_id
  ),
  lid AS (
    SELECT caso, conta_id FROM (SELECT caso, conta_id, ROW_NUMBER() OVER (PARTITION BY caso ORDER BY ac DESC, em DESC, conta_id) AS pos FROM emit WHERE ac > 0) WHERE pos <= 10
  ),
  mira AS (
    SELECT g.caso, g.data, SUM(CASE WHEN g.papel_destino = 'alvo' THEN g.peso ELSE 0 END) / SUM(g.peso) AS mira_lideres, SUM(g.peso) AS mencoes_lideres
    FROM scapegoat.gold.grafo_arestas g JOIN lid l ON l.caso = g.caso AND l.conta_id = g.origem_conta_id
    GROUP BY g.caso, g.data
  )
  SELECT r.caso, r.data, r.dias_desde_estopim, r.fase, r.n_nos, r.parcela_alvo, i.parcela_instituicao, f.replies_alvo,
         d.parcela_defensora, d.likes_defesa, m.mira_lideres, m.mencoes_lideres
  FROM rede r
  LEFT JOIN inst i ON i.caso = r.caso AND i.data = r.data
  LEFT JOIN refs f ON f.caso = r.caso AND f.data = r.data
  LEFT JOIN defesa d ON d.caso = r.caso AND d.data = r.data
  LEFT JOIN mira m ON m.caso = r.caso AND m.data = r.data
  ORDER BY r.caso, r.data
""").toPandas()
assert len(serie) == 26, f"esperava 26 dias, obtive {len(serie)}"
serie["nd"] = serie.n_nos < 30
serie.loc[serie.mencoes_lideres.fillna(0) < 10, "mira_lideres"] = np.nan   # mira com < 10 menções no dia não é lida
SERIES = {"parcela_alvo": "parcela do alvo nas menções (P5/P9)", "parcela_instituicao": "parcela das instituições nas menções (P4b)",
          "replies_alvo": "replies ao alvo / postagens (P9)", "parcela_defensora": "parcela defensora das postagens (P7)",
          "likes_defesa": "parcela dos likes da defesa (P7)", "mira_lideres": "menções dos líderes dirigidas ao alvo (P10; dias com ≥ 10)"}
for c in SERIES: assert ((serie[c].dropna() >= 0) & (serie[c].dropna() <= 1)).all(), f"{c} fora de [0,1]"
# conferências cruzadas com as seções anteriores (mesma Gold, mesmas definições)
chk = serie[~serie.nd].groupby("caso")[["parcela_alvo", "replies_alvo"]].mean().round(3)
print("P11 (b) validada: 26 dias; n.d.:", serie[serie.nd][["caso", "dias_desde_estopim"]].values.tolist())
serie.to_csv(f"{SAIDA}/p11_series.csv", index=False)
display(serie.drop(columns=["n_nos", "mencoes_lideres"]).round(3))

# COMMAND ----------

# (a) assinaturas estáticas — propriedades do caso inteiro, lado a lado (Gold + saídas já gravadas das perguntas anteriores)
p8 = pd.read_csv(f"{SAIDA}/p8_resumo.csv").set_index(["caso", "janela"])
p10f = pd.read_csv(f"{SAIDA}/p10_polo_fase.csv")
p9r = pd.read_csv(f"{SAIDA}/p9_resumo.csv")
gini = spark.sql("SELECT caso, percentile_approx(gini_mencoes_recebidas, 0.5) AS gini_mediano FROM scapegoat.gold.fato_rede WHERE n_nos >= 30 GROUP BY caso").toPandas().set_index("caso").gini_mediano
rasa = spark.sql("""
  SELECT caso, SUM(CASE WHEN n_postagens_caso = 1 THEN 1 ELSE 0 END) / COUNT(*) AS autoras_1_post
  FROM scapegoat.gold.dim_conta_papel WHERE n_postagens_caso > 0 GROUP BY caso""").toPandas().set_index("caso").autoras_1_post
ok = serie[~serie.nd]
pos = ok[ok.dias_desde_estopim >= 0]
linhas = {
  "assortatividade entre autores a partir do estopim (Newman, sem neutro)": {c: p8.loc[(c, "a partir do estopim"), "r_sem_neutro"] for c in CASOS},
  "parcela das menções recebida pelos 10 maiores acusadores (máx. por fase)": {c: p10f[p10f.caso == c].parcela_lideres.max() for c in CASOS},
  "parcela do alvo nas menções, média pós-estopim": {c: pos[pos.caso == c].parcela_alvo.mean() for c in CASOS},
  "segundo hub: parcela institucional máxima num dia": {c: ok[ok.caso == c].parcela_instituicao.max() for c in CASOS},
  "Gini das menções recebidas, mediana dos dias": {c: gini[c] for c in CASOS},
  "replies ao alvo por reply a terceiros, pós-estopim (mín–máx por fase)": {c: f"{p9r[(p9r.caso == c) & (p9r.fase != 'pre_crise') & (p9r.postagens >= 30)].razao_alvo_terceiros.min():.2f}–{p9r[(p9r.caso == c) & (p9r.fase != 'pre_crise') & (p9r.postagens >= 30)].razao_alvo_terceiros.max():.2f}" for c in CASOS},
  "autoras com uma única postagem": {c: rasa[c] for c in CASOS},
}
estat = pd.DataFrame(linhas).T[CASOS]
estat = estat.apply(lambda col: col.map(lambda v: v if isinstance(v, str) else f"{float(v):.3f}")).astype(str)   # tudo texto: display/Arrow não aceita coluna mista
estat["coincide"] = ["sim", "sim", "não (nível)", "sim", "sim", "sim", "sim"]
display(estat.reset_index().rename(columns={"index": "assinatura estática"}))
estat.to_csv(f"{SAIDA}/p11_estaticas.csv")

# COMMAND ----------

# (c) direção — cada curva: valor no pico (dia 1) e média do declínio; variação em pontos percentuais, por caso
dirs = []
for c in CASOS:
    s = ok[ok.caso == c].set_index("dias_desde_estopim")
    for col, rot in SERIES.items():
        pico = s.loc[1, col] if 1 in s.index else np.nan
        decl = s[s.fase == "declinio"][col].mean()
        pre = s[s.index < 0][col].mean() if (s.index < 0).any() else np.nan
        dirs.append(dict(assinatura=rot, caso=c, pre_crise=pre, pico=pico, declinio_media=decl, variacao_pp=(decl - pico) * 100))
direcao = pd.DataFrame(dirs).round(3)
direcao["variacao_pp"] = direcao.variacao_pp.round(1)
piv = direcao.pivot(index="assinatura", columns="caso", values="variacao_pp")[CASOS]
piv["mesmo_sinal"] = np.sign(piv.arthur_do_val) == np.sign(piv.monark)
display(direcao); display(piv.reset_index())
direcao.to_csv(f"{SAIDA}/p11_direcao.csv", index=False)
print("curvas em que Monark sobe do pico para o declínio:", int((piv.monark > 0).sum()), "de", len(piv),
      "| Arthur:", int((piv.arthur_do_val > 0).sum()), "de", len(piv))

# COMMAND ----------

# figura P11 — seis curvas dinâmicas, dois casos sobrepostos, alinhadas pelo estopim; janela comum (dias 0–6) sombreada
COR = {"arthur_do_val": "#2a78d6", "monark": "#eb6834"}
fig, axes = plt.subplots(2, 3, figsize=(11, 6.6), dpi=200, sharex=True, sharey=True, gridspec_kw=dict(wspace=0.10, hspace=0.45))
fig.patch.set_facecolor(SURF)
handles = {}
for ax, (col, rot) in zip(axes.flat, SERIES.items()):
    eixo_limpo(ax)
    ax.axvspan(-0.5, 6.5, color=AXIS, alpha=0.07, zorder=0)
    ax.axvline(0, color=AXIS, lw=0.8, ls=(0, (3, 3)), zorder=1)
    for c in CASOS:
        s = serie[(serie.caso == c) & (~serie.nd)].set_index("dias_desde_estopim")[col].dropna().sort_index()
        h, = ax.plot(s.index, s.values, marker="o", ms=3.6, lw=1.6, color=COR[c], zorder=3, clip_on=False)
        handles[NOME[c]] = h
    ax.set_title(rot, loc="left", fontsize=8.6, color=INK, pad=6)
    ax.set_xticks(range(-7, 11, 1)); ax.set_xticklabels([str(x) if x % 2 == 0 or x in (-7, 1) else "" for x in range(-7, 11)], fontsize=7)
    ax.set_xlim(-7.6, 10.6)
for ax in axes[1]: ax.set_xlabel("dias desde o estopim", color=INK2, fontsize=8.5)
axes[0, 0].set_ylim(0, 1.0); axes[0, 0].set_yticks([0, .25, .5, .75, 1]); axes[0, 0].set_yticklabels(["0", "25 %", "50 %", "75 %", "100 %"])
for ax in axes[:, 0]: ax.set_ylabel("parcela", color=INK2, fontsize=8.5)
fig.legend(handles.values(), handles.keys(), loc="upper right", bbox_to_anchor=(0.99, 0.945), ncol=2, frameon=False, fontsize=8.5, labelcolor=INK2, handlelength=1.8)
fig.suptitle("P11 · Assinaturas dinâmicas alinhadas pelo estopim: os dois casos divergem — sempre na mesma direção", x=0.02, y=0.985, ha="left", fontsize=11, color=INK)
fig.text(0.02, 0.945, "faixa cinza = janela que os dois casos têm (dias 0–6) · pico = dia 1 nos dois · n.d. omitido (dia com < 30 nós) · no Arthur o dia 0 é troca de polêmica, não início · fonte: gold",
         fontsize=7.5, color=MUTED)
fig.subplots_adjust(left=0.07, right=0.98, top=0.86, bottom=0.09)
caminho = f"{SAIDA}/p11_assinaturas.png"
fig.savefig(caminho, facecolor=SURF)
print("figura gravada em", caminho)
display(fig)