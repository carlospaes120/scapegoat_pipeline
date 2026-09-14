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
  FROM gold.calendario_caso
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
  FROM gold.calendario_caso
  GROUP BY caso
  ORDER BY caso
""")
display(p1_resumo)

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

p1_resumo.toPandas().to_csv(f"{SAIDA}/p1_resumo.csv", index=False)

# COMMAND ----------



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
    FROM gold.calendario_caso
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
    FROM gold.calendario_caso
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