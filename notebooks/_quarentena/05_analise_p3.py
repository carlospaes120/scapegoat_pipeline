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
