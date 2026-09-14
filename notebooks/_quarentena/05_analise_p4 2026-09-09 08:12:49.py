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
