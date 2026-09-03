# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC Lê os arquivos brutos do Volume, calcula o hash, registra a proveniência e
# MAGIC grava **um registro por postagem, exatamente como a fonte entregou**.
# MAGIC
# MAGIC Nenhum campo é extraído aqui. Todo o parse acontece no notebook seguinte
# MAGIC (`02_silver_qc`), em SQL, à vista — é o que os critérios de Carga e de
# MAGIC Qualidade de Dados avaliam.
# MAGIC
# MAGIC ### Duas fontes de ingestão, uma camada Bronze
# MAGIC
# MAGIC | fonte | caso | formato | o que é |
# MAGIC |---|---|---|---|
# MAGIC | `graphql` | `arthur_do_val` | JSON, um arquivo por dia | payload original da plataforma, com `metadata.query` |
# MAGIC | `consolidado` | `monark` | JSONL, uma linha por postagem | extrato consolidado de coleta anterior a este MVP |
# MAGIC
# MAGIC Nada a jusante da Bronze sabe de qual fonte o registro veio. É a fronteira
# MAGIC que permite acrescentar um coletor novo sem tocar no resto do pipeline.
# MAGIC
# MAGIC ### Idempotência
# MAGIC
# MAGIC Um arquivo já ingerido, identificado pelo seu SHA-256, é ignorado. Rodar o
# MAGIC notebook duas vezes não duplica nada — requisito para o pipeline poder ser
# MAGIC reexecutado sem medo.

# COMMAND ----------

# MAGIC %md ## Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalogo", "scapegoat", "Catálogo")
dbutils.widgets.dropdown("caso", "arthur_do_val", ["arthur_do_val", "monark"], "Caso")
dbutils.widgets.text("versao_pipeline", "v0.1.0", "Versão do pipeline (tag git)")

CATALOGO = dbutils.widgets.get("catalogo")
CASO     = dbutils.widgets.get("caso")
VERSAO   = dbutils.widgets.get("versao_pipeline")

# Cada caso tem uma fonte e um caminho no Volume.
CONFIG = {
    "arthur_do_val": {
        "fonte":   "graphql",
        "caminho": f"/Volumes/{CATALOGO}/bronze/raw/caso=arthur_do_val/coleta=2026-02",
        "formato": "json",
    },
    "monark": {
        "fonte":   "consolidado",
        "caminho": f"/Volumes/{CATALOGO}/bronze/raw/caso=monark/coleta=2025-10",
        "formato": "jsonl",
        # A consulta desta coleta não é recuperável — ver §7.3 do documento de
        # objetivo. Fica NULL, declarado, em vez de inventado.
        "consulta": None,
        "periodo": ("2022-02-07", "2022-02-15"),
    },
}

cfg = CONFIG[CASO]
spark.sql(f"USE CATALOG {CATALOGO}")
print(f"caso={CASO}  fonte={cfg['fonte']}  versao={VERSAO}\ncaminho={cfg['caminho']}")

# COMMAND ----------

# MAGIC %md ## Inventário do Volume
# MAGIC
# MAGIC Regra anti-swamp: nenhum objeto entra na camada Bronze sem uma linha em
# MAGIC `bronze.arquivo`.

# COMMAND ----------

import hashlib, json, os, datetime

def sha256_de(caminho, blocos=1024 * 1024):
    """Hash do arquivo como está no Volume. É o que torna a linhagem verificável."""
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(blocos), b""):
            h.update(bloco)
    return h.hexdigest()

arquivos = sorted(
    os.path.join(cfg["caminho"], nome)
    for nome in os.listdir(cfg["caminho"])
    if nome.lower().endswith((".json", ".jsonl"))
)

if not arquivos:
    raise SystemExit(f"Nenhum arquivo em {cfg['caminho']} — suba os brutos antes de rodar.")

inventario = [
    {"uri": p, "nome": os.path.basename(p), "bytes": os.path.getsize(p), "sha256": sha256_de(p)}
    for p in arquivos
]

print(f"{len(inventario)} arquivos, {sum(i['bytes'] for i in inventario)/1e6:.1f} MB")
for i in inventario[:3]:
    print(f"   {i['nome']:24} {i['bytes']:>10,} bytes  {i['sha256'][:16]}…")
if len(inventario) > 3:
    print(f"   … mais {len(inventario)-3}")

# COMMAND ----------

# MAGIC %md ## Já ingeridos?
# MAGIC
# MAGIC Comparação por hash, não por nome: um arquivo renomeado com o mesmo
# MAGIC conteúdo não é ingerido de novo, e um arquivo alterado com o mesmo nome é.

# COMMAND ----------

ja = {r.sha256 for r in spark.sql("SELECT sha256 FROM bronze.arquivo").collect()}
novos = [i for i in inventario if i["sha256"] not in ja]

print(f"{len(novos)} arquivos novos, {len(inventario)-len(novos)} já ingeridos (ignorados)")
if not novos:
    dbutils.notebook.exit("Nada novo a ingerir.")

# COMMAND ----------

# MAGIC %md ## Leitura dos registros
# MAGIC
# MAGIC O payload de cada postagem é gravado **sem seleção de campos e sem
# MAGIC renomeação**. Nada é descartado: o que a fonte entregou é o que fica.
# MAGIC
# MAGIC O formato `graphql` traz, além das postagens, um bloco `metadata` com a
# MAGIC **consulta literal da coleta** e o período — é dali que sai o
# MAGIC `COLETA.consulta`, o campo que sustenta a declaração de viés e que se
# MAGIC perdeu no caso `monark`.

# COMMAND ----------

def ler_graphql(caminho):
    """Retorna (metadados_da_coleta, [payload_json_por_postagem])."""
    with open(caminho, encoding="utf-8") as f:
        bloco = json.load(f)
    md = bloco.get("metadata", {}) or {}
    consulta = (md.get("query") or "").split(" since:")[0].strip() or None
    registros = [
        json.dumps(t, ensure_ascii=False, separators=(",", ":"))
        for t in bloco.get("tweets", [])
    ]
    return {"consulta": consulta,
            "periodo_inicio": md.get("since"),
            "periodo_fim": md.get("until")}, registros

def ler_jsonl(caminho):
    """Uma linha do arquivo = um registro. Fidelidade byte a byte."""
    with open(caminho, encoding="utf-8") as f:
        registros = [linha.strip() for linha in f if linha.strip()]
    ini, fim = cfg.get("periodo", (None, None))
    return {"consulta": cfg.get("consulta"),
            "periodo_inicio": ini, "periodo_fim": fim}, registros

leitor = ler_graphql if cfg["fonte"] == "graphql" else ler_jsonl

# COMMAND ----------

# MAGIC %md ## Gravação
# MAGIC
# MAGIC Um arquivo por vez: registra a proveniência, recupera o `arquivo_id` que a
# MAGIC identidade gerou e grava os registros vinculados a ele. Se a gravação dos
# MAGIC registros falhar, a linha do arquivo é removida — para não ficar um objeto
# MAGIC catalogado sem conteúdo.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

total_registros = 0
consultas_vistas = set()

for item in novos:
    meta, registros = leitor(item["uri"])
    if not registros:
        print(f"  ⚠ {item['nome']}: nenhum registro — arquivo ignorado")
        continue

    spark.sql(
        """
        INSERT INTO bronze.arquivo
          (caso_slug, fonte, uri, formato, sha256, bytes, consulta,
           periodo_inicio, periodo_fim, ingerido_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), current_timestamp())
        """,
        args=[CASO, cfg["fonte"], item["uri"], cfg["formato"], item["sha256"],
              item["bytes"], meta["consulta"],
              meta["periodo_inicio"], meta["periodo_fim"]],
    )

    arquivo_id = spark.sql(
        "SELECT arquivo_id FROM bronze.arquivo WHERE sha256 = ?", args=[item["sha256"]]
    ).collect()[0][0]

    try:
        (spark.createDataFrame(
            [Row(arquivo_id=int(arquivo_id), linha=i, payload=p)
             for i, p in enumerate(registros, start=1)])
         .withColumn("ingerido_em", current_timestamp())
         .write.mode("append").saveAsTable("bronze.registro"))
    except Exception:
        spark.sql("DELETE FROM bronze.arquivo WHERE arquivo_id = ?", args=[int(arquivo_id)])
        raise

    total_registros += len(registros)
    if meta["consulta"]:
        consultas_vistas.add(meta["consulta"])
    print(f"  ✓ {item['nome']:24} {len(registros):>6} registros")

print(f"\n{len(novos)} arquivos, {total_registros} registros gravados em bronze.registro")
if consultas_vistas:
    print("\nConsulta(s) de coleta recuperadas do próprio bruto:")
    for c in consultas_vistas:
        print(f"   {c}")
else:
    print("\nSem consulta registrada nesta fonte — COLETA.consulta fica NULL, declarado.")

# COMMAND ----------

# MAGIC %md ## Conferência
# MAGIC
# MAGIC Os números têm de bater com `tests/valores_esperados_qc.md`, que foi gerado
# MAGIC simulando a lógica do pipeline contra os arquivos originais.
# MAGIC
# MAGIC | caso | arquivos | registros esperados |
# MAGIC |---|---:|---:|
# MAGIC | `arthur_do_val` | 18 | **13.906** |
# MAGIC | `monark` | 1 | **5.143** |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.caso_slug,
# MAGIC   a.fonte,
# MAGIC   count(DISTINCT a.arquivo_id)          AS arquivos,
# MAGIC   count(r.linha)                        AS registros,
# MAGIC   round(sum(DISTINCT a.bytes)/1e6, 1)   AS mb,
# MAGIC   count(DISTINCT a.consulta)            AS consultas_distintas,
# MAGIC   min(a.periodo_inicio)                 AS de,
# MAGIC   max(a.periodo_fim)                    AS ate
# MAGIC FROM bronze.arquivo a
# MAGIC LEFT JOIN bronze.registro r USING (arquivo_id)
# MAGIC GROUP BY a.caso_slug, a.fonte
# MAGIC ORDER BY a.caso_slug;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência
# MAGIC
# MAGIC Tire um print do resultado acima para `evidencias/`. Ele mostra, numa
# MAGIC imagem só: as duas fontes coexistindo na mesma camada, a contagem de
# MAGIC registros, o volume ingerido e a consulta de coleta preservada — que é o
# MAGIC critério de Coleta inteiro.
# MAGIC
# MAGIC **Nenhum print desta etapa pode mostrar a coluna `payload`** — ela contém
# MAGIC texto de postagem e handles. Vale a regra C1: evidência só de agregados.
