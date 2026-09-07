# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Classificacao de stance com BERTimbau (`silver.classificacao`)
# MAGIC
# MAGIC 1. Carrega o modelo publicado `carlospaes120/bertimbau-base-stance` (mesma chamada validada em `99_teste_classificador`).
# MAGIC 2. Reproduz a acuracia no conjunto de teste do gabarito (157) - prova de que e o mesmo modelo.
# MAGIC 3. Classifica as postagens da Silver a partir de `texto_limpo`, em lotes.
# MAGIC 4. Grava em `silver.classificacao` com modelo, versao (commit do Hugging Face) e confianca.
# MAGIC 5. Distribuicao dos rotulos por caso contra a do gabarito; indicadores em `silver.qc_resultado`.

# COMMAND ----------

# MAGIC %pip install -q transformers torch huggingface_hub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transformers import pipeline
from huggingface_hub import model_info

dbutils.widgets.text("versao_pipeline", "v0.2.0-dev", "Versao do pipeline")
VERSAO_PIPELINE = dbutils.widgets.get("versao_pipeline")

spark.sql("USE CATALOG scapegoat")
spark.sql("USE SCHEMA silver")

REPO       = "carlospaes120/bertimbau-base-stance"
MODELO     = "bertimbau-stance"
MAX_LENGTH = 128       # o que reproduziu o card em 99_teste_classificador
BATCH      = 32
CLASSES    = ["acusador", "defensor", "neutro"]
GABARITO   = "/Volumes/scapegoat/bronze/raw/gabarito_stance"

sha = model_info(REPO).sha
VERSAO_MODELO = f"hf@{sha[:7]}"
print("modelo:", REPO, " commit:", sha, " -> versao gravada:", VERSAO_MODELO)

clf = pipeline("text-classification", model=REPO,
               truncation=True, max_length=MAX_LENGTH, batch_size=BATCH, top_k=None)
print("dispositivo:", clf.device)

def classificar(textos):
    """Devolve lista de (rotulo, confianca) - confianca = probabilidade da classe escolhida."""
    saida = clf(textos)
    res = []
    for scores in saida:
        melhor = max(scores, key=lambda s: s["score"])
        res.append((melhor["label"], float(melhor["score"])))
    return res

# COMMAND ----------

# MAGIC %md ## 2. Prova de identidade: acuracia no teste do gabarito (esperado ~0,732)

# COMMAND ----------

teste = spark.read.json(f"{GABARITO}/merged_test.jsonl").select("clean_text", "stance").toPandas()
prev = [r for r, _ in classificar(teste["clean_text"].fillna("").astype(str).tolist())]
verdade = teste["stance"].tolist()
acc_teste = sum(a == b for a, b in zip(prev, verdade)) / len(verdade)
print(f"acuracia no teste: {acc_teste:.3f}   (card: 0.732)")
for c in CLASSES:
    tp = sum(1 for t, p in zip(verdade, prev) if t == c and p == c)
    fp = sum(1 for t, p in zip(verdade, prev) if t != c and p == c)
    fn = sum(1 for t, p in zip(verdade, prev) if t == c and p != c)
    pr = tp / (tp + fp) if tp + fp else 0.0
    rc = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * pr * rc / (pr + rc) if pr + rc else 0.0
    print(f"  F1 {c:>9}: {f1:.3f}")

# COMMAND ----------

# MAGIC %md ## 3. O que falta classificar
# MAGIC Reclassificar nao sobrescreve: cada (modelo, versao) e um conjunto proprio. So entram postagens sem rotulo desta versao.

# COMMAND ----------

pend = spark.sql(f"""
  SELECT p.postagem_id, p.caso_slug, p.texto_limpo
  FROM silver.postagem p
  WHERE p.texto_limpo IS NOT NULL AND trim(p.texto_limpo) <> ''
    AND NOT EXISTS (
      SELECT 1 FROM silver.classificacao k
      WHERE k.postagem_id = p.postagem_id
        AND k.modelo = '{MODELO}' AND k.versao = '{VERSAO_MODELO}')
""")
display(pend.groupBy("caso_slug").count())

vazias = spark.sql("SELECT caso_slug, count(*) AS n FROM silver.postagem WHERE texto_limpo IS NULL OR trim(texto_limpo) = '' GROUP BY caso_slug")
print("postagens com texto_limpo vazio (ficam sem rotulo):")
display(vazias)

pdf = pend.toPandas()
print("a classificar:", len(pdf))

# COMMAND ----------

# MAGIC %md ## 4. Inferencia em lotes e gravacao

# COMMAND ----------

LOTE = 512
rotulos, confs = [], []
t0 = time.time()
textos = pdf["texto_limpo"].astype(str).tolist()
for i in range(0, len(textos), LOTE):
    res = classificar(textos[i:i + LOTE])
    rotulos.extend(r for r, _ in res)
    confs.extend(c for _, c in res)
    feitos = min(i + LOTE, len(textos))
    print(f"  {feitos}/{len(textos)}  ({time.time() - t0:.0f}s)")

pdf["rotulo"] = rotulos
pdf["confianca"] = confs
assert set(pdf["rotulo"]).issubset(CLASSES), set(pdf["rotulo"])

spark.createDataFrame(pdf[["postagem_id", "rotulo", "confianca"]]).createOrReplaceTempView("novos_rotulos")

spark.sql(f"""
  INSERT INTO silver.classificacao
    (postagem_id, esquema, rotulo, modelo, versao, confianca, classificado_em)
  SELECT postagem_id, 'stance', rotulo, '{MODELO}', '{VERSAO_MODELO}', confianca, current_timestamp()
  FROM novos_rotulos
""")
print("gravado em silver.classificacao:", spark.sql(f"SELECT count(*) FROM silver.classificacao WHERE modelo='{MODELO}' AND versao='{VERSAO_MODELO}'").collect()[0][0])

# COMMAND ----------

# MAGIC %md ## 5. Distribuicao dos rotulos por caso, contra o gabarito

# COMMAND ----------

dist = spark.sql(f"""
  SELECT p.caso_slug, k.rotulo, count(*) AS n,
         round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY p.caso_slug), 1) AS pct,
         round(avg(k.confianca), 3) AS confianca_media
  FROM silver.classificacao k JOIN silver.postagem p ON p.postagem_id = k.postagem_id
  WHERE k.modelo = '{MODELO}' AND k.versao = '{VERSAO_MODELO}'
  GROUP BY p.caso_slug, k.rotulo ORDER BY p.caso_slug, k.rotulo
""")
display(dist)

gab = spark.read.json(f"{GABARITO}/merged_train.jsonl").unionByName(
      spark.read.json(f"{GABARITO}/merged_val.jsonl")).unionByName(
      spark.read.json(f"{GABARITO}/merged_test.jsonl"))
print("distribuicao no gabarito (rotulagem manual), por caso:")
display(gab.groupBy("case", "stance").count()
           .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy("case")), 1))
           .orderBy("case", "stance"))

# COMMAND ----------

# MAGIC %md ## 6. Indicadores em `silver.qc_resultado`

# COMMAND ----------

spark.sql(f"""
  DELETE FROM silver.qc_resultado
  WHERE indicador IN ('qc_classificacao_cobertura', 'qc_classificador_acuracia_teste', 'qc_classificacao_pct_neutro')
    AND versao_pipeline = '{VERSAO_PIPELINE}'
""")

spark.sql(f"""
  INSERT INTO silver.qc_resultado
    (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline, verificado_em)
  SELECT p.caso_slug, 'qc_classificacao_cobertura',
         CAST(100.0 * count(k.classificacao_id) / count(*) AS DOUBLE), 'informa', true,
         concat('postagens com rotulo de stance ({MODELO} {VERSAO_MODELO}); sem rotulo = texto_limpo vazio'),
         '{VERSAO_PIPELINE}', current_timestamp()
  FROM silver.postagem p
  LEFT JOIN silver.classificacao k
         ON k.postagem_id = p.postagem_id AND k.modelo = '{MODELO}' AND k.versao = '{VERSAO_MODELO}'
  GROUP BY p.caso_slug
""")

spark.sql(f"""
  INSERT INTO silver.qc_resultado
    (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline, verificado_em)
  SELECT p.caso_slug, 'qc_classificacao_pct_neutro',
         CAST(100.0 * sum(CASE WHEN k.rotulo = 'neutro' THEN 1 ELSE 0 END) / count(*) AS DOUBLE), 'alerta',
         100.0 * sum(CASE WHEN k.rotulo = 'neutro' THEN 1 ELSE 0 END) / count(*) >= 10.0,
         'abaixo de 10 pct de neutro sugere colapso de classe (o checkpoint degenerado dava 3,6 pct)',
         '{VERSAO_PIPELINE}', current_timestamp()
  FROM silver.classificacao k JOIN silver.postagem p ON p.postagem_id = k.postagem_id
  WHERE k.modelo = '{MODELO}' AND k.versao = '{VERSAO_MODELO}'
  GROUP BY p.caso_slug
""")

spark.sql(f"""
  INSERT INTO silver.qc_resultado
    (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline, verificado_em)
  VALUES ('monark', 'qc_classificador_acuracia_teste', {acc_teste:.4f}, 'alerta', {str(acc_teste >= 0.70).lower()},
          'acuracia do modelo publicado no teste do gabarito (157); card 0,732', '{VERSAO_PIPELINE}', current_timestamp())
""")

display(spark.sql(f"""
  SELECT caso_slug, indicador, valor, severidade, aprovado, detalhe
  FROM silver.qc_resultado
  WHERE indicador LIKE 'qc_classific%' AND versao_pipeline = '{VERSAO_PIPELINE}'
  ORDER BY caso_slug, indicador
"""))
display(spark.sql("SELECT * FROM silver.v_qc_portao ORDER BY caso_slug"))

# COMMAND ----------

import collections
print("previsto no teste :", collections.Counter(prev))
print("verdade no teste  :", collections.Counter(verdade))