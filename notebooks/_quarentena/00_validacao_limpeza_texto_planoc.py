# Databricks notebook source
# MAGIC %md
# MAGIC # Validacao da limpeza de texto (`texto_limpo`) contra o oraculo de 1.569 pares
# MAGIC
# MAGIC 1. Carrega o gabarito (`merged_train` + `merged_val` + `merged_test`).
# MAGIC 2. **Teste A** - a expressao Spark SQL reproduz `clean_text` a partir de `text`? (em memoria)
# MAGIC 3. Preenche `silver.postagem.texto_limpo` com a mesma expressao.
# MAGIC 4. **Teste B** - a coluna materializada na Silver bate com `clean_text` do gabarito, juntando por id?
# MAGIC    (o marcador `[IRONY]`, anotacao humana presente em 21 pares do gabarito, e removido antes de comparar)
# MAGIC 5. Grava o resultado em `silver.qc_resultado`.

# COMMAND ----------

dbutils.widgets.text("versao", "v0.2.0-dev", "Versao")
VERSAO = dbutils.widgets.get("versao")

spark.sql("USE CATALOG scapegoat")
spark.sql("USE SCHEMA silver")

GABARITO = "/Volumes/scapegoat/bronze/raw/gabarito_stance"
ARQUIVOS = ["merged_train.jsonl", "merged_val.jsonl", "merged_test.jsonl"]

# A regra canonica (claude/especificacao-limpeza-texto.md), como expressao SQL.
# __COL__ e substituido pelo nome da coluna de entrada.
# Passo 0: \p{Z} troca espacos Unicode (U+00A0 nao separavel etc.) por espaco comum,
# reproduzindo o \s do Python, que e Unicode; o \s do Java e ASCII.
LIMPEZA_SQL = r"""
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(__COL__, '\\p{Z}', ' '),
        'http\\S+|www\\S+|pic\\.twitter\\.com\\S+', ''),
      '@([A-Za-z0-9_]{1,15})', ' '),
    '#[\\p{L}\\p{N}_]+', ' '),
  '\\s+', ' '))
"""

def limpeza(coluna):
    return LIMPEZA_SQL.replace("__COL__", coluna)

print("versao:", VERSAO)

# COMMAND ----------

# MAGIC %md ## 1. Carregar o oraculo

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

partes = []
for nome in ARQUIVOS:
    df = spark.read.json(f"{GABARITO}/{nome}").withColumn("arquivo", F.lit(nome))
    partes.append(df.select("id", "case", "text", "clean_text", "stance", "arquivo"))
oraculo = reduce(lambda a, b: a.unionByName(b), partes)
oraculo.createOrReplaceTempView("oraculo")

total = oraculo.count()
ids_unicos = oraculo.select("id").distinct().count()
print(f"pares no oraculo: {total}  (esperado 1569)")
print(f"ids unicos:       {ids_unicos}")
display(oraculo.groupBy("arquivo", "case").count().orderBy("arquivo", "case"))

# COMMAND ----------

# MAGIC %md ## 2. Teste A - a expressao reproduz `clean_text` em memoria?

# COMMAND ----------

teste_a = spark.sql(f"""
  SELECT id, case, text, clean_text,
         {limpeza("text")} AS obtido
  FROM oraculo
""").withColumn("igual", F.expr("obtido <=> clean_text"))

a_total = teste_a.count()
a_iguais = teste_a.filter("igual").count()
print(f"Teste A: {a_iguais}/{a_total} iguais  ({100.0*a_iguais/a_total:.2f}%)")

if a_iguais < a_total:
    print("Divergencias (esperado x obtido):")
    display(teste_a.filter("NOT igual").select("id", "text", "clean_text", "obtido").limit(20))

# COMMAND ----------

# MAGIC %md ## 3. Preencher `silver.postagem.texto_limpo`
# MAGIC Mesma expressao. Recalcula todas as linhas (coluna derivada; a fonte `texto` nao muda).

# COMMAND ----------

antes = spark.sql("SELECT caso_slug, count(*) AS postagens, count(texto_limpo) AS com_texto_limpo FROM silver.postagem GROUP BY caso_slug")
display(antes)

spark.sql(f"""
  UPDATE silver.postagem
     SET texto_limpo = {limpeza("texto")}
""")

depois = spark.sql("SELECT caso_slug, count(*) AS postagens, count(texto_limpo) AS com_texto_limpo FROM silver.postagem GROUP BY caso_slug")
display(depois)

# COMMAND ----------

# MAGIC %md ## 4. Teste B - a coluna materializada bate com o oraculo, juntando por id?

# COMMAND ----------

teste_b = spark.sql("""
  SELECT o.id, o.case, p.caso_slug,
         o.text, p.texto,
         o.clean_text, p.texto_limpo,
         trim(regexp_replace(o.clean_text, '^\\[IRONY\\]\\s*', '')) AS clean_text_sem_marcador,
         (o.clean_text RLIKE '^\\[IRONY\\]')  AS oraculo_tem_marcador,
         (p.texto <=> o.text)              AS texto_igual,
         (p.texto_limpo <=> trim(regexp_replace(o.clean_text, '^\\[IRONY\\]\\s*', ''))) AS texto_limpo_igual
  FROM oraculo o
  LEFT JOIN silver.postagem p
         ON p.plataforma = 'x' AND p.id_nativo = CAST(o.id AS STRING)
""")
teste_b.createOrReplaceTempView("teste_b")

resumo_b = spark.sql("""
  SELECT coalesce(caso_slug, '(nao esta na Silver)') AS caso_slug,
         count(*)                                       AS pares_oraculo,
         sum(CASE WHEN caso_slug IS NOT NULL THEN 1 ELSE 0 END) AS encontrados_na_silver,
         sum(CASE WHEN texto_igual THEN 1 ELSE 0 END)           AS texto_original_igual,
         sum(CASE WHEN oraculo_tem_marcador THEN 1 ELSE 0 END)  AS oraculo_com_marcador_irony,
         sum(CASE WHEN texto_limpo_igual THEN 1 ELSE 0 END)     AS texto_limpo_igual_sem_marcador
  FROM teste_b
  GROUP BY coalesce(caso_slug, '(nao esta na Silver)')
  ORDER BY 1
""")
display(resumo_b)

divergentes_b = spark.sql("""
  SELECT id, caso_slug, clean_text_sem_marcador, texto_limpo
  FROM teste_b
  WHERE caso_slug IS NOT NULL AND NOT texto_limpo_igual
""")
n_div = divergentes_b.count()
print(f"Teste B: {n_div} divergencias entre a Silver e o oraculo (esperado 0)")
if n_div > 0:
    display(divergentes_b.limit(20))

# COMMAND ----------

# MAGIC %md ## 5. Registrar em `silver.qc_resultado`

# COMMAND ----------

spark.sql(f"""
  DELETE FROM silver.qc_resultado
  WHERE indicador = 'qc_texto_limpo_reproduz_oraculo' AND versao_pipeline = '{VERSAO}'
""")

spark.sql(f"""
  INSERT INTO silver.qc_resultado
    (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline, verificado_em)
  SELECT caso_slug,
         'qc_texto_limpo_reproduz_oraculo',
         CAST(100.0 * sum(CASE WHEN texto_limpo_igual THEN 1 ELSE 0 END) / count(*) AS DOUBLE),
         'bloqueia',
         sum(CASE WHEN texto_limpo_igual THEN 0 ELSE 1 END) = 0,
         concat('pares do oraculo na Silver: ', count(*),
                '; texto_limpo identico a clean_text (sem o marcador [IRONY] do gabarito) em ',
                sum(CASE WHEN texto_limpo_igual THEN 1 ELSE 0 END),
                '; pares com marcador [IRONY] no gabarito: ',
                sum(CASE WHEN oraculo_tem_marcador THEN 1 ELSE 0 END)),
         '{VERSAO}',
         current_timestamp()
  FROM teste_b
  WHERE caso_slug IS NOT NULL
  GROUP BY caso_slug
""")

display(spark.sql(f"""
  SELECT caso_slug, indicador, valor, severidade, aprovado, detalhe
  FROM silver.qc_resultado
  WHERE indicador = 'qc_texto_limpo_reproduz_oraculo' AND versao_pipeline = '{VERSAO}'
"""))
display(spark.sql("SELECT * FROM silver.v_qc_portao ORDER BY caso_slug"))