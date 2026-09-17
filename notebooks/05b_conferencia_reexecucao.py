# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05b - Conferencia da reexecucao da analise (plano C, passo 5)
# MAGIC
# MAGIC Compara, arquivo a arquivo, a pasta de entrega (`evidencias/bloco4`, gerada em 08-10/09/2026)
# MAGIC com a pasta gravada pelo job (`evidencias/bloco5/analise_reexecucao`).
# MAGIC Esperado: todos os CSV identicos byte a byte (nenhum numero da analise mudou).
# MAGIC PNG podem diferir em metadados internos; para eles a conferencia e o tamanho e o CSV correspondente.
# MAGIC Nao le tabela nenhuma; nao grava nada.

# COMMAND ----------

import os, hashlib

RAIZ = "/Workspace/Users/paes120@gmail.com/scapegoat_pipeline"
ORIGINAL = f"{RAIZ}/evidencias/bloco4"
REEXEC   = f"{RAIZ}/evidencias/bloco5/analise_reexecucao"

def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()

reexec = sorted(f for f in os.listdir(REEXEC) if f.endswith((".csv", ".png")))
linhas = []
for nome in reexec:
    a, b = f"{ORIGINAL}/{nome}", f"{REEXEC}/{nome}"
    if not os.path.exists(a):
        linhas.append((nome, "SEM ORIGINAL", None, None, False)); continue
    ha, hb = sha(a), sha(b)
    linhas.append((nome, nome.rsplit(".", 1)[1], os.path.getsize(a), os.path.getsize(b), ha == hb))

import pandas as pd
df = pd.DataFrame(linhas, columns=["arquivo", "tipo", "bytes_original", "bytes_reexecucao", "identico"])
display(df)

csv = df[df.tipo == "csv"]
png = df[df.tipo == "png"]
print(f"CSV: {int(csv.identico.sum())}/{len(csv)} identicos")
print(f"PNG: {int(png.identico.sum())}/{len(png)} identicos (diferencas de metadados sao esperadas)")
assert len(csv) > 0 and csv.identico.all(), "algum CSV da analise mudou - investigar antes de seguir"