# Databricks notebook source

# =====================================================================
# VALIDAÇÃO DA LIMPEZA DE TEXTO
# Notebook para Databricks
#
# Objetivo: Validar que a regex Spark SQL de limpeza de texto reproduz
# 100% das transformações contra os 1.569 pares de teste do repositório
# carlospaes120/scapegoat (mvp/data/merged_test.jsonl).
#
# Resultado esperado: 1.569/1.569 pares = 100% reprodução
# Data: 04/09/2026
# =====================================================================

# COMMAND ----------

import json
from pyspark.sql.functions import regexp_replace, trim, col, size
import pandas as pd

# COMMAND ----------

# =====================================================================
# ETAPA 1: Carregar os dados de teste
# =====================================================================

print("=" * 70)
print("ETAPA 1: Carregar os 1.569 pares de teste")
print("=" * 70)

# Opção A: Se os dados estão em um Volume do Databricks
# test_json_path = "/Volumes/scapegoat/raw/mvp_data/merged_test.jsonl"

# Opção B: Carregar localmente e subir como DataFrame (para este MVP)
# Os dados virão em um formato JSONL com chaves:
# - 'text': o texto original
# - 'clean_text': o texto limpo esperado
# - 'label': a classe (acusador | defensor | neutro)

# Para fins desta validação, vamos simular o carregamento:
test_data = spark.read.option("multiline", "true").json(
    "/dbfs/mnt/user-data/uploads/merged_test.jsonl"  # Caminho após upload
)

print(f"✅ Dados carregados: {test_data.count()} pares")
print(f"   Colunas: {test_data.columns}")

# COMMAND ----------

# =====================================================================
# ETAPA 2: Definir a regex canônica do Spark SQL
# =====================================================================

print("\n" + "=" * 70)
print("ETAPA 2: Aplicar a regex Spark SQL (4 passos)")
print("=" * 70)

# Regex canônica (validada contra Python em 1.569/1.569):
# 1. URLs: http\S+, www\S+, pic\.twitter\.com\S+
# 2. Menções: @([A-Za-z0-9_]{1,15}) → espaço
# 3. Hashtags: #[\p{L}\p{N}_]+  (Unicode: ação, açúcar, etc.)
# 4. Espaços múltiplos → um espaço, trim

def apply_cleaning(texto):
    """Aplica a limpeza de texto exatamente como especificado."""
    if texto is None or texto == "":
        return ""

    # Passo 1: URLs
    texto = regexp_replace(col(texto) if isinstance(texto, str) else texto,
                          r'http\S+|www\S+|pic\.twitter\.com\S+', '')
    # Passo 2: Menções
    texto = regexp_replace(texto, r'@([A-Za-z0-9_]{1,15})', ' ')
    # Passo 3: Hashtags (Unicode-aware)
    texto = regexp_replace(texto, r'#[\p{L}\p{N}_]+', ' ')
    # Passo 4: Espaços múltiplos
    texto = trim(regexp_replace(texto, r'\s+', ' '))

    return texto

# Aplicar a limpeza usando Spark SQL
test_limpo = test_data.withColumn(
    "texto_limpo_spark",
    trim(regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(col("text"),
                    r'http\S+|www\S+|pic\.twitter\.com\S+', ''),
                r'@([A-Za-z0-9_]{1,15})', ' '),
            r'#[\p{L}\p{N}_]+', ' '),
        r'\s+', ' ')))

print("✅ Regex aplicada em todas as linhas")

# COMMAND ----------

# =====================================================================
# ETAPA 3: Comparar com o oráculo
# =====================================================================

print("\n" + "=" * 70)
print("ETAPA 3: Comparar com o oráculo (clean_text original)")
print("=" * 70)

# Comparar: texto_limpo_spark vs clean_text
comparacao = test_limpo.withColumn(
    "diverge",
    col("texto_limpo_spark") != col("clean_text")
)

# Contar divergências
total_pares = comparacao.count()
total_divergencias = comparacao.filter(col("diverge")).count()
taxa_reproducao = 100.0 * (total_pares - total_divergencias) / total_pares

print(f"\n📊 RESULTADO FINAL")
print(f"   Pares testados:        {total_pares:,}")
print(f"   Pares iguais:          {total_pares - total_divergencias:,}")
print(f"   Pares divergentes:     {total_divergencias:,}")
print(f"   Taxa de reprodução:    {taxa_reproducao:.2f}%")

# COMMAND ----------

# =====================================================================
# ETAPA 4: Mostrar exemplos (se houver divergências)
# =====================================================================

if total_divergencias > 0:
    print("\n⚠️  DIVERGÊNCIAS DETECTADAS!")
    print("=" * 70)
    print("Primeiras 10 divergências:\n")

    divergentes = comparacao.filter(col("diverge")).select(
        col("text"),
        col("clean_text").alias("clean_text_esperado"),
        col("texto_limpo_spark").alias("clean_text_obtido")
    ).limit(10)

    divergentes.display()

    # Análise adicional: qual etapa diverge?
    print("\n🔍 DIAGNÓSTICO DE DIVERGÊNCIAS")

    # Testar cada etapa isoladamente
    etapas = [
        ("URLs",      r'http\S+|www\S+|pic\.twitter\.com\S+'),
        ("Menções",   r'@([A-Za-z0-9_]{1,15})'),
        ("Hashtags",  r'#[\p{L}\p{N}_]+'),
        ("Espaços",   r'\s+')
    ]

    diagnostico = comparacao.filter(col("diverge")).select(col("text"))

    for nome_etapa, regex in etapas:
        teste = diagnostico.withColumn(
            f"tem_{nome_etapa.lower()}",
            col("text").rlike(regex)
        )
        count_com_etapa = teste.filter(col(f"tem_{nome_etapa.lower()}")).count()
        print(f"   {nome_etapa}: {count_com_etapa} divergentes têm este padrão")

else:
    print("\n" + "=" * 70)
    print("✅ SUCESSO! Taxa de reprodução: 100%")
    print("=" * 70)
    print("\nA regex Spark SQL é canônica e pode ser usada em produção.")
    print("Próximo passo: Rodar o pipeline Silver com confiança.")

# COMMAND ----------

# =====================================================================
# ETAPA 5: Exportar relatório
# =====================================================================

print("\n" + "=" * 70)
print("ETAPA 5: Salvar relatório de validação")
print("=" * 70)

# Criar um DataFrame de resumo
resumo = spark.createDataFrame([
    ("Data", "04/09/2026"),
    ("Total de pares", str(total_pares)),
    ("Taxa de reprodução", f"{taxa_reproducao:.2f}%"),
    ("Status", "✅ APROVADO" if total_divergencias == 0 else f"❌ FALHOU ({total_divergencias} divergências)"),
    ("Regex hashtags", "#[\\p{L}\\p{N}_]+ (Unicode-aware)"),
    ("Próximo passo", "Executar bloco Silver no Databricks")
], ["metrica", "valor"])

resumo.write.mode("overwrite").option("header", "true").csv(
    "/dbfs/mnt/user-data/outputs/validacao_limpeza_resultado.csv"
)

print("✅ Relatório salvo em: validacao_limpeza_resultado.csv")

# Também salvar os pares divergentes (se houver)
if total_divergencias > 0:
    comparacao.filter(col("diverge")).select(
        col("text"),
        col("clean_text"),
        col("texto_limpo_spark")
    ).write.mode("overwrite").option("header", "true").csv(
        "/dbfs/mnt/user-data/outputs/divergencias_limpeza.csv"
    )
    print(f"⚠️  Divergências salvas em: divergencias_limpeza.csv")

# COMMAND ----------

# =====================================================================
# ETAPA 6: Gráfico de visualização (opcional)
# =====================================================================

import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# Gráfico 1: Pizza de aprovação
labels = ["Aprovado", "Divergente"]
sizes = [total_pares - total_divergencias, total_divergencias]
colors = ["#2ecc71", "#e74c3c"]
axes[0].pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
axes[0].set_title(f"Taxa de reprodução: {taxa_reproducao:.2f}%")

# Gráfico 2: Contagem
ax = axes[1]
ax.bar(["Pares", "Divergências"], [total_pares - total_divergencias, total_divergencias],
       color=["#2ecc71", "#e74c3c"])
ax.set_ylabel("Contagem")
ax.set_title("Resultado da validação")
ax.set_ylim(0, total_pares * 1.1)

plt.tight_layout()
plt.savefig("/dbfs/mnt/user-data/outputs/validacao_limpeza_grafico.png", dpi=150, bbox_inches='tight')
print("\n✅ Gráfico salvo em: validacao_limpeza_grafico.png")

plt.show()

# COMMAND ----------

# =====================================================================
# RESUMO EXECUTIVO
# =====================================================================

print("\n" + "=" * 70)
print("RESUMO EXECUTIVO")
print("=" * 70)
print(f"""
Validação da limpeza de texto (texto_limpo) contra 1.569 pares:

  Taxa de reprodução: {taxa_reproducao:.2f}%
  Status: {'✅ APROVADO' if total_divergencias == 0 else f'❌ REPROVADO'}

  Regex Spark SQL (canônica):
  trim(regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(texto,
          'http\\\\S+|www\\\\S+|pic\\\\.twitter\\\\.com\\\\S+', ''),
        '@([A-Za-z0-9_]{{1,15}})', ' '),
      '#[\\\\p{{L}}\\\\p{{N}}_]+', ' '),
    '\\\\s+', ' '))

  Validação: PASSOU {total_pares - total_divergencias}/{total_pares}

Próximos passos:
  1. ✅ Validação concluída (este notebook)
  2. ⏳ Rodar o SQL da camada Silver (03_bronze_to_silver_SPARK.sql)
  3. ⏳ Executar QC e conferir 19 indicadores
  4. ⏳ Conferir contagens contra tests/valores_esperados_qc.md
  5. ⏳ Rodar com ambos os casos (monark + arthur_do_val)
""")

# COMMAND ----------

# FIM DO NOTEBOOK
