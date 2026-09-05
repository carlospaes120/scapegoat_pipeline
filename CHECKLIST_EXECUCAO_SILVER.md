# 📋 Checklist de Execução: Bronze → Silver

**Data de preparação:** 04/09/2026  
**Status:** Pronto para execução no Databricks  
**Duração estimada:** 3–4 horas (incluindo debug)

---

## 🚀 Pré-requisitos (✅ já atendidos)

- [x] Bronze carregada e conferida (18 + 1 arquivos, 19.049 registros)
- [x] Classificador auditado (BERTimbau v2b, acurácia 0,732)
- [x] Limpeza de texto especificada (1.569/1.569 pares em Python)
- [x] SQL traduzido de PostgreSQL para Spark SQL
- [x] Valores esperados do QC documentados (19 indicadores)

---

## 🔧 Fase 1: Setup Databricks

### 1.1 Validar catálogos e schemas

Executar no Databricks SQL Editor:

```sql
-- Verificar catálogo
SHOW CATALOGS;  -- Procurar por 'scapegoat'

-- Entrar no catálogo
USE CATALOG scapegoat;

-- Verificar schemas
SHOW SCHEMAS;
-- Esperado: bronze, silver, gold, pub
```

**Resultado esperado:** ✅ Todos os 4 schemas visíveis

### 1.2 Validar Bronze

```sql
USE CATALOG scapegoat;
USE SCHEMA bronze;

-- Contar arquivos
SELECT count(*) AS num_arquivos FROM arquivo;
-- Esperado: 2 (graphql + consolidado)

-- Contar registros brutos
SELECT count(*) AS num_registros FROM registro;
-- Esperado: 19.049 (13.906 + 5.143)

-- Ver os dois casos
SELECT caso_slug, fonte, count(*) AS registros
  FROM arquivo
  GROUP BY caso_slug, fonte
  ORDER BY caso_slug, fonte;
-- Esperado:
-- arthur_do_val | graphql     | 13.906
-- monark        | consolidado |  5.143
```

**Resultado esperado:** ✅ Contagens exatas

---

## 📝 Fase 2: Criar as views e tabelas Silver

### 2.1 Executar o SQL traduzido (Bloco 0 + Bloco 2)

Copiar todo o arquivo `03_bronze_to_silver_SPARK.sql` para o Databricks SQL Editor.

**Dividir em chunks:**

1. **Bloco 0:** Criar schemas (já feito, skip)
2. **Bloco 1:** Bronze (já existe, skip)
3. **Bloco 2:** Views de normalização e deduplicação
4. **Bloco 3:** QC
5. **Bloco 4:** Promoção (MERGE)

**Executar Bloco 2 primeiro:**

```sql
-- Bloco 2: Normalização
CREATE OR REPLACE VIEW silver.v_normalizado AS ...
-- Copiar do arquivo

CREATE OR REPLACE VIEW silver.v_deduplicado AS ...
-- Copiar do arquivo (inclui texto_limpo)
```

**Validar a normalização:**

```sql
SELECT count(*) FROM silver.v_normalizado WHERE caso_slug = 'monark';
-- Esperado: 5.143

SELECT count(*) FROM silver.v_deduplicado WHERE caso_slug = 'monark';
-- Esperado: 4.803

-- Validar que texto_limpo existe e é não-nulo
SELECT count(*) FROM silver.v_deduplicado 
 WHERE caso_slug = 'monark' AND texto_limpo IS NOT NULL;
-- Esperado: 4.803 (100%)
```

**Resultado esperado:** ✅ Views criadas, contagens exatas

### 2.2 Validar a limpeza de texto (amostra)

```sql
-- Comparar antes e depois em uma amostra
SELECT 
  texto AS original,
  texto_limpo AS limpo,
  length(texto) AS len_orig,
  length(texto_limpo) AS len_limpo
FROM silver.v_deduplicado
WHERE caso_slug = 'monark'
  AND (texto LIKE '%http%' OR texto LIKE '%@%' OR texto LIKE '%#%')
LIMIT 10;
-- Observar que URLs, menções e hashtags foram removidas/substituídas
```

**Resultado esperado:** ✅ Limpeza visível nos exemplos

---

## 🧪 Fase 3: Executar o QC (Bloco 3)

### 3.1 Criar tabela de resultados

```sql
-- Bloco 3 — parte inicial (até antes do INSERT)
CREATE TABLE IF NOT EXISTS silver.qc_resultado (
  caso_slug       STRING NOT NULL,
  indicador       STRING NOT NULL,
  valor           DOUBLE,
  severidade      STRING NOT NULL,
  aprovado        BOOLEAN NOT NULL,
  detalhe         STRING,
  versao_pipeline STRING NOT NULL,
  verificado_em   TIMESTAMP NOT NULL DEFAULT current_timestamp()
) COMMENT 'Resultado das verificações de qualidade...';

ALTER TABLE silver.qc_resultado ADD CONSTRAINT qc_severidade_valida
  CHECK (severidade IN ('bloqueia','alerta','informa'));
```

**Resultado esperado:** ✅ Tabela criada

### 3.2 Executar a bateria de QC (CASO 1: MONARK)

```sql
-- Caso 1: monark
-- Parâmetros: :caso = 'monark', :versao = 'v0.2.0-dev'

DELETE FROM silver.qc_resultado WHERE caso_slug = 'monark';

INSERT INTO silver.qc_resultado (caso_slug, indicador, valor, severidade, aprovado, detalhe, versao_pipeline)
WITH base AS (SELECT * FROM silver.v_normalizado WHERE caso_slug = 'monark'),
     dedup AS (SELECT * FROM silver.v_deduplicado WHERE caso_slug = 'monark'),
     arq   AS (SELECT * FROM bronze.arquivo WHERE caso_slug = 'monark')
SELECT * FROM (
  -- Colar TODO o conteúdo do INSERT FROM do arquivo
  -- (linhas 212–359 de 03_bronze_to_silver_SPARK.sql)
);
```

**Obs:** O INSERT é muito longo (~150 linhas). Copiar inteiro do arquivo.

### 3.3 Verificar a bateria executou

```sql
SELECT count(*) AS num_indicadores FROM silver.qc_resultado 
 WHERE caso_slug = 'monark';
-- Esperado: 19

SELECT count(*) AS num_reprovados FROM silver.qc_resultado 
 WHERE caso_slug = 'monark' AND NOT aprovado AND severidade = 'bloqueia';
-- Esperado: 0 (todos bloqueadores devem passar)
```

**Resultado esperado:** ✅ 19 indicadores, 0 reprovados bloqueadores

### 3.4 Ver o portão

```sql
CREATE OR REPLACE VIEW silver.v_qc_portao AS
SELECT caso_slug, versao_pipeline,
       sum(CASE WHEN severidade = 'bloqueia' AND NOT aprovado THEN 1 ELSE 0 END) AS bloqueios,
       sum(CASE WHEN severidade = 'alerta'   AND NOT aprovado THEN 1 ELSE 0 END) AS alertas,
       sum(CASE WHEN severidade = 'bloqueia' AND NOT aprovado THEN 1 ELSE 0 END) = 0 AS pode_promover
FROM silver.qc_resultado
GROUP BY caso_slug, versao_pipeline;

SELECT * FROM silver.v_qc_portao WHERE caso_slug = 'monark';
-- Esperado: pode_promover = TRUE
```

**Resultado esperado:** ✅ `pode_promover = true`

---

## 🔍 Fase 4: Conferência dos 19 indicadores vs. Oráculo

### 4.1 Tabela de confronto (MONARK)

```sql
-- Tabela: valores esperados (do arquivo tests/valores_esperados_qc.md)
WITH esperado AS (
  SELECT 'qc_linhas_bronze' AS ind, 5143.0 AS val UNION ALL
  SELECT 'qc_postagens_unicas', 4803.0 UNION ALL
  SELECT 'qc_duplicatas_removidas', 340.0 UNION ALL
  SELECT 'qc_ids_duplicados', 305.0 UNION ALL
  SELECT 'qc_duplicatas_divergentes', 0.0 UNION ALL
  SELECT 'qc_id_fora_do_padrao', 0.0 UNION ALL
  SELECT 'qc_descartadas_sem_autor', 0.0 UNION ALL
  SELECT 'qc_pct_sem_autor', 0.0 UNION ALL
  SELECT 'qc_data_nao_parseada', 0.0 UNION ALL
  SELECT 'qc_fora_da_janela', 0.0 UNION ALL
  SELECT 'qc_reply_sem_destino', 0.0 UNION ALL
  SELECT 'qc_replies_reclassificados', 2335.0 UNION ALL
  SELECT 'qc_contador_negativo', 0.0 UNION ALL
  SELECT 'qc_texto_vazio', 0.0 UNION ALL
  SELECT 'qc_idioma_inesperado', 0.0 UNION ALL
  SELECT 'qc_mencoes_totais', 5932.0 UNION ALL
  SELECT 'qc_pct_mencoes_com_id', 100.0 UNION ALL
  SELECT 'qc_cv_volume_diario', 0.74 UNION ALL
  SELECT 'qc_com_stance_previa', 0.0
)
SELECT 
  e.ind,
  e.val AS esperado,
  r.valor AS obtido,
  CASE 
    WHEN abs(e.val - r.valor) < 0.01 THEN '✅ OK'
    ELSE '❌ DIVERGE'
  END AS status,
  r.aprovado,
  r.severidade
FROM esperado e
LEFT JOIN silver.qc_resultado r 
  ON r.indicador = e.ind AND r.caso_slug = 'monark'
ORDER BY status DESC, e.ind;
```

**Resultado esperado:** ✅ 19/19 `✅ OK`

### 4.2 Mostrar reprovados (se houver)

```sql
SELECT indicador, valor, severidade, aprovado, detalhe
  FROM silver.qc_resultado
 WHERE caso_slug = 'monark' AND NOT aprovado
 ORDER BY severidade DESC, indicador;
-- Esperado: (nenhuma linha)
```

**Resultado esperado:** ✅ Sem resultados (todos aprovados)

---

## 🏗️ Fase 5: Promoção para Silver (Bloco 4)

### 5.1 Validar que Silver tem as tabelas vazias ou prontas

```sql
-- Verificar se as tabelas existem
SHOW TABLES IN silver;
-- Esperado: conta, postagem, captura, mencao, postagem_hashtag, classificacao

-- Limpar execuções anteriores (se houver)
DELETE FROM silver.postagem WHERE caso_slug = 'monark';
DELETE FROM silver.captura WHERE postagem_id IN 
  (SELECT postagem_id FROM silver.postagem WHERE caso_slug = 'monark');
DELETE FROM silver.mencao WHERE postagem_id IN 
  (SELECT postagem_id FROM silver.postagem WHERE caso_slug = 'monark');
DELETE FROM silver.postagem_hashtag WHERE postagem_id IN 
  (SELECT postagem_id FROM silver.postagem WHERE caso_slug = 'monark');
DELETE FROM silver.classificacao WHERE postagem_id IN 
  (SELECT postagem_id FROM silver.postagem WHERE caso_slug = 'monark');
-- Deixar conta por último (FK)
```

### 5.2 Executar o Bloco 4 (Views + MERGE)

**Importante:** Executar APENAS se `pode_promover = true` (validado na Fase 4).

```sql
-- Bloco 4 — Views
CREATE OR REPLACE VIEW silver.v_promovivel AS ...
-- Copiar do arquivo

-- Bloco 4 — MERGE (executar um por um, na ordem)

-- 4.1 CONTA
MERGE INTO silver.conta AS alvo
USING (
  -- Colar do arquivo
) AS origem
ON ...
WHEN ...;

-- 4.2 POSTAGEM
MERGE INTO silver.postagem AS alvo
USING (
  -- Colar do arquivo
) AS origem
ON ...
WHEN ...;

-- 4.3 CAPTURA
INSERT INTO silver.captura (postagem_id, likes, retweets, quotes, respostas, capturas, capturado_em)
-- Colar do arquivo

-- 4.4 MENCAO
INSERT INTO silver.mencao (postagem_id, conta_id)
-- Colar do arquivo

-- 4.5 POSTAGEM_HASHTAG
INSERT INTO silver.postagem_hashtag (postagem_id, hashtag)
-- Colar do arquivo

-- 4.6 CLASSIFICACAO
INSERT INTO silver.classificacao (postagem_id, esquema, rotulo, modelo, versao, confianca, classificado_em)
-- Colar do arquivo
```

**Resultado esperado:** ✅ Todos os MERGE/INSERT completam sem erro

### 5.3 Validar as tabelas Silver

```sql
-- CONTA
SELECT count(*) FROM silver.conta 
 WHERE plataforma = 'x' AND handle LIKE '%monark%';
-- Esperado: ~50–100 (depende do grafo)

-- POSTAGEM (Monark)
SELECT count(*) FROM silver.postagem WHERE caso_slug = 'monark';
-- Esperado: 4.803

-- MENCAO (Monark)
SELECT count(*) FROM silver.mencao m
 WHERE EXISTS (SELECT 1 FROM silver.postagem p 
               WHERE p.postagem_id = m.postagem_id 
               AND p.caso_slug = 'monark');
-- Esperado: 5.868

-- POSTAGEM_HASHTAG (Monark)
SELECT count(*) FROM silver.postagem_hashtag ph
 WHERE EXISTS (SELECT 1 FROM silver.postagem p 
               WHERE p.postagem_id = ph.postagem_id 
               AND p.caso_slug = 'monark');
-- Esperado: 1.138
```

**Resultado esperado:** ✅ Contagens exatas ou bem próximas

---

## 🔄 Fase 6: Repetir com CASE 2 (ARTHUR_DO_VAL)

Repetir as **Fases 3, 4 e 5** com `:caso = 'arthur_do_val'` e `:versao = 'v0.2.0-dev'`.

### Valores esperados para arthur_do_val:

| Indicador | arthur_do_val |
|---|---:|
| qc_linhas_bronze | 13.906 |
| qc_postagens_unicas | 13.906 |
| qc_duplicatas_removidas | 0 |
| qc_ids_duplicados | 0 |
| qc_descartadas_sem_autor | 13 |
| qc_pct_sem_autor | 0,093% |
| qc_replies_reclassificados | 9.893 |
| qc_mencoes_totais | 23.190 |
| qc_cv_volume_diario | 0,313 |

### Tabelas esperadas:

| Tabela | arthur_do_val |
|---|---:|
| postagem | 13.893 |
| mencao | 22.954 |
| postagem_hashtag | 1.034 |

**Resultado esperado:** ✅ 19/19 indicadores, `pode_promover = true`

---

## 📊 Fase 7: Documentação e evidências

### 7.1 Capturar screenshots

- [ ] Bronze counts (2 arquivos, 19.049 registros)
- [ ] Normalizado/Deduplicado (5.143 → 4.803, monark)
- [ ] QC portão (pode_promover = true, ambos casos)
- [ ] Tabela de confronto (19 indicadores vs. oráculo, monark)
- [ ] Silver tabelas (conta, postagem, mencao, hashtag, monark)

Salvar em: `evidencias/silver_qc_*.png`

### 7.2 Atualizar diário

Adicionar entrada em `claude/diario-de-execucao.md`:

```markdown
## 04/09/2026 — dia 2.5 · Silver em Delta, 19 indicadores validados

### Silver — carregada e conferida

- **Normalização e deduplicação:** Monark 5.143 → 4.803 (340 duplicatas)
- **Limpeza de texto:** 1.569/1.569 pares em Spark SQL, 100% reprodução
- **QC bloqueante:** 19 indicadores, 8 bloqueadores, ambos aprovados
- **Portão:** pode_promover = true (monark e arthur_do_val)
- **Silver tabelas:** conta, postagem, mencao, postagem_hashtag populadas
  - Monark: 4.803 postagens, 5.868 menções, 1.138 hashtags
  - Arthur: 13.893 postagens, 22.954 menções, 1.034 hashtags
```

### 7.3 Git commit

```bash
cd C:\NETLOGO\13_scapegoat_pipeline_mvp

git add sql/03_bronze_to_silver_SPARK.sql
git add tests/valores_esperados_qc.md
git add evidencias/silver_*.png
git add claude/diario-de-execucao.md

git commit -m "Camada Silver em Delta: limpeza Unicode, 19 indicadores QC validados

- Traduzir 03_bronze_to_silver.sql PostgreSQL → Spark SQL/Delta
- Regex hashtags: #\w+ → #[\p{L}\p{N}_]+ (Unicode: ã, ç, etc.)
- Limpeza validada contra 1.569/1.569 pares: 100% reprodução
- QC: 19 indicadores, 8 bloqueadores, ambos casos aprovados
- Silver tabelas persistidas: conta, postagem, mencao, postagem_hashtag
- Monark 4.803 postagens; Arthur 13.893

Bloco 2 (Bronze→Silver) 100% completo.
Próximo: Bloco 3 (Gold e catálogo de dados).

Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>"

git tag v0.2.0-silver-delta
```

---

## 🚨 Troubleshooting

### Erro: `RLIKE` não reconhecido

→ Usar `regexp` em vez de `RLIKE` no Spark SQL:

```sql
WHERE texto regexp '^[0-9]{15,20}$'
```

### Erro: Parâmetro `:caso` não funciona

→ Usar SQL dinâmica ou substituir inline:

```sql
-- Em vez de:
WHERE caso_slug = :caso

-- Usar:
WHERE caso_slug = 'monark'  -- ou 'arthur_do_val'
```

### Divergência no QC (< 19 indicadores OK)

→ Verificar a order das operações de regex na limpeza:
1. URLs
2. Menções
3. Hashtags (⚠️ deve usar `[\p{L}\p{N}_]+`, não `\w`)
4. Espaços

Se ainda divergir, executar o notebook `00_validacao_limpeza_texto.py` para debug isolado.

### Erro: Tabela Silver não tem texto_limpo

→ Verificar se a coluna foi adicionada ao DDL da tabela `postagem`:

```sql
ALTER TABLE silver.postagem ADD COLUMN texto_limpo STRING;
```

---

## ✅ Critério de sucesso

- [x] Bronze carregada: 19.049 registros
- [ ] Views Silver (normalizado, deduplicado): contagens exatas
- [ ] Limpeza de texto: 1.569/1.569 pares (se validado)
- [ ] QC: 19 indicadores, 19/19 aprovados
- [ ] Portão: `pode_promover = true` (ambos casos)
- [ ] Tabelas Silver: conta, postagem, mencao, postagem_hashtag populadas
- [ ] Documentação: screenshots, commit, diário atualizado

**Quando todos estiverem checked:** Pronto para Bloco 3 (Gold).

---

**Tempo estimado:** 3–4 horas (50% execução, 50% validação)  
**Buffer:** ±30 min para debug de regexes ou divergências QC  
**Próxima sessão:** Bloco 3 (Gold e Catálogo), 15–20/09
