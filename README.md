# MVP de Engenharia de Dados — repositório analítico de episódios de hostilidade coletiva online

**Aluno:** Carlos A. Paes · **Curso:** Pós-graduação em Ciência de Dados e Analytics, PUC-Rio
**Sprint:** Engenharia de Dados · **Entrega:** 27/09/2026 · **Plataforma:** Databricks Free Edition

> **Estado:** esqueleto. Os sete tópicos abaixo são os exigidos pelo item 5 do enunciado e serão
> preenchidos ao longo dos blocos de trabalho. Este arquivo é **o documento avaliado** — os
> notebooks são código referenciado a partir daqui.

---

## Estrutura do repositório

```
README.md          este documento — o entregável avaliado
sql/
  ddl_v3_1_mvp.sql        especificação do modelo, testada em PostgreSQL 16
  roles_grants.sql        perfis de acesso e privilégios (§9 da Política de Dados)
  03_bronze_to_silver.sql normalização das duas fontes, QC bloqueante e promoção
notebooks/
  01_ingestao_bronze.py   lê o Volume, calcula hash, grava Bronze
tests/
  test_ddl_v3_1_mvp.sql   25 verificações que provam as regras do modelo
  run_local.sh            recria um banco limpo e roda a bateria
  valores_esperados_qc.md oráculo: os números que a execução tem de reproduzir
casos/             manifesto por caso (consulta, janela, fonte)
catalogo/          Catálogo de Dados + diagrama do esquema estrela
evidencias/        screenshots de cada etapa e de cada resposta
```

**Os dados não estão neste repositório**, por decisão de projeto e conforme o item 4 do enunciado.

---

## 1. Contexto de Negócios e Perguntas (Etapa 2. e 4.1)

<!-- Bloco 5. Fontes: claude/mvp-objetivo.md v6 (§1, §2, §3, §4) + a seção de licença.
     Inclui obrigatoriamente: perguntas de negócio, contexto dos dados brutos,
     resumo da estrutura (colunas e tabelas) e a LICENÇA dos dados. -->

*A preencher.*

## 2. Carga dos Dados (Etapa 4.2)

<!-- Bloco 2. Como os arquivos chegaram ao Volume, o inventário com hash,
     as duas fontes de ingestão, e referência a notebooks/01_ingestao_bronze.py -->

*A preencher.*

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

<!-- Bloco 3. Arquitetura medalhão, esquema estrela, e o Catálogo transcrito
     com tipo, domínio de valores e linhagem por campo, mais screenshots do
     Unity Catalog. Vale 2,0 pt. -->

*A preencher.*

## 4. Pipeline de Dados (Etapa 4.4)

<!-- Bloco 4. Como o pipeline foi organizado em notebooks, cada transformação
     justificada, e screenshots das tabelas persistidas. -->

*A preencher.*

## 5. Qualidade de Dados (Etapa 4.5)

<!-- Bloco 4. Problemas detectados por atributo e como cada um foi tratado.
     Critério próprio, 1,0 pt. Base: silver.qc_resultado + tests/valores_esperados_qc.md -->

*A preencher.*

## 6. Análise de Dados (Etapa 4.5)

<!-- Bloco 4. Uma query por pergunta + gráfico + parágrafo de discussão.
     Discussão geral ao final. A discussão vale tanto quanto a resposta. -->

*A preencher.*

## 7. Autoavaliação

<!-- Bloco 5. O que foi atingido, o que não foi e por quê, dificuldades,
     trabalhos futuros. P4 entra aqui como não respondível, com a razão. -->

*A preencher.*
