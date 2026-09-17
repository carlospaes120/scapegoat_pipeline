# Régua de reexecução — plano C

Pipeline `scapegoat_pipeline` · consulta `tests/regua_reexecucao.sql` · rodada no SQL Editor (Serverless Starter), catálogo `scapegoat`.

**Antes:** 16/09/2026, após o `DEEP CLONE` de 14 tabelas para `scapegoat.backup` (`evidencias/bloco5/backup_clones_contagem.png`) e antes de qualquer alteração de código; branch `plano_c` criado a partir de `main` em `ecc7269`. Print: `evidencias/bloco5/regua_antes.png`.

**Depois (Monark):** 16/09/2026, após a execução do job `scapegoat_pipeline` com `caso = monark`, `versao_pipeline = v1.0` (dez tarefas Succeeded, com dois reparos — ver diário). Print: não guardado; os valores desta coluna estão na tabela abaixo.

**Depois (Arthur do Val):** 16/09/2026, após a execução com `caso = arthur_do_val`, `versao_pipeline = v1.0` (dez tarefas Succeeded). Print: `evidencias/bloco5/regua_depois_arthur.png`.

**Resultado:** todas as linhas de conteúdo (contagens, somas, distribuições, domínios) idênticas nas três colunas. Só mudam, como previsto pela decisão do Passo 3 (substituir o rótulo de trabalho por `v1.0`), as chaves de versão: `fato_rede` passa de `gold-v1` a `v1.0`; `qc_resultado` ganha as linhas `v1.0` ao lado das `v0.2.0-dev`; `v_qc_portao` passa a quatro linhas, todas `pode_promover = true`.

**Regra:** os doubles da `fato_rede` são arredondados a 6 casas na própria consulta; a barra vertical dos valores foi trocada por barra comum nesta tabela.

| tabela | métrica | chave | esperado (fonte) | antes 16/09 | depois monark | depois arthur_do_val |
|---|---|---|---|---|---|---|
| `gold.calendario_caso` | linhas | - | 26 (catalogo) | 26 | 26 | 26 |
| `gold.calendario_caso` | min/max | data | 2022-02-07 / 2022-03-14 (dominios 15/09) | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 |
| `gold.calendario_caso` | min/max | dias_desde_estopim | -7 / 10 (dominios 15/09) | -7 / 10 | -7 / 10 | -7 / 10 |
| `gold.calendario_caso` | min/max | volume_postagens | 2 / 1359 (dominios 15/09) | 2 / 1359 | 2 / 1359 | 2 / 1359 |
| `gold.dim_conta_papel` | linhas | - | 16932 (catalogo) | 16932 | 16932 | 16932 |
| `gold.dim_conta_papel` | min/max | n_mencoes_recebidas_caso | 0 / 9132 (dominios 15/09) | 0 / 9132 | 0 / 9132 | 0 / 9132 |
| `gold.dim_conta_papel` | min/max | n_postagens_caso | 0 / 77 (dominios 15/09) | 0 / 77 | 0 / 77 | 0 / 77 |
| `gold.dim_conta_papel` | min/max | primeiro_dia | 2022-02-07 / 2022-03-14 (dominios 15/09) | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 |
| `gold.dim_conta_papel` | min/max | ultimo_dia | 2022-02-07 / 2022-03-14 (dominios 15/09) | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 | 2022-02-07 / 2022-03-14 |
| `gold.fato_atividade` | linhas | - | 222 (novo 16/09) | 222 | 222 | 222 |
| `gold.fato_atividade` | linhas_por_versao_classificacao | hf@a483947 | 222 (diario 13/09) | 222 | 222 | 222 |
| `gold.fato_atividade` | min/max | likes | 0 / 333577 (dominios 15/09) | 0 / 333577 | 0 / 333577 | 0 / 333577 |
| `gold.fato_atividade` | min/max | n_autores_distintos | 1 / 530 (dominios 15/09) | 1 / 530 | 1 / 530 | 1 / 530 |
| `gold.fato_atividade` | min/max | n_postagens | 1 / 623 (dominios 15/09) | 1 / 623 | 1 / 623 | 1 / 623 |
| `gold.fato_atividade` | min/max | quotes | 0 / 4930 (dominios 15/09) | 0 / 4930 | 0 / 4930 | 0 / 4930 |
| `gold.fato_atividade` | min/max | respostas | 0 / 2939 (dominios 15/09) | 0 / 2939 | 0 / 2939 | 0 / 2939 |
| `gold.fato_atividade` | min/max | retweets | 0 / 41208 (dominios 15/09) | 0 / 41208 | 0 / 41208 | 0 / 41208 |
| `gold.fato_atividade` | soma_n_postagens | arthur_do_val | 13893 (catalogo) | 13893 | 13893 | 13893 |
| `gold.fato_atividade` | soma_n_postagens | monark | 4803 (catalogo) | 4803 | 4803 | 4803 |
| `gold.fato_rede` | linhas | - | 26 (catalogo) | 26 | 26 | 26 |
| `gold.fato_rede` | linhas_por_caso | arthur_do_val | 18 (catalogo) | 18 | 18 | 18 |
| `gold.fato_rede` | linhas_por_caso | monark | 8 (catalogo) | 8 | 8 | 8 |
| `gold.fato_rede` | linhas_por_versao_pipeline | gold-v1 | 26 (diario 13/09) | 26 | — | — |
| `gold.fato_rede` | linhas_por_versao_pipeline | v1.0 | — (linha nova: chave de versão criada pela reexecução) | — | 26 | 26 |
| `gold.fato_rede` | min/max | assortatividade_stance | NULL / NULL (dominios 15/09) | NULL / NULL | NULL / NULL | NULL / NULL |
| `gold.fato_rede` | min/max | centralizacao_grau_entrada | 0.160303 / 1.0 (dominios 15/09) | 0.160303 / 1.0 | 0.160303 / 1.0 | 0.160303 / 1.0 |
| `gold.fato_rede` | min/max | densidade | 0.001103 / 0.5 (dominios 15/09) | 0.001103 / 0.5 | 0.001103 / 0.5 | 0.001103 / 0.5 |
| `gold.fato_rede` | min/max | gini_mencoes_recebidas | 0.5 / 0.96795 (dominios 15/09) | 0.5 / 0.96795 | 0.5 / 0.96795 | 0.5 / 0.96795 |
| `gold.fato_rede` | min/max | hhi_mencoes_recebidas | 0.0295 / 1.0 (dominios 15/09) | 0.0295 / 1.0 | 0.0295 / 1.0 | 0.0295 / 1.0 |
| `gold.fato_rede` | min/max | isolamento_alvo | 0.126667 / 1.0 (dominios 15/09) | 0.126667 / 1.0 | 0.126667 / 1.0 | 0.126667 / 1.0 |
| `gold.fato_rede` | min/max | n_arestas | 1 / 1958 (dominios 15/09) | 1 / 1958 | 1 / 1958 | 1 / 1958 |
| `gold.fato_rede` | min/max | n_mencoes | 1 / 2552 (dominios 15/09) | 1 / 2552 | 1 / 2552 | 1 / 2552 |
| `gold.fato_rede` | min/max | n_nos | 2 / 1113 (dominios 15/09) | 2 / 1113 | 2 / 1113 | 2 / 1113 |
| `gold.fato_rede` | soma_n_arestas | arthur_do_val | 17585 (catalogo) | 17585 | 17585 | 17585 |
| `gold.fato_rede` | soma_n_arestas | monark | 4899 (catalogo) | 4899 | 4899 | 4899 |
| `gold.fato_rede` | soma_n_mencoes | arthur_do_val | 22954 (catalogo) | 22954 | 22954 | 22954 |
| `gold.fato_rede` | soma_n_mencoes | monark | 5868 (catalogo) | 5868 | 5868 | 5868 |
| `gold.fato_referencia` | linhas | - | 296 (catalogo) | 296 | 296 | 296 |
| `gold.fato_referencia` | min/max | n_autores_distintos | 1 / 357 (dominios 15/09) | 1 / 357 | 1 / 357 | 1 / 357 |
| `gold.fato_referencia` | min/max | n_postagens | 1 / 372 (dominios 15/09) | 1 / 372 | 1 / 372 | 1 / 372 |
| `gold.fato_referencia` | soma_n_postagens | arthur_do_val | 13893 (catalogo) | 13893 | 13893 | 13893 |
| `gold.fato_referencia` | soma_n_postagens | monark | 4803 (catalogo) | 4803 | 4803 | 4803 |
| `gold.grafo_arestas` | alvo_como_origem | - | 0 (catalogo) | 0 | 0 | 0 |
| `gold.grafo_arestas` | linhas | - | 22484 (novo 16/09) | 22484 | 22484 | 22484 |
| `gold.grafo_arestas` | min/max | peso | 1 / 46 (dominios 15/09) | 1 / 46 | 1 / 46 | 1 / 46 |
| `gold.grafo_arestas` | pares_linhas | arthur_do_val | 17585 (catalogo) | 17585 | 17585 | 17585 |
| `gold.grafo_arestas` | pares_linhas | monark | 4899 (catalogo) | 4899 | 4899 | 4899 |
| `gold.grafo_arestas` | soma_peso | arthur_do_val | 22954 (catalogo) | 22954 | 22954 | 22954 |
| `gold.grafo_arestas` | soma_peso | monark | 5868 (catalogo) | 5868 | 5868 | 5868 |
| `gold.papel_narrativo_v0` | linhas | - | 40 (catalogo) | 40 | 40 | 40 |
| `gold.papel_narrativo_v0` | min/max | decidido_em | 2026-09-08 / 2026-09-08 (dominios 15/09) | 2026-09-08 / 2026-09-08 | 2026-09-08 / 2026-09-08 | 2026-09-08 / 2026-09-08 |
| `gold.papel_narrativo_v0` | min/max | dia_virada | 1 / 4 (dominios 15/09) | 1 / 4 | 1 / 4 | 1 / 4 |
| `silver.captura` | linhas | - | 18696 (diario 06/09) | 18696 | 18696 | 18696 |
| `silver.captura` | linhas_por_caso | arthur_do_val | 13893 (diario 05/09) | 13893 | 13893 | 13893 |
| `silver.captura` | linhas_por_caso | monark | 4803 (diario 05/09) | 4803 | 4803 | 4803 |
| `silver.classificacao` | linhas | - | 18696 (diario 06/09) | 18696 | 18696 | 18696 |
| `silver.classificacao` | linhas_por_versao | hf@a483947 | 18696 (diario 13/09) | 18696 | 18696 | 18696 |
| `silver.classificacao` | rotulo_por_caso | arthur_do_val / acusador | 8290 (novo 16/09 (percentuais na Qualidade)) | 8290 | 8290 | 8290 |
| `silver.classificacao` | rotulo_por_caso | arthur_do_val / defensor | 2155 (novo 16/09 (percentuais na Qualidade)) | 2155 | 2155 | 2155 |
| `silver.classificacao` | rotulo_por_caso | arthur_do_val / neutro | 3448 (novo 16/09 (percentuais na Qualidade)) | 3448 | 3448 | 3448 |
| `silver.classificacao` | rotulo_por_caso | monark / acusador | 1930 (novo 16/09 (percentuais na Qualidade)) | 1930 | 1930 | 1930 |
| `silver.classificacao` | rotulo_por_caso | monark / defensor | 1980 (novo 16/09 (percentuais na Qualidade)) | 1980 | 1980 | 1980 |
| `silver.classificacao` | rotulo_por_caso | monark / neutro | 893 (novo 16/09 (percentuais na Qualidade)) | 893 | 893 | 893 |
| `silver.conta` | linhas | - | 16758 (diario 06/09) | 16758 | 16758 | 16758 |
| `silver.mencao` | linhas | - | 28822 (diario 06/09) | 28822 | 28822 | 28822 |
| `silver.mencao` | linhas_por_caso | arthur_do_val | 22954 (diario 05/09) | 22954 | 22954 | 22954 |
| `silver.mencao` | linhas_por_caso | monark | 5868 (diario 05/09) | 5868 | 5868 | 5868 |
| `silver.postagem` | linhas | - | 18696 (diario 06/09) | 18696 | 18696 | 18696 |
| `silver.postagem` | linhas_por_caso | arthur_do_val | 13893 (diario 05/09) | 13893 | 13893 | 13893 |
| `silver.postagem` | linhas_por_caso | monark | 4803 (diario 05/09) | 4803 | 4803 | 4803 |
| `silver.postagem_hashtag` | linhas | - | 2172 (diario 06/09) | 2172 | 2172 | 2172 |
| `silver.qc_resultado` | linhas | - | 48 (diario 06/09) | 48 | 75 | 96 |
| `silver.qc_resultado` | linhas_por_caso_versao | arthur_do_val / v0.2.0-dev | 23 (novo 16/09 (ver nota 2)) | 23 | 23 | 23 |
| `silver.qc_resultado` | linhas_por_caso_versao | arthur_do_val / v1.0 | — (linha nova: chave de versão criada pela reexecução) | — | 2 | 23 |
| `silver.qc_resultado` | linhas_por_caso_versao | monark / v0.2.0-dev | 25 (novo 16/09 (ver nota 2)) | 25 | 25 | 25 |
| `silver.qc_resultado` | linhas_por_caso_versao | monark / v1.0 | — (linha nova: chave de versão criada pela reexecução) | — | 25 | 25 |
| `silver.v_qc_portao` | bloqueios/alertas/pode_promover | arthur_do_val / v0.2.0-dev | 0 / 2 / true (diario 06/09) | 0 / 2 / true | 0 / 2 / true | 0 / 2 / true |
| `silver.v_qc_portao` | bloqueios/alertas/pode_promover | arthur_do_val / v1.0 | — (linha nova: chave de versão criada pela reexecução) | — | 0 / 0 / true | 0 / 2 / true |
| `silver.v_qc_portao` | bloqueios/alertas/pode_promover | monark / v0.2.0-dev | 0 / 0 / true (diario 06/09) | 0 / 0 / true | 0 / 0 / true | 0 / 0 / true |
| `silver.v_qc_portao` | bloqueios/alertas/pode_promover | monark / v1.0 | — (linha nova: chave de versão criada pela reexecução) | — | 1 / 0 / false | 0 / 0 / true |

## Notas

1. Conferência de 16/09 (antes): todas as linhas com fonte documental bateram (somas 4 803 / 13 893, 5 868 / 22 954, 4 899 / 17 585; alvo nunca origem; `pode_promover = true` nos dois casos; 27 domínios iguais aos de 15/09; rótulos coerentes com os percentuais da seção 5 do README).
2. `qc_resultado`: 25 linhas para `monark` e 23 para `arthur_do_val` por versão = 21 indicadores do portão + cobertura e neutros da classificação (os dois casos) + acurácia no teste e reprodução do oráculo da limpeza (só Monark, cujo gabarito existe). A seção 5.0 do README dizia "24 avaliados" para o Arthur do Val; corrigido para 23 em 17/09. Após a reexecução: 48 linhas `v0.2.0-dev` (primeira carga, preservadas) + 48 `v1.0` = 96.
3. Depois do Monark, `arthur_do_val / v1.0` tinha 2 linhas (só as da classificação, que não depende de caso) e `monark / v1.0` estava com `pode_promover = false` por um defeito do Teste B do `00_validacao_limpeza_texto` (string não raw; o mesmo defeito 4 de 06/09), corrigido antes da execução do Arthur, que regravou o indicador com 600/600. Registrado no diário.
4. `fato_atividade` (222 linhas) e `grafo_arestas` (22 484 linhas) não tinham contagem de linhas declarada; 22 484 = 4 899 + 17 585, a soma de `n_arestas` da `fato_rede`.
5. A `versao_classificacao` (`hf@a483947`) não mudou: na reexecução o classificador encontrou zero postagens sem rótulo e não gravou nada (`evidencias/bloco5/classificacao_reexecucao_zero_pendentes.png`).
