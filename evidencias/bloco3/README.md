# Evidências — Bloco 3 (Gold e Catálogo), 07/09/2026

Todos os prints saem de `gold`, `pub` ou `information_schema`. Nenhum contém handle ou texto de postagem.

| arquivo | o que mostra |
|---|---|
| `01_calendario_caso_carga_inicial.png` | Primeira carga da `calendario_caso` (26 linhas) e contagem por fase — Arthur ainda sem `pico`. |
| `02_calendario_caso_arthur_antes_correcao_pico.png` | Dia a dia do Arthur antes da correção: 10 dias de `declinio` acima do volume do estopim — o pico estava ancorado em 28/02 (artefato). |
| `03_calendario_caso_por_fase_corrigida.png` | Após restringir pico e limiar a dias ≥ estopim: Arthur com `pico` em 05/03 (896) e 9 dias de `declinio`; Monark inalterado. |
| `04_dim_conta_papel_alvos.png` | Um `alvo` por caso (conta_id 5238 e 5046), `n_postagens_caso = 0`, menções recebidas 1.335 e 9.132. |
| `05_fato_atividade_por_fase_stance.png` | `fato_atividade` recarregada: postagens por fase e stance; totais 4.803 e 13.893. |
| `06_fato_rede_carga_26_linhas.png` | Carga da `fato_rede`: 26 linhas inseridas. |
| `07_fato_rede_metricas_por_dia.png` | Métricas de rede por dia (densidade, Gini, HHI, centralização, isolamento do alvo). |
| `08_fato_rede_totais_vs_silver.png` | Σ menções = 5.868 / 22.954 (= `silver.mencao`); arestas 4.899 / 17.585. |
| `09_grafo_arestas_soma_pesos_pares.png` | `grafo_arestas`: Σ peso e nº de pares idênticos à `fato_rede`. |
| `10_grafo_arestas_alvo_nunca_origem.png` | O alvo aparece 0 vezes como origem; menções ao alvo batem com a dimensão. |
| `11_pub_pseudonimo_injetivo.png` | 16.399 contas distintas → 16.399 pseudônimos distintos; 16.932 linhas caso × conta. |
| `12_catalogo_colunas_information_schema.png` | Comentários de coluna gravados no Unity Catalog (`information_schema.columns`, schema `gold`). |
| `13_catalogo_tabelas_gold_pub.png` | Comentários de tabela/view em `gold` e `pub` (`information_schema.tables`). |
