# Valores esperados do QC — oráculo de conferência

A lógica de `sql/03_bronze_to_silver.sql` foi simulada em Python contra os **corpora reais**, registro a registro, em 03/09/2026. Os números abaixo são o que a execução no Databricks **tem de reproduzir**. Divergência = bug no SQL ou dado diferente do inventariado.

| Indicador | arthur_do_val | monark | Severidade |
|---|---:|---:|---|
| `qc_linhas_bronze` | 13.906 | 5.143 | informa |
| `qc_postagens_unicas` | 13.906 | 4.803 | informa |
| `qc_duplicatas_removidas` | 0 | 340 | informa |
| `qc_ids_duplicados` | 0 | **305** | informa |
| `qc_duplicatas_divergentes` | 0 | **0** | alerta |
| `qc_id_fora_do_padrao` | 0 | 0 | **bloqueia** |
| `qc_descartadas_sem_autor` | **13** | 0 | alerta |
| `qc_pct_sem_autor` | 0,093% | 0% | **bloqueia acima de 1%** |
| `qc_data_nao_parseada` | 0 | 0 | **bloqueia** |
| `qc_fora_da_janela` | 0 | 0 | alerta |
| `qc_reply_sem_destino` | 0 | 0 | **bloqueia** |
| `qc_replies_reclassificados` | 9.893 | 2.335 | informa |
| `qc_contador_negativo` | 0 | 0 | **bloqueia** |
| `qc_texto_vazio` | 0 | 0 | alerta |
| `qc_idioma_inesperado` | 90 | 0 | alerta |
| `qc_mencoes_totais` | 23.190 | 5.932 | informa |
| `qc_pct_mencoes_com_id` | 100,0% | 100,0% | alerta |
| `qc_cv_volume_diario` | 0,313 | 0,740 | informa |
| `qc_com_stance_previa` | 0 | 0 | informa |

**Os dois casos passam em todas as verificações bloqueantes.** `pode_promover = true` para ambos.

## Linhas resultantes na camada Silver

| Tabela | arthur_do_val | monark |
|---|---:|---:|
| `conta` | 11.565 | 5.741 |
| `postagem` | 13.893 | 4.803 |
| `mencao` | 22.954 | 5.868 |
| `postagem_hashtag` | 1.034 | 1.138 |

*(As contagens de `conta` são por caso e se sobrepõem parcialmente entre os dois — a tabela é global, então o total carregado será menor que a soma.)*

## Três leituras que estes números já entregam

**A divergência histórica "305 × 340" está resolvida.** São duas contagens de coisas diferentes: **305 identificadores** aparecem mais de uma vez, produzindo **340 linhas excedentes** — alguns ids se repetem três ou mais vezes. Ambas as contagens anteriores estavam certas; mediam coisas distintas. E a divergência de *conteúdo* entre as cópias é **zero** no monark: as duplicatas são cópias idênticas, não capturas em momentos diferentes.

**O coeficiente de variação confirma que nenhum dos dois casos está no teto do coletor.** Monark 0,74 e Arthur do Val 0,313, contra **0,04** medido no corpus da Patrícia Moreira, cujo volume ficava travado em ~990 postagens/dia. O platô do Arthur entre 01 e 13/03 é forma do episódio, não limite da raspagem — a suspeita levantada no inventário se dissolve, e a verificação fica no pipeline para os próximos casos.

**Os replies reclassificados são a maior transformação isolada do pipeline.** 9.893 e 2.335 postagens que o corpus entregava como `original` e que o `tipo_ref` derivado corrige. Sem isso, 71% da rede de conversa do Arthur do Val e 49% da do Monark seriam invisíveis.

## Como conferir no Databricks

```sql
SELECT indicador, valor, severidade, aprovado
  FROM silver.qc_resultado
 WHERE caso_slug = 'arthur_do_val' AND versao_pipeline = :versao
 ORDER BY indicador;

SELECT * FROM silver.v_qc_portao;   -- pode_promover tem de ser true
```

*Oráculo gerado em 03/09/2026 pela simulação da lógica do SQL contra os arquivos brutos.*
