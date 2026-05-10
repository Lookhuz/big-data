# Projeto Big Data — Crimes em Chicago

## Visão Geral

Pipeline completo de Big Data com Apache Spark: Ingestão CSV → ETL → EDA → Machine Learning.
**Target:** `Arrest` — classificação binária para predizer se um crime resultará em prisão.
Dataset: ~7,9 milhões de registros, 23 colunas.

---

## Ambiente de Execução

O projeto roda **obrigatoriamente dentro do Docker**. Nunca abrir o notebook diretamente no VS Code ou Finder — o SparkSession trava silenciosamente tentando conectar em `spark://spark-master:7077`.

### Subir o ambiente

```bash
cd docker/
docker compose up
```

### Acessar o Jupyter

```bash
docker exec jupyter jupyter server list
# Pega a URL com token e acessa no browser: http://127.0.0.1:8888/lab?token=...
```

### Estrutura dos containers

| Container | Função | Porta |
|---|---|---|
| `spark-master` | Master do cluster Spark | 7077, 8080 |
| `spark-worker-1` | Worker 1 (2 cores, 2g RAM) | — |
| `spark-worker-2` | Worker 2 (2 cores, 2g RAM) | — |
| `jupyter` | JupyterLab + PySpark driver | 8888, 4040 |

- Spark UI (jobs em tempo real): `http://localhost:4040`
- Spark Master UI: `http://localhost:8080`

### Volumes mapeados

```
../archive        → /home/jovyan/work/archive         (CSVs originais)
../data_parquet   → /home/jovyan/work/data_parquet    (Parquet + modelos salvos)
../notebooks      → /home/jovyan/work/notebooks       (notebook principal)
```

---

## Estrutura do Notebook (`notebooks/projeto_big_data.ipynb`)

### Parte 1 — Ingestão
- Lê todos os CSVs de `/home/jovyan/work/archive/`
- Converte para Parquet em `/home/jovyan/work/data_parquet/crimes_raw`

### Parte 2 — ETL / EDA
- Limpeza: remove nulos nas colunas críticas, filtra coordenadas (0,0)
- Feature engineering: `Hour`, `DayOfWeek`, `Month` extraídos do timestamp
- `Arrest_label`: converte string "True"/"False" para inteiro 0/1
- EDA via Spark SQL: top crimes, taxa de prisão por hora, distritos críticos
- Balanceamento: undersampling da classe majoritária (`Arrest=0`) para equilibrar com `Arrest=1`

### Parte 3 — Machine Learning
Pipeline padrão: `StringIndexer → OneHotEncoder → VectorAssembler → Modelo`

| Modelo | Config |
|---|---|
| Logistic Regression | `maxIter=10` (baseline) |
| Decision Tree | `maxDepth=5` |
| MLP Neural Network | camadas `[INPUT_SIZE, 12, 8, 2]`, `maxIter=50` |

Avaliação: Accuracy, Weighted Precision, Weighted Recall, F1-Score via `MulticlassClassificationEvaluator`.

Saída salva em:
- Modelo: `/home/jovyan/work/data_parquet/model_decision_tree`
- Dataset balanceado: `/home/jovyan/work/data_parquet/crimes_balanced`

---

## Próximos Passos — Apresentação Final

Após o código estar funcionando e testado, montar uma **apresentação completa para o professor** com:

### Gráficos planejados
- Distribuição de crimes por tipo (bar chart)
- Taxa de prisão por hora do dia (line chart)
- Taxa de prisão por distrito (heatmap ou bar)
- Distribuição geográfica dos crimes (scatter map por lat/lon)
- Desbalanceamento antes/depois do undersampling (pie ou bar)
- Comparação de métricas dos 3 modelos (grouped bar: Accuracy, Precision, Recall, F1)
- Confusion matrix de cada modelo
- Feature importance do Decision Tree
- Curva de aprendizado (se viável)

### Conteúdo da apresentação
- Cada decisão técnica tomada (ex: por que Parquet, por que undersampling, por que esses modelos)
- Falhas encontradas (do dataset, da lógica, da execução)
- Melhorias implementadas ao longo do projeto
- Melhorias que gostaríamos de ter feito mas não conseguimos
- Limitações honestas do modelo e do dataset
- Conclusões e próximos passos

### Objetivo
Impressionar o professor mostrando raciocínio crítico além dos resultados — expor o processo de decisão, não só os números finais.

---

## Resultados da Primeira Execução Completa

### Números do pipeline
| Etapa | Linhas |
|---|---|
| CSV raw | 7.941.286 |
| Após limpeza (dropna + filtro lat/lon) | 7.834.133 |
| Após seleção de features + dropna | 7.145.306 |
| Após undersampling (balanceado) | 4.037.330 |
| Treino (80%) | 3.230.261 |
| Teste (20%) | 807.069 |
| Tamanho do vetor de features | 206 |

### Desbalanceamento original
- `Arrest=1`: 2.018.002 (28.2%)
- `Arrest=0`: 5.127.304 (71.8%)
- Razão: 1:2

### EDA — Top 10 crimes (por volume)
| Primary Type | Total | Taxa de Prisão |
|---|---|---|
| THEFT | 1.476.390 | 11.69% |
| BATTERY | 1.298.245 | 22.99% |
| CRIMINAL DAMAGE | 835.316 | 6.99% |
| NARCOTICS | 796.054 | **99.21%** |
| OTHER OFFENSE | 440.885 | 16.95% |
| ASSAULT | 432.905 | 23.77% |
| BURGLARY | 430.571 | 5.70% |
| MOTOR VEHICLE THEFT | 328.768 | 8.53% |
| ROBBERY | 271.450 | 9.89% |
| DECEPTIVE PRACTICE | 247.715 | 18.31% |

> NARCOTICS tem 99.21% de taxa de prisão — isso vai distorcer os modelos. Ponto importante a discutir na apresentação.

### EDA — Taxa de prisão por hora
- Pico noturno (19h–21h): ~35% de taxa de prisão
- Menor taxa: madrugada/manhã cedo (5h–8h): ~18%

### EDA — Distritos com menor taxa de prisão (hotspots críticos)
- Distrito 16: 19.22% (236.834 crimes)
- Distrito 14: 22.07% (278.375 crimes)
- Distrito 19: 22.19% (314.362 crimes)

### Resultados dos Modelos (primeira execução)
| Modelo | Accuracy | Precision | Recall | F1-Score |
|---|---|---|---|---|
| Logistic Regression | **0.7992** | 0.8192 | 0.7992 | **0.7959** |
| Decision Tree (maxDepth=5) | 0.7677 | **0.8253** | 0.7677 | 0.7568 |
| MLP Neural Network | — | — | — | — |

> MLP ainda não terminou na primeira execução (travou por tempo/memória). Resultado pendente.

**Observação:** LR superou DT em accuracy e F1 — baseline linear venceu a árvore com maxDepth=5. Possível causa: maxDepth muito conservador. Vale testar `maxDepth=10` ou `maxDepth=15`.

---

## Observações Técnicas

- `spark-worker-1` e `spark-worker-2` aparecem como `unhealthy` no `docker ps` mas funcionam normalmente — healthcheck mal configurado, não indica problema real.
- Células com `count()` e treino do MLP demoram bastante (~10–20 min total) com 7,9M linhas. Normal.
- O `[*]` nas células significa que estão na fila de execução, não que travaram.
