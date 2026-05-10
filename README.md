# Big Data — Análise de Crimes em Chicago

Projeto da disciplina de Big Data. Pipeline completo com Apache Spark para análise e predição de prisões em crimes registrados na cidade de Chicago entre 2001 e 2017.

**Problema:** Classificação binária — dado um registro de crime, o modelo prevê se resultou em prisão (`Arrest = 0` ou `1`).

---

## Dataset

**Fonte:** [Chicago Crime Dataset — Kaggle](https://www.kaggle.com/datasets/chicago/chicago-crime)

| Arquivo | Período | Tamanho |
|---|---|---|
| Chicago_Crimes_2001_to_2004.csv | 2001–2004 | 453 MB |
| Chicago_Crimes_2005_to_2007.csv | 2005–2007 | 449 MB |
| Chicago_Crimes_2008_to_2011.csv | 2008–2011 | 646 MB |
| Chicago_Crimes_2012_to_2017.csv | 2012–2017 | 350 MB |

**Total: ~1,9 GB de CSVs | 7.941.286 registros | 23 colunas**

Baixar do Kaggle e colocar os 4 arquivos dentro de `archive/` antes de rodar o notebook. A pasta não é versionada por conta do tamanho.

---

## Estrutura do Repositório

```
big-data/
├── docker/
│   └── docker-compose.yml       # Cluster Spark: master + 2 workers + JupyterLab
├── notebooks/
│   └── projeto_big_data.ipynb   # Notebook principal (Partes 1, 2 e 3)
├── slides_apresentacao.html     # Slides da apresentação final (reveal.js)
├── slides_apresentacao.pdf      # Slides em PDF
└── arquivos/                    # Logs e gráficos das execuções de teste
```

---

## Como Rodar

### Pré-requisitos

- Docker e Docker Compose instalados
- Dataset baixado e colocado em `archive/`

### 1. Subir o cluster

```bash
cd docker
docker compose up -d
```

Isso sobe 4 containers: `spark-master`, `spark-worker-1`, `spark-worker-2` e `jupyter`.

### 2. Pegar o token do Jupyter

```bash
docker exec jupyter jupyter server list
```

Copie a URL com o token e acesse no navegador (porta 8888).

### 3. Rodar o notebook

Dentro do JupyterLab, abra `work/notebooks/projeto_big_data.ipynb` e execute as células em ordem. O notebook está dividido em 3 partes — cada uma pode ser rodada de forma independente se os arquivos Parquet já existirem.

**Tempo estimado de execução completa:** 25–40 minutos (dependendo da máquina).

### 4. Interfaces disponíveis

| Interface | URL |
|---|---|
| JupyterLab | http://localhost:8888 |
| Spark Master UI | http://localhost:8080 |
| Spark Jobs (durante execução) | http://localhost:4040 |

### 5. Parar o cluster

```bash
docker compose down
```

---

## Estrutura do Cluster Spark

| Container | Função | Recursos |
|---|---|---|
| `spark-master` | Gerencia distribuição de tarefas | — |
| `spark-worker-1` | Processa dados | 2 CPUs / 2 GB RAM |
| `spark-worker-2` | Processa dados | 2 CPUs / 2 GB RAM |
| `jupyter` | Driver PySpark + JupyterLab | — |

Todos os containers usam a imagem `jupyter/pyspark-notebook:spark-3.5.0`, garantindo compatibilidade de versão entre driver e workers.

---

## O Notebook em Detalhe

### Parte 1 — Ingestão e Armazenamento

- Lê os 4 CSVs (`~1,9 GB`) com `spark.read.csv()`
- Converte para **Parquet com compressão Snappy** em `data_parquet/crimes_raw`
- Resultado: 551 MB (70% menor que os CSVs originais), leitura colunar eficiente

### Parte 2 — ETL e EDA

**Limpeza dos dados:**
- Remove linhas com nulos nas colunas críticas (`Primary Type`, `District`, `Arrest`, lat/lon)
- Filtra coordenadas `(0, 0)` — registros sem geolocalização válida
- Resultado após limpeza: 7.834.133 registros

**Feature Engineering:**
- Extrai `Hour`, `DayOfWeek` e `Month` do campo `Date` (timestamp formato Chicago: `MM/dd/yyyy hh:mm:ss a`)
- Converte `Arrest` de string `"True"/"False"` para inteiro `0/1`
- Converte `Domestic` para inteiro; garante tipos numéricos em lat/lon, `Beat`, `District`, `Ward`, `Community Area`

**Análise Exploratória (Spark SQL):**
- Top 10 tipos de crime por volume e taxa de prisão
- Taxa de prisão por hora do dia
- Distritos com maior volume e menor taxa de prisão (hotspots)

**Balanceamento:**
- Dataset original: 71,8% sem prisão / 28,2% com prisão
- Undersampling da classe majoritária (`Arrest=0`) para equilibrar 50/50
- Dataset balanceado: 4.037.330 registros

### Parte 3 — Machine Learning

Pipeline Spark MLlib: `StringIndexer → OneHotEncoder → VectorAssembler → Modelo`

- **StringIndexer:** converte colunas categóricas (`Primary Type`, `Location Description`, etc.) em índices numéricos
- **OneHotEncoder:** transforma índices em vetores binários esparsos
- **VectorAssembler:** concatena todas as features num único vetor (206 dimensões)

Split: 80% treino / 20% teste (estratificado por `Arrest_label`).

**Modelos treinados:**

| Modelo | Configuração |
|---|---|
| Logistic Regression | `maxIter=10`, baseline linear |
| Decision Tree | `maxDepth=5` |
| MLP Neural Network | camadas `[206, 12, 8, 2]`, `maxIter=50`, treino em amostra de 10% |

**Avaliação:** Accuracy, Weighted Precision, Weighted Recall e F1-Score via `MulticlassClassificationEvaluator`.

---

## Resultados

| Modelo | Accuracy | Precision | Recall | F1-Score |
|---|---|---|---|---|
| Logistic Regression | **0.7992** | 0.8192 | 0.7992 | **0.7959** |
| Decision Tree | 0.7677 | **0.8253** | 0.7677 | 0.7568 |
| MLP Neural Network | 0.7463 | 0.7561 | 0.7463 | 0.7376 |

A Regressão Logística superou os outros dois modelos em Accuracy e F1-Score, o que indica que a separação das classes tem componente linear forte — provavelmente influenciada pela variável `Primary Type` (NARCOTICS tem 99% de taxa de prisão e domina a predição).

---

## Stack

- Apache Spark 3.5.0 + PySpark + Spark MLlib
- Docker + Docker Compose
- JupyterLab
- Python: `matplotlib`, `seaborn`, `numpy`, `pandas`
