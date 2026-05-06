# Big Data — Crimes em Chicago

Pipeline completo de Big Data com Apache Spark para análise e predição de prisões em crimes registrados em Chicago entre 2001 e 2017.

**Tarefa:** Classificação binária — predizer se um crime resultará em prisão (`Arrest = true/false`).

---

## Estrutura do Projeto

```
crimes-in-la/
├── docker/
│   └── docker-compose.yml       # Cluster Spark: 1 master + 2 workers + Jupyter Lab
├── notebooks/
│   └── projeto_big_data.ipynb   # Notebook principal com as 3 partes do projeto
├── archive/                     # CSVs originais (~1.9GB) — não versionados
├── data_parquet/                # Dados processados e modelos salvos — não versionados
├── aula_06_parte_2.ipynb        # Notebook de referência fornecido pelo professor
└── .gitignore
```

---

## As 3 Partes do Projeto

### Parte 1 — Ambiente de Big Data (Docker)

Cluster Spark simulado via Docker Compose com a imagem `jupyter/pyspark-notebook:spark-3.5.0` em todos os containers, garantindo a mesma versão de Python (3.11) e Spark (3.5.0) entre driver e workers.

| Container | Função | Porta |
|---|---|---|
| `spark-master` | Gerencia o cluster | 8080 (UI), 7077 (cluster) |
| `spark-worker-1` | Processa dados | — |
| `spark-worker-2` | Processa dados | — |
| `jupyter` | Executa os notebooks | 8888 |

**Subir o cluster:**
```bash
cd docker
docker compose up -d
```

**Acessar:**
- Jupyter Lab: `http://localhost:8888`
- Spark UI: `http://localhost:8080`

**Parar:**
```bash
docker compose down
```

---

### Parte 2 — ETL e Análise Exploratória

Executada no notebook `notebooks/projeto_big_data.ipynb`.

**Ingestão:**
- Leitura dos CSVs com `spark.read.csv` e schema inferido
- Conversão para Parquet antes da análise (formato colunar, reduz I/O em ~70%)

**Limpeza:**
- Remoção de nulos nas colunas críticas (`Date`, `Primary Type`, `Arrest`, coordenadas)
- Filtro de registros com coordenadas inválidas (lat/lon = 0)

**Feature Engineering:**
- `Hour` — hora do crime extraída do timestamp
- `DayOfWeek` — dia da semana
- `Month` — mês do ano

**Análise com Spark SQL (3 queries):**
1. Top 10 tipos de crime por volume e taxa de prisão
2. Distribuição de crimes e taxa de prisão por hora do dia
3. Distritos com maior volume e menor taxa de prisão (hotspots críticos)

**Balanceamento:**
- Distribuição original: Arrest=0 (~74%) vs Arrest=1 (~26%)
- Técnica aplicada: **Undersampling** da classe majoritária
- Resultado: dataset balanceado com ~742 mil registros

---

### Parte 3 — Machine Learning

Pipeline Spark ML encapsulado com `pyspark.ml.Pipeline`:

```
StringIndexer → OneHotEncoder → VectorAssembler → Modelo
```

**Features categóricas** (encoding):
- `Primary Type` e `Location Description` → StringIndexer + OneHotEncoder

**Features numéricas:**
- `Hour`, `DayOfWeek`, `Month`, `District`, `Beat`, `Community Area`, `Domestic`, `Latitude`, `Longitude`

**Vetor de features resultante:** 171 dimensões

#### Modelos Treinados

| Modelo | Accuracy | Precision | Recall | F1-Score |
|---|---|---|---|---|
| Logistic Regression | 0.7853 | 0.7955 | 0.7853 | 0.7834 |
| Decision Tree | 0.7435 | 0.8157 | 0.7435 | 0.7280 |
| MLP Neural Network | 0.5083 | 0.5186 | 0.5083 | 0.4325 |

Avaliação via `MulticlassClassificationEvaluator` com métricas: accuracy, weighted precision, weighted recall e F1-score.

**Melhor modelo:** Logistic Regression — maior accuracy (78,5%) e F1 (78,3%).

**Persistência:**
- Modelo Decision Tree salvo em `data_parquet/model_decision_tree/`
- Dataset balanceado salvo em `data_parquet/crimes_balanced/` (formato Parquet)

---

## Dataset

**Fonte:** [Chicago Crimes — Kaggle](https://www.kaggle.com/datasets/chicago/chicago-crime)

| Arquivo | Período | Tamanho |
|---|---|---|
| Chicago_Crimes_2001_to_2004.csv | 2001–2004 | 453 MB |
| Chicago_Crimes_2005_to_2007.csv | 2005–2007 | 449 MB |
| Chicago_Crimes_2008_to_2011.csv | 2008–2011 | 646 MB |
| Chicago_Crimes_2012_to_2017.csv | 2012–2017 | 350 MB |

Total: ~1.9 GB | ~6 milhões de registros

> Os arquivos CSV não são versionados no repositório. Colocar em `archive/` antes de rodar o notebook.

---

## Stack

- **Apache Spark 3.5.0** — processamento distribuído
- **PySpark** — API Python do Spark
- **Spark MLlib** — pipeline de machine learning
- **Docker + Docker Compose** — simulação do cluster
- **Jupyter Lab** — ambiente de desenvolvimento
