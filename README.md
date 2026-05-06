# Big Data — Crimes em Chicago

Pipeline completo de Big Data com Apache Spark para análise e predição de prisões em crimes registrados em Chicago entre 2001 e 2017.

**Tarefa:** Classificação binária — predizer se um crime resultará em prisão (`Arrest = true/false`).

---

## Estrutura do Projeto

```
big-data/
├── docker/
│   └── docker-compose.yml       # Cluster Spark: 1 master + 2 workers + Jupyter Lab
└── notebooks/
    └── projeto_big_data.ipynb   # Notebook principal com as 3 partes do projeto
```

---

## Como Rodar

### Pré-requisitos
- Docker e Docker Compose instalados
- Dataset em `archive/` (não versionado — ver seção Dataset abaixo)

### Subir o cluster

```bash
cd docker
docker compose up -d
```

### Acessar

| Interface | URL |
|---|---|
| Jupyter Lab | http://localhost:8888 |
| Spark Master UI | http://localhost:8080 |

Abra `work/notebooks/projeto_big_data.ipynb` no Jupyter e execute as células em ordem.

### Parar o cluster

```bash
docker compose down
```

---

## As 3 Partes do Projeto

### Parte 1 — Ambiente de Big Data

Cluster Spark simulado via Docker Compose usando `jupyter/pyspark-notebook:spark-3.5.0` em todos os containers, garantindo compatibilidade de versão entre driver e workers.

| Container | Função | Recursos |
|---|---|---|
| `spark-master` | Gerencia o cluster | 1 CPU / 1GB RAM |
| `spark-worker-1` | Processa dados | 2 CPUs / 2GB RAM |
| `spark-worker-2` | Processa dados | 2 CPUs / 2GB RAM |
| `jupyter` | Executa os notebooks | 2 CPUs / 3GB RAM |

### Parte 2 — ETL e Análise Exploratória

- Leitura dos CSVs com `spark.read.csv` e conversão para Parquet
- Limpeza: remoção de nulos e coordenadas inválidas
- Feature engineering: extração de `Hour`, `DayOfWeek` e `Month` do timestamp
- EDA com 3 consultas Spark SQL (crimes por tipo, por hora e por distrito)
- Balanceamento com **Undersampling** da classe majoritária (Arrest=0)

### Parte 3 — Machine Learning

Pipeline Spark ML: `StringIndexer → OneHotEncoder → VectorAssembler → Modelo`

| Modelo | Accuracy | Precision | Recall | F1-Score |
|---|---|---|---|---|
| Logistic Regression | 0.7853 | 0.7955 | 0.7853 | 0.7834 |
| Decision Tree | 0.7435 | 0.8157 | 0.7435 | 0.7280 |
| MLP Neural Network | 0.5083 | 0.5186 | 0.5083 | 0.4325 |

---

## Dataset

**Fonte:** [Chicago Crime Dataset — Kaggle](https://www.kaggle.com/datasets/chicago/chicago-crime)

| Arquivo | Período | Tamanho |
|---|---|---|
| Chicago_Crimes_2001_to_2004.csv | 2001–2004 | 453 MB |
| Chicago_Crimes_2005_to_2007.csv | 2005–2007 | 449 MB |
| Chicago_Crimes_2008_to_2011.csv | 2008–2011 | 646 MB |
| Chicago_Crimes_2012_to_2017.csv | 2012–2017 | 350 MB |

Total: ~1.9 GB | ~6 milhões de registros

> Baixar do Kaggle e colocar em `archive/` antes de rodar o notebook. A pasta não é versionada.

---

## Stack

- Apache Spark 3.5.0
- PySpark + Spark MLlib
- Docker + Docker Compose
- Jupyter Lab
