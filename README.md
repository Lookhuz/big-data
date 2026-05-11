# Big Data: Análise de Crimes em Chicago

Projeto da disciplina de Big Data (CP905TIN3, 9º Semestre 2026/1).
Pipeline completo com Apache Spark para análise e predição de prisões em crimes registrados na cidade de Chicago entre 2001 e 2017.

**Alunos:** Raphael Nobuyuki Haga Okuyama (RA 222808) e Lucas de Moraes Silveira (RA 211668)
**Professor:** Fabrício Torquato

**Problema:** Classificação binária. Dado um registro de crime, o modelo prevê se resultou em prisão (`Arrest_label = 0` ou `1`).

---

## Dataset

**Fonte:** [Chicago Crime Dataset — Kaggle](https://www.kaggle.com/datasets/currie32/crimes-in-chicago)

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
│   └── docker-compose.yml        # Cluster Spark: master + 2 workers + JupyterLab
├── notebooks/
│   └── projeto_big_data.ipynb    # Notebook principal (Partes 1 a 4)
├── slides_apresentacao.html      # Slides da apresentação final (reveal.js)
├── slides_apresentacao.pdf       # Slides em PDF (16:9)
├── slides_apresentacao.pptx      # Slides em PowerPoint
└── arquivos/                     # Logs e gráficos das execuções
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

Sobe 4 containers: `spark-master`, `spark-worker-1`, `spark-worker-2` e `jupyter`.

### 2. Pegar o token do Jupyter

```bash
docker exec jupyter jupyter server list
```

Copie a URL com o token e acesse no navegador (porta 8888).

### 3. Rodar o notebook

Dentro do JupyterLab, abra `work/notebooks/projeto_big_data.ipynb` e execute todas as células em ordem com **Kernel limpo** (Kernel > Restart Kernel and Run All Cells).

O notebook tem 4 partes. As partes 2, 3 e 4 leem dos arquivos Parquet gerados anteriormente, então não é necessário re-rodar a Parte 1 se o Parquet já existir.

**Tempo estimado de execução completa:** 40–60 minutos.

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
| `spark-master` | Gerencia distribuição de tarefas | 7077 (Spark), 8080 (UI) |
| `spark-worker-1` | Processa dados | 2 CPUs / 2 GB RAM |
| `spark-worker-2` | Processa dados | 2 CPUs / 2 GB RAM |
| `jupyter` | Driver PySpark + JupyterLab | 8888 (Lab), 4040 (Spark UI) |

Todos os containers usam a imagem `jupyter/pyspark-notebook:spark-3.5.0`.

---

## O Notebook em Detalhe

### Parte 1: Ingestão e Armazenamento

- Lê os 4 CSVs (~1,9 GB) com `spark.read.csv()`
- Converte para **Parquet com compressão Snappy** em `data_parquet/crimes_raw`
- Resultado: 551 MB (70% menor), leitura colunar eficiente nas etapas seguintes

### Parte 2: ETL e EDA

**Limpeza:**
- Remove linhas com nulos nas colunas críticas (`Primary Type`, `District`, `Arrest`, lat/lon)
- Filtra coordenadas `(0, 0)` sem geolocalização válida
- Remove crimes do tipo **NARCOTICS** (99,21% de taxa de prisão, criava atalho trivial que distorcia feature importance e métricas)
- Resultado após limpeza: 6.962.654 registros

**Feature Engineering:**
- Extrai `Hour`, `DayOfWeek` e `Month` do campo `Date`
- Converte `Arrest` de string `"True"/"False"` para inteiro `0/1`
- Converte `Domestic`, lat/lon, `Beat`, `District` e `Community Area` para tipos numéricos

**Análise Exploratória (Spark SQL):**
- Top 15 tipos de crime por volume e taxa de prisão
- Taxa de prisão por hora do dia (pico às 20h: ~22,7%)
- Taxa de prisão por distrito (Distrito 1: 26,7% / Distrito 22: 14,8%)
- Distribuição geográfica dos crimes em Chicago
- Tendência anual 2001–2017

**Balanceamento:**
- Dataset após feature engineering: 6.349.252 registros
- Desbalanceamento original: 80,7% sem prisão / 19,3% com prisão (razão 1:4)
- Undersampling da classe majoritária para equilibrar 50/50
- Dataset balanceado: 2.457.954 registros

### Parte 3: Machine Learning

Pipeline Spark MLlib: `StringIndexer > OneHotEncoder > VectorAssembler > Modelo`

- **StringIndexer:** converte `Primary Type` e `Location Description` em índices
- **OneHotEncoder:** transforma índices em vetores binários esparsos
- **VectorAssembler:** concatena features num único vetor de **203 dimensões**

Split: 80% treino (1.966.338 linhas) / 20% teste (491.616 linhas).

**Modelos treinados:**

| Modelo | Configuração |
|---|---|
| Logistic Regression | `maxIter=10`, baseline linear |
| Decision Tree | `maxDepth=5` |
| MLP Neural Network | camadas `[203, 12, 8, 2]`, `maxIter=20`, treino em 10% do conjunto |

**Avaliação:** Accuracy, Weighted Precision, Weighted Recall e F1-Score via `MulticlassClassificationEvaluator`. Curvas ROC e AUC via `sklearn.metrics`.

### Parte 4: Visualizações

Gera e salva em `data_parquet/` os gráficos para a apresentação:
- Qualidade dos dados (nulos e cardinalidade por coluna)
- Distribuição por tipo de crime e taxa de prisão
- Taxa de prisão por hora e por distrito
- Mapa geográfico dos crimes (hexbin + scatter)
- Antes/depois do balanceamento
- Comparação de métricas dos 3 modelos
- Confusion matrix dos 3 modelos
- Feature importance da Decision Tree
- Tendência anual 2001–2017
- Curvas ROC (LR vs DT)
- Matriz de correlação de Pearson

---

## Resultados

| Modelo | Accuracy | Precision | Recall | F1-Score | AUC |
|---|---|---|---|---|---|
| Logistic Regression | **0,7181** | 0,7182 | 0,7181 | **0,7181** | **0,8058** |
| Decision Tree (d=5) | 0,6100 | **0,7501** | 0,6100 | 0,5462 | 0,6454 |
| MLP Neural Net | 0,5253 | 0,5273 | 0,5253 | 0,5154 | — |

A Regressão Logística superou os outros modelos em accuracy, F1 e AUC. A Decision Tree tem precision alta mas 38% de falsos negativos, reflexo da profundidade limitada (maxDepth=5 cria apenas 31 folhas para 203 features). O MLP foi treinado em apenas 10% dos dados por limitação de RAM, o que compromete seu desempenho.

---

## Decisões Técnicas Relevantes

**Por que remover NARCOTICS?**
Com 99,21% de taxa de prisão, o tipo NARCOTICS funcionava como um atalho trivial: qualquer modelo que identificasse esse tipo acertava automaticamente. A feature importance da Decision Tree apontava quase 70% do peso para esse único tipo. Após a remoção, as métricas caíram (LR: 79,9% > 71,8%), mas o modelo passou a capturar padrões reais.

**Por que Undersampling e não SMOTE?**
Com 1,2 milhão de amostras reais da classe minoritária, não havia necessidade de sintetizar dados. SMOTE também não está disponível no Spark MLlib e tem desempenho ruim em espaços esparsos de alta dimensão (OHE com 203 features).

**Por que a LR superou a Decision Tree?**
maxDepth=5 cria apenas 31 folhas para um espaço de 203 features, o que é muito restritivo. A LR aproveita todas as 203 dimensões continuamente. Com maxDepth=10–15 a DT provavelmente melhoraria, mas o tempo de treino inviabilizaria no hardware disponível.

---

## Stack

- Apache Spark 3.5.0 + PySpark + Spark MLlib
- Docker + Docker Compose
- JupyterLab
- Python: `matplotlib`, `seaborn`, `numpy`, `pandas`, `scikit-learn`
