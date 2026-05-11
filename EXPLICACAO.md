# Explicação Completa do Projeto: Big Data com Apache Spark

**Alunos:** Raphael Nobuyuki Haga Okuyama (RA 222808) e Lucas de Moraes Silveira (RA 211668)
**Disciplina:** Big Data — CP905TIN3 — 9º Semestre 2026/1
**Professor:** Fabrício Torquato

---

## Apache Spark: O que é, como funciona e por que existe

### O problema que o Spark resolve

Antes do Spark, o principal framework de Big Data era o **Hadoop MapReduce** (2004). O MapReduce processava dados em etapas sequenciais gravando em disco entre cada etapa. Para calcular algo iterativo (como um modelo de ML que roda 50 iterações), ele gravava e relia o disco 50 vezes. Era extremamente lento.

O **Apache Spark** surgiu em 2009 no laboratório AMPLab de Berkeley para resolver exatamente isso: processar dados em **memória RAM** ao invés de disco, e fazer várias etapas seguidas sem precisar gravar o resultado intermediário. Resultado: Spark é **10x a 100x mais rápido** que Hadoop MapReduce para cargas iterativas.

Mas o principal motivo de existir é este: **um único computador não consegue processar datasets de terabytes**. O Spark distribui o processamento entre dezenas, centenas ou milhares de máquinas ao mesmo tempo, tratando tudo como se fosse uma operação única.

---

### Arquitetura do Spark: Driver, Master e Executors

O Spark funciona num modelo **mestre-escravo**:

```
┌──────────────────────────────────────────────────────────────┐
│                        Cluster Spark                         │
│                                                              │
│   ┌─────────────────────┐                                    │
│   │    DRIVER (jupyter) │  ← seu código Python roda aqui    │
│   │  SparkContext/Session│                                    │
│   │  Cria o plano DAG   │                                    │
│   └──────────┬──────────┘                                    │
│              │ envia tarefas via RPC                         │
│   ┌──────────▼──────────┐                                    │
│   │   MASTER (spark-    │  ← coordena, não processa dados    │
│   │    master:7077)     │  ← aloca workers para cada job     │
│   └──────────┬──────────┘                                    │
│              │                                               │
│    ┌─────────┴──────────┐                                    │
│    ▼                    ▼                                    │
│ ┌──────────┐     ┌──────────┐                                │
│ │EXECUTOR 1│     │EXECUTOR 2│  ← processam os dados de fato  │
│ │(worker-1)│     │(worker-2)│  ← cada um tem suas partições  │
│ └──────────┘     └──────────┘                                │
└──────────────────────────────────────────────────────────────┘
```

- **Driver**: onde seu código Python/PySpark roda. Cria o plano de execução e coordena os resultados. No projeto, é o container `jupyter`.
- **Master**: gerencia o cluster, decide quais executors recebem quais tarefas. Não processa dados. No projeto, é o container `spark-master`.
- **Executor**: processa os dados de verdade. Cada executor tem um pedaço dos dados (uma partição). No projeto, são os containers `spark-worker-1` e `spark-worker-2`.

**Analogia**: imagine uma empresa processando papelada. O driver é o gerente que planejou o trabalho. O master é o RH que alocou funcionários. Os executors são os funcionários que efetivamente trabalham.

---

### RDD: a estrutura de dados fundamental

O **RDD (Resilient Distributed Dataset)** é a estrutura de dados de baixo nível do Spark. É uma coleção de objetos distribuídos entre os executors, imutável e tolerante a falhas.

- **Resilient**: se um executor morrer, o Spark sabe recomputar aquela partição do zero
- **Distributed**: os dados estão fisicamente em máquinas diferentes
- **Dataset**: é uma coleção de dados

No projeto você vê `spark.sparkContext.parallelize([1], 1).count()` — esse é o uso direto do RDD. `parallelize` cria um RDD a partir de uma lista Python. Mas no dia-a-dia do projeto usamos **DataFrames** (a abstração de alto nível) que por baixo dos panos são RDDs com schema.

---

### DataFrame: a abstração de alto nível

O **DataFrame** do Spark é como uma tabela com colunas nomeadas e tipadas, igual ao pandas DataFrame — mas distribuído. A diferença fundamental:

| Característica | pandas DataFrame | Spark DataFrame |
|---|---|---|
| Onde os dados ficam | RAM de 1 máquina | RAM de N máquinas (particionado) |
| Tamanho limite | RAM disponível (~16-64 GB) | PB (petabytes) |
| Processamento | Serial (1 CPU) | Paralelo (todos os cores de todas as máquinas) |
| Sintaxe | `df["coluna"]` | `df.select(col("coluna"))` |
| Execução | Imediata | Lazy (só quando necessário) |

No projeto: `df_raw = spark.read.csv(...)` cria um DataFrame Spark distribuído com 7,9 milhões de linhas espalhadas pelos 2 workers.

---

### Lazy Evaluation: como o Spark realmente executa

Este é o conceito mais importante do Spark e o mais diferente do que você está acostumado.

**Em pandas**, cada linha de código executa imediatamente:
```python
df = pd.read_csv("arquivo.csv")   # executa AGORA — lê o arquivo
df = df.dropna()                   # executa AGORA — remove nulos
df = df[df["col"] > 5]            # executa AGORA — filtra linhas
```

**No Spark**, quase nada executa imediatamente. O Spark acumula as operações e monta um **plano de execução** (DAG — Directed Acyclic Graph):
```python
df = spark.read.csv("arquivo.csv")     # NÃO executa — anota "vou ler esse CSV"
df = df.dropna(...)                     # NÃO executa — anota "vou dropar nulos"
df = df.filter(col("col") > 5)         # NÃO executa — anota "vou filtrar"
df.show()                               # EXECUTA TUDO DE UMA VEZ
```

Só quando você chama uma **ação** (`.show()`, `.count()`, `.write()`, `.collect()`, `.toPandas()`) o Spark otimiza e executa todo o plano acumulado.

**Por que isso é melhor?** O otimizador (Catalyst) pode reordenar operações, pushdown filtros, eliminar colunas desnecessárias. Por exemplo: se você faz `df.select("A").filter("B > 5")`, o Catalyst pode empurrar o filtro para antes da leitura, evitando carregar linhas que serão descartadas.

---

### Transformações vs Ações

Toda operação no Spark é uma **transformação** (lazy) ou uma **ação** (executa agora):

**Transformações** (retornam um novo DataFrame, não executam):
- `df.select(...)` — seleciona colunas
- `df.filter(...)` / `df.where(...)` — filtra linhas
- `df.withColumn(...)` — adiciona/substitui coluna
- `df.dropna(...)` — remove nulos
- `df.groupBy(...)` — agrupa (ainda não agrega)
- `df.join(...)` — une dois DataFrames
- `df.sample(...)` — amostra aleatória
- `df.union(...)` — concatena dois DataFrames

**Ações** (executam o plano e retornam resultado):
- `df.count()` — conta linhas
- `df.show()` — imprime as primeiras N linhas
- `df.collect()` — traz todos os dados para o driver (cuidado com datasets grandes!)
- `df.toPandas()` — converte para pandas (traz tudo para o driver)
- `df.write.parquet(...)` — salva em disco

No projeto, cada vez que aparece `.count()` ou `.show()`, o Spark executa todas as transformações acumuladas desde a última ação. Isso explica por que algumas células demoram muito mais que outras.

---

### Partições: como os dados são distribuídos

O Spark divide o DataFrame em **partições** — pedaços menores que são processados em paralelo. Com 50 partições e 2 workers de 2 cores cada (4 cores total), o Spark pode processar 4 partições simultaneamente.

```
DataFrame com 7.9M linhas dividido em 50 partições:
┌──────────┬──────────┬──────────┬─────────────────┐
│Partição 1│Partição 2│Partição 3│ ... Partição 50 │
│ ~158k lin│ ~158k lin│ ~158k lin│   ~158k linhas  │
└──────────┴──────────┴──────────┴─────────────────┘
     ▼            ▼
  Worker 1     Worker 2    (cada um processa algumas partições em paralelo)
```

**`spark.sql.shuffle.partitions = 50`**: quando o Spark precisa redistribuir dados entre workers (após GROUP BY, JOIN, etc.), ele cria novas partições. O padrão é 200, mas para um cluster pequeno, 200 partições criam overhead de comunicação. 50 é melhor para nosso caso.

---

### DAG: o plano de execução

Cada vez que você dispara uma ação, o Spark cria um **DAG (Directed Acyclic Graph)** — um grafo direcionado sem ciclos que representa como os dados fluem e se transformam. Você pode ver o DAG visualmente acessando `http://localhost:4040` enquanto um job está rodando.

O DAG é dividido em **stages**: grupos de transformações que podem ser feitas sem mover dados entre workers. Quando é necessário mover dados (shuffle), começa um novo stage.

```
Stage 1: leitura + filter + withColumn (sem shuffle — tudo local)
    ↓ (shuffle: GROUP BY distribui dados por chave)
Stage 2: aggregation + ORDER BY
    ↓ 
Resultado
```

---

### SparkContext vs SparkSession

- **SparkContext** (o mais antigo, desde o Spark 1.x): o motor de baixo nível. Gerencia a conexão com o cluster, cria RDDs. Uma JVM pode ter no máximo um SparkContext.
- **SparkSession** (desde o Spark 2.0): a interface moderna que encapsula o SparkContext. É por ela que você cria DataFrames e escreve SQL.

No projeto:
```python
spark = SparkSession.builder...getOrCreate()    # cria a SparkSession
spark.sparkContext                              # acessa o SparkContext subjacente
```

Se o SparkContext morrer (por falta de memória ou timeout), toda a sessão precisa ser reiniciada. É por isso que o notebook tem verificações `if SparkContext._active_spark_context is None` antes do MLP.

---

### Spark SQL: executando SQL em DataFrames

O Spark tem um módulo SQL completo. Você pode registrar qualquer DataFrame como uma "tabela virtual" e consultá-la com SQL padrão:

```python
df.createOrReplaceTempView("crimes")       # registra como tabela temporária
spark.sql("SELECT * FROM crimes LIMIT 5")  # SQL puro, igual a um banco de dados
```

As tabelas temporárias vivem apenas na SparkSession atual. Se a sessão reiniciar, a tabela some e precisa ser recriada. É por isso que o notebook tem o `try/except` no início da Parte 4 para recriar a view se necessário.

---

### UDF: User Defined Functions

UDFs permitem criar funções Python customizadas que rodam dentro do Spark distribuído:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

_prob_udf = udf(lambda v: float(v[1]), DoubleType())
df.withColumn("prob_prisao", _prob_udf(col("probability")))
```

No projeto, a UDF extrai o segundo elemento do vetor `probability` (probabilidade da classe 1). UDFs são mais lentas que funções nativas do Spark (como `col`, `when`, `count`) porque serializam dados Python↔JVM para cada linha. Mas às vezes são a única opção para lógica customizada.

---

### Spark MLlib: Machine Learning Distribuído

O **MLlib** é a biblioteca de ML do Spark. A principal diferença do scikit-learn:

| Característica | scikit-learn | Spark MLlib |
|---|---|---|
| Dados | pandas DataFrame (1 máquina) | Spark DataFrame (cluster) |
| Formato de features | array numpy / DataFrame | coluna de vetor (único vetor por linha) |
| Pipeline | `sklearn.pipeline.Pipeline` | `pyspark.ml.Pipeline` |
| Fitting | `model.fit(X, y)` | `model.fit(df)` — df tem features + label |
| Predição | `model.predict(X)` | `model.transform(df)` — retorna df com nova coluna |

**O VectorAssembler** não existe no scikit-learn porque o sklearn aceita arrays numpy ou DataFrames com múltiplas colunas. O Spark MLlib exige que **todas as features estejam numa única coluna de vetor**. O VectorAssembler faz essa junção.

**Por que tudo numa única coluna?**
O Spark processa cada linha independentemente em executors diferentes. Para garantir que cada linha carregue seus próprios dados, tudo precisa estar autocontido numa única célula da tabela. Uma lista de colunas separadas não funcionaria bem em operações distribuídas de ML.

---

### Shuffle: o gargalo do Spark

O **shuffle** acontece quando o Spark precisa mover dados entre workers — por exemplo, para juntar todas as linhas de um mesmo grupo (GROUP BY) ou unir tabelas (JOIN). É o passo mais lento do Spark porque envolve serialização, transferência de rede e deserialização.

No projeto, os principais shuffles acontecem:
- Em cada `.groupBy(...).count()` da EDA
- No `randomSplit([0.8, 0.2])` (redistribui 20% dos dados para o conjunto de teste)
- No `.union(df_majority, df_minority)` do balanceamento
- Durante o treino dos modelos (otimizadores distribuídos)

O parâmetro `spark.sql.shuffle.partitions = 50` controla quantas partições o Spark cria após um shuffle. Menos partições = menos overhead de comunicação (bom para cluster pequeno), mas partições muito grandes podem não caber na RAM de um executor.

---

### `.toPandas()`: trazendo dados para o driver

```python
df.toPandas()
```

Esta é uma operação perigosa com datasets grandes. Ela coleta **todas** as linhas do DataFrame distribuído e as traz para a RAM do driver (container jupyter). Se o DataFrame tiver mais linhas do que cabem na RAM do driver, vai travar.

No projeto, `.toPandas()` é usado apenas para plotar gráficos — e sempre com uma amostra pequena ou depois de uma agregação SQL que reduziu os dados para poucas linhas (ex: 24 horas do dia, 30 distritos, 15 tipos de crime).

---

### `.collect()`: ainda mais perigoso

```python
result = df.agg(*null_expr).collect()[0]
```

`collect()` traz **todo o resultado** para o driver como uma lista de `Row` Python. O `[0]` pega a primeira linha. Usado no projeto para pegar o resultado de uma agregação que tem exatamente 1 linha (as contagens de nulos). Nunca use `collect()` em um DataFrame com milhões de linhas — vai travar.

---

### Tolerância a falhas: por que o Spark não perde dados

O Spark registra a **linhagem** (lineage) de cada RDD/DataFrame — todos os passos de transformação desde a origem. Se um executor falhar no meio de um job, o Spark reexecuta apenas as partições afetadas, refazendo o caminho desde a fonte.

Isso significa: você não precisa se preocupar com crashes de worker durante o processamento normal. O Spark recupera automaticamente. O que não recupera é a morte do **driver** ou do **SparkContext** — aí a sessão toda precisa ser reiniciada.

---

### Resumo: conceitos-chave para responder ao professor

| Conceito | O que é | Exemplo no projeto |
|---|---|---|
| **Spark** | Framework de processamento distribuído em memória | Toda a base do projeto |
| **SparkSession** | Porta de entrada — cria DataFrames, executa SQL | `spark = SparkSession.builder...getOrCreate()` |
| **SparkContext** | Motor interno — gerencia cluster e RDDs | `spark.sparkContext.parallelize(...)` |
| **DataFrame** | Tabela distribuída com schema | `df_raw`, `df_balanced`, etc. |
| **RDD** | Coleção distribuída de objetos (mais baixo nível) | Usado internamente, exposto em `sc.parallelize` |
| **Lazy evaluation** | Nada executa até uma ação ser disparada | `.filter(...)` não executa, `.count()` executa |
| **Transformação** | Operação que retorna novo DataFrame (lazy) | `.select()`, `.filter()`, `.withColumn()` |
| **Ação** | Operação que executa e retorna resultado | `.count()`, `.show()`, `.write()`, `.toPandas()` |
| **Partição** | Pedaço do DataFrame processado por um executor | `shuffle.partitions = 50` |
| **Shuffle** | Redistribuição de dados entre workers | Ocorre em GROUP BY, JOIN |
| **DAG** | Plano de execução em grafo | Visualizado em localhost:4040 |
| **TempView** | Tabela virtual para Spark SQL | `df.createOrReplaceTempView("crimes")` |
| **UDF** | Função Python customizada no Spark | `udf(lambda v: float(v[1]), DoubleType())` |
| **VectorAssembler** | Junta colunas num único vetor de features | Obrigatório para o MLlib |
| **Pipeline** | Encadeia transformações + modelo | `Pipeline(stages=BASE_STAGES + [lr])` |
| **StringIndexer** | Texto → número por frequência | "THEFT" → 0, "BATTERY" → 1 |
| **OneHotEncoder** | Número → vetor binário esparso | tipo 2 de 30 → vetor com 1 na posição 2 |
| **Checkpoint** | Salvar estado intermediário em disco | `df_balanced.write.parquet(...)` |

---

## O que o projeto faz?

O projeto pega 7,9 milhões de registros de crimes ocorridos em Chicago entre 2001 e 2017, processa tudo com Apache Spark (processamento distribuído), e treina modelos de Machine Learning para responder uma pergunta:

> **Dado um registro de crime (tipo, local, horário, distrito), o modelo consegue prever se vai resultar em prisão ou não?**

Isso é chamado de **classificação binária**: a resposta é 0 (sem prisão) ou 1 (com prisão).

---

## Por que Apache Spark?

Com 1,9 GB de dados e quase 8 milhões de linhas, o pandas normal travaria a memória. O Spark distribui o processamento entre vários nós (computadores), processa os dados em partes menores na RAM e usa processamento paralelo. No projeto, rodamos um cluster com 1 master e 2 workers, cada um com 2 CPUs e 2 GB de RAM.

Outro ponto: o Spark é **lazy** (preguiçoso). Quando você escreve `df.filter(...)`, ele não executa nada ainda. Ele só executa de verdade quando você pede um resultado concreto, como `.count()`, `.show()` ou `.write()`. Isso permite que ele otimize todo o plano de execução antes de rodar.

---

## Arquitetura do Ambiente (Docker)

```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                     │
│                                                      │
│  ┌──────────────┐    distribui    ┌───────────────┐  │
│  │ spark-master │ ─────────────> │ spark-worker-1│  │
│  │  porta 7077  │                └───────────────┘  │
│  │  porta 8080  │ ─────────────> ┌───────────────┐  │
│  └──────────────┘                │ spark-worker-2│  │
│         ▲                        └───────────────┘  │
│         │ conecta                                    │
│  ┌──────────────┐                                    │
│  │   jupyter    │ ← você acessa aqui (porta 8888)   │
│  │  (driver)    │                                    │
│  └──────────────┘                                    │
└─────────────────────────────────────────────────────┘
```

- **spark-master**: coordena o cluster, distribui as tarefas
- **spark-worker-1/2**: executam o processamento de dados
- **jupyter**: onde o código roda (o "driver" do Spark). É aqui que você abre o notebook

---

## Fluxo Geral do Pipeline

```
CSVs (1,9 GB)
    │
    ▼ Parte 1
Parquet (551 MB) ← conversão para formato colunar comprimido
    │
    ▼ Parte 2
Limpeza + Feature Engineering + EDA
    │
    ▼ Parte 2 (fim)
Dataset Balanceado (2,4M linhas) ← undersampling da classe majoritária
    │
    ▼ Parte 3
Treino dos modelos (80%) / Teste (20%)
    │
    ├── Logistic Regression  → Accuracy 71,8% | AUC 0,806
    ├── Decision Tree (d=5)  → Accuracy 61,0% | AUC 0,645
    └── MLP Neural Network   → Accuracy 52,5%
    │
    ▼ Parte 4
Gráficos para apresentação (salvos em data_parquet/)
```

---

## PARTE 1: Imports e Sessão Spark

### Célula 02 — Imports

```python
from pyspark.sql import SparkSession
```
Importa a classe principal do Spark. É por ela que tudo começa.

```python
from pyspark.sql.functions import col, to_timestamp, hour, dayofweek, month
```
Funções do Spark para manipular colunas. `col("nome")` referencia uma coluna. `to_timestamp` converte string para data/hora. `hour`, `dayofweek`, `month` extraem partes de uma data.

```python
from pyspark.sql.types import IntegerType, DoubleType
```
Tipos de dados do Spark para fazer conversões (cast) de colunas.

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
```
As três peças do pipeline de feature engineering para ML:
- `StringIndexer`: transforma texto em número (ex: "THEFT" → 0, "BATTERY" → 1)
- `OneHotEncoder`: transforma esse número em vetor binário (ex: tipo 2 de 5 → [0,0,1,0,0])
- `VectorAssembler`: junta todas as features num único vetor (necessário para o Spark MLlib)

```python
from pyspark.ml.classification import (LogisticRegression, DecisionTreeClassifier, MultilayerPerceptronClassifier)
```
Os três modelos de ML que o projeto treina.

```python
from pyspark.ml import Pipeline
```
Permite encadear várias etapas de transformação + modelo em sequência, garantindo que o mesmo processamento seja aplicado em treino e teste.

```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
```
Calcula as métricas de avaliação dos modelos (accuracy, precision, recall, F1).

---

### Célula 03 — Criação da SparkSession

```python
spark = SparkSession.builder \
```
Inicia a configuração da sessão. `builder` é um objeto que acumula as configs antes de criar a sessão.

```python
    .appName("CrimesChicago_BigData") \
```
Nome da aplicação, aparece na Spark UI (localhost:4040) para identificar o job.

```python
    .master("spark://spark-master:7077") \
```
Endereço do cluster. O driver (jupyter) vai se conectar ao master Spark na porta 7077. Dentro do Docker, o container se chama `spark-master`.

```python
    .config("spark.driver.host", "jupyter") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
```
Necessário dentro do Docker: diz ao Spark que o driver está no container chamado `jupyter` e que aceita conexões de qualquer IP (`0.0.0.0`). Sem isso, os workers não conseguiriam enviar resultados de volta ao driver.

```python
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
```
Define 2 GB de RAM para o driver (jupyter) e 2 GB para cada executor (worker). Com 4 GB no total de RAM no cluster, isso ocupa quase tudo.

```python
    .config("spark.sql.shuffle.partitions", "50") \
```
Quando o Spark precisa embaralhar dados (ex: GROUP BY, JOIN), ele divide em partições. O padrão é 200, o que gera overhead desnecessário para um cluster pequeno. 50 é mais adequado.

```python
    .getOrCreate()
```
Cria a sessão se não existir, ou retorna a existente se já estiver ativa. Evita criar duas sessões por acidente.

```python
spark.sparkContext.setLogLevel("WARN")
```
Reduz os logs do Spark para mostrar só avisos e erros. Sem isso, o notebook ficaria inundado de logs de INFO a cada operação.

---

## PARTE 1: Ingestão dos dados

### Célula 05 — Leitura dos CSVs

```python
df_raw = spark.read.csv(
    "/home/jovyan/work/archive/",
```
Lê todos os arquivos CSV dentro da pasta `archive/`. O Spark lê todos de uma vez e combina em um único DataFrame distribuído. O caminho `/home/jovyan/work/` é o diretório padrão dentro do container jupyter.

```python
    header=True,
```
Usa a primeira linha de cada CSV como nome das colunas. Sem isso, as colunas se chamariam `_c0`, `_c1`, etc.

```python
    inferSchema=True
```
O Spark analisa uma amostra dos dados e tenta adivinhar o tipo de cada coluna (inteiro, string, float). Sem isso, tudo viria como string.

```python
print(f"Total de linhas: {df_raw.count():,}")
```
`.count()` é uma **ação** que força o Spark a processar tudo e contar as linhas. Resultado: 7.941.286. O `:,` formata com separador de milhar.

```python
df_raw.printSchema()
```
Mostra a estrutura do DataFrame: nome de cada coluna e seu tipo inferido.

---

### Célula 06 — Visualizar amostra

```python
df_raw.show(5)
```
Mostra as 5 primeiras linhas do DataFrame no terminal. Útil para conferir se os dados foram lidos corretamente.

---

### Célula 08 — Converter para Parquet

```python
PARQUET_PATH = "/home/jovyan/work/data_parquet/crimes_raw"
```
Define o caminho onde o Parquet será salvo. É uma variável que será reutilizada nas células seguintes.

```python
df_raw.write.mode("overwrite").parquet(PARQUET_PATH)
```
Salva o DataFrame no formato Parquet. `.mode("overwrite")` sobrescreve se já existir.

**Por que Parquet?**
- **Formato colunar**: se você pede só a coluna `Primary Type`, o Parquet lê só ela do disco. O CSV lê a linha inteira.
- **Compressão Snappy embutida**: de 1,9 GB vira 551 MB (70% menor) sem perda de dados.
- **Schema preservado**: os tipos das colunas ficam salvos junto com os dados.
- **Velocidade**: nas próximas partes, ler o Parquet é 5-10x mais rápido que reler os CSVs.

---

## PARTE 2: ETL e EDA

ETL = Extract, Transform, Load (Extrair, Transformar, Carregar).
EDA = Exploratory Data Analysis (Análise Exploratória dos Dados).

### Célula 11 — Reler do Parquet

```python
df = spark.read.parquet(PARQUET_PATH)
```
Abre o Parquet salvo na Parte 1. A partir daqui, o DataFrame `df` é o que usaremos para processar. O `df_raw` pode ser descartado da memória.

```python
df.printSchema()
```
Confirma que o schema (tipos das colunas) foi preservado corretamente no Parquet.

---

### Célula 13 — Análise de Qualidade dos Dados (gráfico de nulos)

```python
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
```
Bibliotecas para visualização. `matplotlib` é a base, `seaborn` adiciona estilos mais bonitos em cima do matplotlib, `pandas` é usado para manipular os dados antes de plotar (os gráficos precisam de dados locais, não distribuídos).

```python
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({"figure.dpi": 120, "font.size": 11})
SAVE_DIR = "/home/jovyan/work/data_parquet/"
```
Configura o estilo visual padrão e o diretório onde os gráficos serão salvos.

```python
total_raw = df.count()
```
Conta o total de linhas para calcular percentuais de nulos.

```python
null_expr = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
```
Para cada coluna `c`, cria uma expressão que conta quantas linhas têm valor nulo nessa coluna. `when(condição, valor)` é um IF do Spark. Isso cria uma lista de expressões, uma por coluna.

```python
null_row = df.agg(*null_expr).collect()[0]
```
Executa todas as contagens de uma vez só (1 única passagem pelos dados). `.agg(*lista)` aplica todas as funções de agregação juntas. `.collect()[0]` traz o resultado para o driver como uma Row Python.

```python
null_pd = pd.DataFrame([
    {"Coluna": c, "Nulos (%)": round(null_row[c] / total_raw * 100, 2)}
    for c in df.columns
]).sort_values("Nulos (%)", ascending=False)
```
Cria um DataFrame pandas com o percentual de nulos por coluna, ordenado do maior para o menor.

```python
uniq_pd = pd.DataFrame([
    {"Coluna": c, "Valores únicos": df.select(c).distinct().count()}
    for c in df.columns
]).sort_values("Valores únicos", ascending=False)
```
Para cada coluna, conta quantos valores distintos ela tem (cardinalidade). Útil para identificar colunas com muita variação (ID, Case Number) vs poucas categorias (Arrest: True/False).

```python
colors_null = ["#e74c3c" if p > 50 else "#e67e22" if p > 10 else "#27ae60" for p in null_pd["Nulos (%)"]]
```
Define a cor de cada barra: vermelho se mais de 50% nulos (coluna problemática), laranja se entre 10-50%, verde se menos de 10%.

```python
axes[0].axvline(x=50, ...)
axes[0].axvline(x=10, ...)
```
Linhas verticais de referência nos 50% e 10% para facilitar a leitura do gráfico.

```python
axes[1].set_xscale("log")
```
Escala logarítmica no eixo X do gráfico de cardinalidade, porque os valores variam muito (de 2 até 6 milhões).

---

### Célula 15 — Limpeza dos Dados

```python
df = df.dropna(subset=["Date", "Primary Type", "Location Description",
                        "Arrest", "Latitude", "Longitude", "District", "Beat"])
```
Remove linhas onde **qualquer uma** dessas colunas essenciais está nula. São as colunas que usaremos como features ou target — sem elas, o registro é inútil. As outras colunas (Ward, Community Area, etc.) podem ter nulos porque não são usadas.

```python
df = df.filter((col("Latitude") != 0.0) & (col("Longitude") != 0.0))
```
Remove registros onde latitude e longitude são exatamente zero. Coordenada (0,0) está no Oceano Atlântico — são erros de preenchimento, não crimes reais em Chicago.

```python
df = df.filter(col("Primary Type") != "NARCOTICS")
```
Remove todos os crimes do tipo NARCOTICS.

**Por que remover NARCOTICS?**
NARCOTICS tem 99,21% de taxa de prisão — crimes de tráfico quase sempre resultam em flagrante. Se o modelo aprender que "se é NARCOTICS → prisão", ele vai acertar 99% das vezes nesse tipo sem aprender nada de útil. A feature importance do Decision Tree antes da remoção mostrava que 69% de todo o peso da árvore estava numa única feature: `Type=NARCOTICS`. Isso é chamado de **data leakage por proxy**: o tipo de crime praticamente entregava a resposta. Depois da remoção, as métricas caíram (LR: 79,9% → 71,8%), mas o modelo aprendeu padrões reais.

---

### Célula 17 — Feature Engineering

```python
df = df.withColumn("Date_ts", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
```
Cria uma nova coluna `Date_ts` convertendo a string de data (ex: "10/07/2008 12:39:00 PM") para um tipo Timestamp real do Spark. O padrão `"MM/dd/yyyy hh:mm:ss a"` descreve o formato: MM=mês, dd=dia, yyyy=ano, hh=hora 12h, mm=minuto, ss=segundo, a=AM/PM.

```python
df = df.withColumn("Hour",      hour("Date_ts"))
df = df.withColumn("DayOfWeek", dayofweek("Date_ts"))
df = df.withColumn("Month",     month("Date_ts"))
```
Extrai a hora (0-23), o dia da semana (1=domingo, 7=sábado) e o mês (1-12) do timestamp. O ML não consegue trabalhar com datas diretamente, mas consegue com números como hora e dia da semana. Esses padrões temporais têm valor preditivo: crimes de noite têm perfis diferentes de crimes de manhã.

```python
df = df.withColumn("Arrest_label", (col("Arrest") == "True").cast(IntegerType()))
```
Cria a coluna **target** (variável que queremos prever). A coluna `Arrest` é uma string ("True" ou "False"). Converte para inteiro: True→1 (houve prisão), False→0 (não houve). O `.cast(IntegerType())` faz a conversão de tipo.

```python
df = df.withColumn("Domestic_int", (col("Domestic") == "True").cast(IntegerType()))
```
Mesma lógica para a coluna `Domestic` (crime doméstico). Converte para 0/1.

```python
df = df.withColumn("District",       col("District").cast(IntegerType()))
df = df.withColumn("Beat",           col("Beat").cast(IntegerType()))
df = df.withColumn("Community Area", col("Community Area").cast(IntegerType()))
```
Converte colunas numéricas que vieram como string (por causa do `inferSchema` imperfeito) para inteiro de verdade.

```python
df = df.withColumn("Latitude",  col("Latitude").cast(DoubleType()))
df = df.withColumn("Longitude", col("Longitude").cast(DoubleType()))
```
Garante que latitude e longitude sejam doubles (números com decimais). Necessário para o VectorAssembler aceitar essas colunas.

---

### Célula 18 — Seleção de Features e Target

```python
FEATURE_COLS = [
    "Primary Type", "Location Description",   # categóricas (texto)
    "Hour", "DayOfWeek", "Month",             # temporais (números)
    "District", "Beat", "Community Area",     # geográficas (números)
    "Domestic_int", "Latitude", "Longitude"   # outras numéricas
]
TARGET_COL = "Arrest_label"
```
Define quais colunas entram no modelo como features (entradas) e qual é o target (saída). As colunas descartadas (ID, Case Number, IUCR, FBI Code, etc.) não têm poder preditivo — são identificadores únicos ou redundantes.

```python
df = df.select(FEATURE_COLS + [TARGET_COL]).dropna()
```
Mantém apenas as colunas necessárias (descarta as outras) e remove linhas que ainda tenham nulos em qualquer uma delas. Resultado final: 6.349.252 registros, 12 colunas.

---

### Células 20–23 — EDA com Spark SQL

```python
df.createOrReplaceTempView("crimes")
```
Registra o DataFrame como uma "tabela virtual" chamada `crimes`. Depois disso, é possível escrever SQL puro para consultar os dados, igual a um banco de dados.

```python
spark.sql("""
    SELECT `Primary Type`,
           COUNT(*)                                          AS total_crimes,
           SUM(Arrest_label)                                 AS total_arrests,
           ROUND(SUM(Arrest_label) / COUNT(*) * 100, 2)     AS arrest_rate_pct
    FROM crimes
    GROUP BY `Primary Type`
    ORDER BY total_crimes DESC
    LIMIT 10
""").show(truncate=False)
```
Top 10 tipos de crime por volume. `COUNT(*)` conta todos os registros, `SUM(Arrest_label)` soma os 1s (prisões), dividir um pelo outro dá a taxa de prisão. O backtick em `\`Primary Type\`` é necessário porque o nome tem espaço.

```python
spark.sql("""
    SELECT Hour,
           COUNT(*)                             AS total_crimes,
           ROUND(AVG(Arrest_label) * 100, 2)   AS arrest_rate_pct
    FROM crimes
    GROUP BY Hour ORDER BY Hour
""").show(24)
```
Taxa de prisão por hora do dia. `AVG(Arrest_label)` é a média dos valores 0/1, que equivale à proporção de prisões (0,22 = 22%).

```python
spark.sql("""
    SELECT District, COUNT(*) AS total_crimes,
           ROUND(SUM(Arrest_label) / COUNT(*) * 100, 2) AS arrest_rate_pct
    FROM crimes WHERE District IS NOT NULL
    GROUP BY District HAVING COUNT(*) > 1000
    ORDER BY arrest_rate_pct ASC LIMIT 10
""").show()
```
Os distritos com menor taxa de prisão. `HAVING COUNT(*) > 1000` filtra distritos com poucos registros (pouco significativos estatisticamente).

---

### Células 25 — Análise de Desbalanceamento

```python
count_arrest    = df.filter(col(TARGET_COL) == 1).count()
count_no_arrest = df.filter(col(TARGET_COL) == 0).count()
```
Conta separadamente quantos crimes resultaram em prisão (1) e quantos não (0). Resultado: 1.228.274 prisões vs 5.120.978 sem prisão. Razão 1:4.

**Por que desbalanceamento é um problema?**
Se um modelo simplesmente responder "sem prisão" para tudo, ele acerta 80,7% das vezes sem aprender nada. O F1-Score seria próximo de zero para a classe minoritária. Precisamos equilibrar as classes.

---

### Célula 27 — Undersampling

```python
fraction = count_arrest / count_no_arrest
```
Calcula a fração para amostrar da classe majoritária. Como há 1,2M de prisões e 5,1M de não-prisões, a fração é 1,2M/5,1M ≈ 0,24 (24%).

```python
df_majority = df.filter(col(TARGET_COL) == 0).sample(fraction=fraction, seed=42)
```
Pega uma amostra aleatória de 24% dos registros sem prisão. `seed=42` torna o processo reproduzível (sempre a mesma amostra). O resultado tem aproximadamente 1,23M de registros.

```python
df_minority = df.filter(col(TARGET_COL) == 1)
```
Mantém todos os 1,23M registros com prisão (a classe minoritária, sem sampling).

```python
df_balanced = df_majority.union(df_minority)
```
Une os dois grupos. Resultado: ~2,46M registros com proporção 50/50. O `.union()` no Spark é como o UNION ALL do SQL.

**Por que Undersampling e não SMOTE?**
SMOTE cria registros sintéticos interpolando entre exemplos reais. Mas com 1,2 milhão de amostras reais da classe minoritária, não precisamos inventar dados. Além disso, SMOTE não está disponível no Spark MLlib e funciona mal com dados esparsos de alta dimensão (como os vetores OHE de 203 dimensões que vamos gerar).

---

### Célula 28 — Checkpoint em Parquet

```python
df_balanced.write.mode("overwrite").parquet(BALANCED_PATH)
```
Salva o dataset balanceado em Parquet. Isso é um **checkpoint**: se o SparkContext morrer durante o treino do MLP (o que aconteceu várias vezes), podemos recarregar daqui sem precisar re-rodar toda a Parte 2.

---

## PARTE 3: Machine Learning

### Célula 31 — Pipeline de Feature Engineering

```python
CAT_COLS     = ["Primary Type", "Location Description"]
INDEXED_COLS = [c + "_idx" for c in CAT_COLS]
ENCODED_COLS = [c + "_enc" for c in CAT_COLS]
```
Define as colunas categóricas (texto). As versões indexadas e encodadas terão sufixos `_idx` e `_enc`. Por exemplo: `"Primary Type"` → `"Primary Type_idx"` → `"Primary Type_enc"`.

```python
NUMERIC_COLS = ["Hour", "DayOfWeek", "Month", "District", "Beat",
                "Community Area", "Domestic_int", "Latitude", "Longitude"]
```
As colunas numéricas que entram diretamente, sem precisar de transformação categórica.

```python
indexers = [
    StringIndexer(inputCol=c, outputCol=idx, handleInvalid="skip")
    for c, idx in zip(CAT_COLS, INDEXED_COLS)
]
```
Cria um StringIndexer para cada coluna categórica. O StringIndexer aprende um mapeamento: cada valor único de texto recebe um número inteiro, ordenado por frequência (o mais comum recebe 0). `handleInvalid="skip"` descarta linhas com valores que não existiam no treino (em vez de travar).

```python
encoder = OneHotEncoder(inputCols=INDEXED_COLS, outputCols=ENCODED_COLS)
```
Transforma os índices numéricos em vetores binários esparsos. Exemplo: se há 30 tipos de crime e o crime atual é o tipo 5, o vetor tem 29 posições, todas zero exceto a posição 4. Isso evita que o modelo interprete "tipo 5 > tipo 2" como se houvesse uma ordem numérica.

```python
assembler = VectorAssembler(inputCols=ENCODED_COLS + NUMERIC_COLS, outputCol="features")
```
Junta todos os vetores OHE e as colunas numéricas em um único vetor chamado `"features"`. O Spark MLlib exige que todas as features estejam em uma única coluna de vetor. O resultado tem 203 dimensões.

```python
BASE_STAGES = indexers + [encoder, assembler]
```
Lista com todas as etapas de transformação, na ordem certa. Será usada por todos os três modelos.

---

### Célula 33 — Divisão Treino/Teste

```python
train_data, test_data = df_balanced.randomSplit([0.8, 0.2], seed=42)
```
Divide o dataset em 80% para treino (1,97M linhas) e 20% para teste (491K linhas). `seed=42` garante a mesma divisão em todas as execuções.

```python
pre_pipeline  = Pipeline(stages=BASE_STAGES)
pre_model     = pre_pipeline.fit(train_data)
sample_vector = pre_model.transform(train_data.limit(1))
INPUT_SIZE    = len(sample_vector.select("features").first()[0])
```
Roda o pipeline de transformação em uma única linha para descobrir o tamanho do vetor de features. O `INPUT_SIZE` (203) é necessário para configurar a arquitetura do MLP.

**Por que aplicar o pipeline só no treino?**
O StringIndexer aprende o mapeamento de texto→número **apenas** com os dados de treino. Depois aplica o mesmo mapeamento nos dados de teste. Se fizéssemos o contrário (fitting no dataset todo), estaríamos "vazando" informação do teste para o treino.

---

### Célula 35 — Função de Avaliação

```python
def evaluate_model(predictions, label_col=TARGET_COL):
    evaluator = MulticlassClassificationEvaluator(labelCol=label_col)
    metrics = {}
    for metric in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator.setMetricName(metric)
        metrics[metric] = round(evaluator.evaluate(predictions), 4)
    return metrics
```
Reutiliza o mesmo `evaluator` para calcular as 4 métricas. `.evaluate(predictions)` calcula cada métrica a partir das colunas `prediction` e `label` do DataFrame.

**O que cada métrica significa?**

- **Accuracy**: proporção de acertos totais. `(VP + VN) / total`. Simples, mas enganosa com classes desbalanceadas.
- **Precision**: dos que o modelo disse "prisão", quantos realmente foram presos? `VP / (VP + FP)`. Alta precision = poucos alarmes falsos.
- **Recall**: dos que realmente foram presos, quantos o modelo encontrou? `VP / (VP + FN)`. Alto recall = poucas prisões perdidas.
- **F1-Score**: média harmônica entre precision e recall. `2 * (P * R) / (P + R)`. Equilíbrio entre os dois.

---

### Célula 37 — Logistic Regression

```python
lr = LogisticRegression(labelCol=TARGET_COL, featuresCol="features", maxIter=10)
```
Configura a Regressão Logística. `maxIter=10` limita o número de iterações do otimizador L-BFGS. É pouco (o padrão é 100), mas com 2M de linhas e 203 features, cada iteração é cara. 10 já converge razoavelmente.

**Como funciona a Regressão Logística?**
Aprende um peso para cada feature. A soma ponderada passa pela função sigmoide, que comprime tudo entre 0 e 1. Se o resultado é > 0,5, prevê prisão. É um modelo linear: a fronteira de decisão é uma reta (hiperplano em 203 dimensões). Funciona bem quando as features têm relação linear com o target.

```python
pipeline_lr = Pipeline(stages=BASE_STAGES + [lr])
model_lr    = pipeline_lr.fit(train_data)
```
Cria e treina o pipeline completo: StringIndexer → OHE → VectorAssembler → LogisticRegression.

```python
pred_lr = model_lr.transform(test_data)
```
Aplica o modelo treinado nos dados de teste. Gera um DataFrame com as colunas originais mais `rawPrediction`, `probability` e `prediction`.

```python
pred_lr.select("probability", TARGET_COL).write.mode("overwrite").parquet(SAVE_DIR + "pred_lr_roc")
```
Salva a coluna de probabilidade e o label real em Parquet. Necessário para gerar a curva ROC na Parte 4, caso o SparkContext morra depois do treino do MLP.

---

### Célula 39 — Decision Tree

```python
dt = DecisionTreeClassifier(labelCol=TARGET_COL, featuresCol="features", maxDepth=5)
```
Configura a Árvore de Decisão com profundidade máxima 5.

**Como funciona a Decision Tree?**
Cria uma árvore de perguntas binárias: "O tipo de crime é CRIMINAL TRESPASS? Se sim, vá para o nó X. Se não, vá para o nó Y." Cada nó divide os dados pela feature/limiar que maximiza a separação das classes (medido pelo Gini impurity).

Com `maxDepth=5`, a árvore pode fazer no máximo 5 divisões seguidas, criando até 32 folhas (2^5). Com 203 features disponíveis, isso é muito restritivo. A árvore acaba concentrando todo o peso em poucas features categóricas (como `Type=CRIMINAL TRESPASS`), ignorando as numéricas completamente.

**Por que a Decision Tree perdeu para a Regressão Logística?**
A LR usa todas as 203 features continuamente com pesos aprendidos. A DT com maxDepth=5 usa só 7 features e só consegue fazer 5 "perguntas". Para dados com alta dimensionalidade e sem uma feature dominante clara (após remover NARCOTICS), a LR ganha.

---

### Células 41–42 — MLP Neural Network

```python
if SparkContext._active_spark_context is None:
    # reinicia spark e recarrega dados
```
Verifica se o SparkContext ainda está vivo antes de rodar o MLP. O treino do modelo anterior pode ter esgotado a memória e matado o contexto silenciosamente.

```python
mlp_sample = train_data.sample(fraction=0.10, seed=42)
```
Usa apenas 10% do conjunto de treino (≈196K linhas). O MLP usa o otimizador L-BFGS que carrega o dataset inteiro na memória por iteração. Com 2M de linhas e 4 GB totais de RAM no cluster, travar é praticamente certo.

```python
mlp_train_transformed = pre_model.transform(mlp_sample)
mlp_test_transformed  = pre_model.transform(test_data)
```
Aplica o pipeline de features **manualmente** (sem incluir o MLP no Pipeline). Isso porque precisamos treinar o MLP já com as features transformadas, não raw. O `pre_model` foi ajustado no treino completo (célula 33), então os índices/encodings são consistentes.

```python
MLP_LAYERS = [INPUT_SIZE, 12, 8, 2]
```
Arquitetura da rede neural: 203 neurônios na camada de entrada (um por feature), camadas ocultas com 12 e 8 neurônios, e 2 neurônios na saída (um por classe: prisão/sem prisão).

```python
mlp = MultilayerPerceptronClassifier(
    layers=MLP_LAYERS,
    maxIter=20,     # iterações do otimizador
    blockSize=256,  # processa 256 exemplos por vez
    seed=42
)
```
**Como funciona o MLP?**
Uma rede neural com camadas totalmente conectadas. Cada neurônio recebe os valores dos neurônios anteriores, aplica pesos + bias e passa por uma função de ativação (sigmoide nas camadas ocultas, softmax na saída). O treinamento ajusta os pesos usando backpropagation para minimizar o erro.

Com apenas 10% dos dados e 20 iterações, o modelo não convergiu adequadamente, resultando em accuracy 52,5% (apenas um pouco acima do aleatório para dados balanceados, que seria 50%).

---

### Célula 44 — Tabela Comparativa

```python
rows = [
    ("Logistic Regression",  metrics_lr["accuracy"],  ...),
    ("Decision Tree",        metrics_dt["accuracy"],  ...),
    ("MLP Neural Network",   metrics_mlp["accuracy"], ...),
]
spark.createDataFrame(rows, schema).show(truncate=False)
```
Cria um DataFrame Spark com os resultados e mostra como tabela. `truncate=False` garante que nenhum texto seja cortado.

**Resultados finais:**

| Modelo | Accuracy | Precision | Recall | F1-Score | AUC |
|---|---|---|---|---|---|
| Logistic Regression | 0,7181 | 0,7182 | 0,7181 | 0,7181 | 0,8058 |
| Decision Tree (d=5) | 0,6100 | 0,7501 | 0,6100 | 0,5462 | 0,6454 |
| MLP Neural Net | 0,5253 | 0,5273 | 0,5253 | 0,5154 | — |

---

### Célula 46 — Persistência do Modelo

```python
model_dt.write().overwrite().save(MODEL_PATH)
```
Salva o modelo treinado da Decision Tree em disco no formato interno do Spark MLlib. Permite recarregar e usar para predições futuras sem precisar retreinar.

---

## PARTE 4: Visualizações

### Célula 48 — Setup

```python
try:
    spark.sql("SELECT 1 FROM crimes LIMIT 1")
except Exception:
    _df_viz = spark.read.parquet(SAVE_DIR + "crimes_balanced")
    _df_viz.createOrReplaceTempView("crimes")
```
Verifica se a temp view `crimes` ainda existe. Se não (porque a sessão foi reiniciada), recria a partir do Parquet balanceado. Necessário porque temp views vivem na sessão Spark e são perdidas se ela reiniciar.

---

### Célula 49 — Gráfico de Tipos de Crime

```python
df_crimes_pd = spark.sql("...").toPandas().iloc[::-1].reset_index(drop=True)
```
Busca os dados com SQL, converte para pandas (`.toPandas()`), e inverte a ordem (`.iloc[::-1]`) para que o maior apareça em cima no gráfico horizontal.

```python
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
```
Formata o eixo X para mostrar em milhões (ex: 1400000 → "1.4M"). `lambda x, _` é uma função anônima: `x` é o valor, `_` é a posição (ignorada).

```python
colors = ["#d73027" if r > 50 else "#4575b4" for r in df_crimes_pd["arrest_rate_pct"]]
```
Crimes com taxa de prisão acima de 50% ficam em vermelho, o restante em azul. Destaca visualmente os tipos com alta probabilidade de resultar em prisão.

---

### Célula 50 — Gráfico de Taxa por Hora

```python
ax2 = ax1.twinx()
```
Cria um segundo eixo Y compartilhando o mesmo eixo X. Permite sobrepor dois gráficos com escalas diferentes: barras de volume (eixo esquerdo) e linha de taxa de prisão (eixo direito).

---

### Célula 51 — Gráfico de Taxa por Distrito

```python
norm = plt.Normalize(df_dist_pd["arrest_rate_pct"].min(), df_dist_pd["arrest_rate_pct"].max())
cmap = plt.cm.RdYlGn
colors = cmap(norm(df_dist_pd["arrest_rate_pct"]))
```
Mapeamento de cor contínuo: o menor valor (vermelho) ao maior (verde). `Normalize` escala os valores para [0,1], e o colormap `RdYlGn` (vermelho-amarelo-verde) aplica a cor correspondente.

---

### Célula 52 — Mapa Geográfico

```python
WHERE Latitude BETWEEN 41.6 AND 42.1 AND Longitude BETWEEN -88.0 AND -87.5
```
Filtra apenas coordenadas dentro de Chicago. Sem esse filtro, outliers com coordenadas erradas distorceriam o gráfico.

```python
.sample(fraction=0.005, seed=42).toPandas()
```
Amostra 0,5% dos dados antes de trazer para o pandas. Plotar milhões de pontos no matplotlib seria lento e ilegível.

```python
hb = axes[0].hexbin(df_geo_pd["Longitude"], df_geo_pd["Latitude"], gridsize=60, cmap="YlOrRd", mincnt=1)
```
Hexbin divide o mapa em hexágonos e pinta cada um pela densidade de pontos dentro dele. É uma forma de heatmap para dados geográficos. `gridsize=60` define a resolução da grade.

---

### Célula 55 — Confusion Matrix

```python
def compute_cm(predictions):
    cm_df = predictions.groupBy(TARGET_COL, "prediction").count().toPandas()
    raw = np.zeros((2, 2), dtype=int)
    for _, row in cm_df.iterrows():
        raw[real][pred] = int(row["count"])
    return raw.T
```
Agrupa as predições por (valor real, valor previsto) e conta. Monta uma matriz 2x2:

```
              Previsto 0    Previsto 1
Real 0    [   VN (certo)    FP (errou)  ]
Real 1    [   FN (errou)    VP (certo)  ]
```

- **VN** (Verdadeiro Negativo): previu sem prisão, estava certo
- **VP** (Verdadeiro Positivo): previu prisão, estava certo
- **FP** (Falso Positivo): previu prisão, estava errado
- **FN** (Falso Negativo): previu sem prisão, estava errado

```python
QUAD = {(0, 0): "VN", (0, 1): "FN", (1, 0): "FP", (1, 1): "VP"}
```
Dicionário que mapeia posição na matriz para o rótulo correspondente.

---

### Célula 56 — Feature Importance

```python
si_primary  = model_dt.stages[0]  # StringIndexer de Primary Type
si_location = model_dt.stages[1]  # StringIndexer de Location Description
ohe         = model_dt.stages[2]  # OneHotEncoder
dt_clf      = model_dt.stages[4]  # o classificador em si (stage 3 é o assembler)
```
O modelo da DT é um Pipeline com 5 estágios (2 indexers + 1 encoder + 1 assembler + 1 classifier). Precisamos acessar individualmente para extrair os metadados.

```python
n_primary  = ohe.categorySizes[0] - 1
```
O OHE cria N-1 colunas para N categorias (a última categoria vira o vetor zero, para evitar multicolinearidade). Isso diz quantas features cada coluna categórica originou.

```python
feature_names = (
    [f"Type={lbl}" for lbl in si_primary.labels[:n_primary]] +
    [f"Loc={lbl[:18]}" for lbl in si_location.labels[:n_location]] +
    NUMERIC_COLS
)
```
Reconstrói os nomes das features na mesma ordem em que o VectorAssembler as empilhou. Necessário para saber qual feature corresponde a qual índice no vetor.

```python
importances = dt_clf.featureImportances.toArray()
```
Extrai o vetor de importâncias. Cada posição corresponde a uma feature, com valor entre 0 e 1 indicando quanto aquela feature contribuiu para as divisões da árvore (Gini gain).

---

### Célula 57 — Tendência Anual

```python
_df_raw = spark.read.parquet('/home/jovyan/work/data_parquet/crimes_raw')
_df_raw = _df_raw.filter(col('Primary Type') != 'NARCOTICS')
_df_raw = _df_raw.withColumn('Year_int', col('Year').cast(IntegerType()))
```
Lê o Parquet raw (antes da limpeza) para ter todos os anos. Filtra NARCOTICS. A coluna `Year` é string no raw, então converte para inteiro.

```python
_df_raw.filter((col('Year_int') >= 2001) & (col('Year_int') <= 2017))
```
Filtra para o intervalo 2001–2017 após o cast. O cast retorna `null` para valores não numéricos (como coordenadas que estavam na coluna Year por erro), e o filtro exclui automaticamente os nulos.

---

### Célula 58 — Curvas ROC

```python
try:
    spark.sparkContext.parallelize([1], 1).count()
except Exception:
    # reinicia spark e recarrega pred_lr, pred_dt do parquet
```
Testa se o SparkContext ainda está vivo enviando um job trivial (parallelizar [1] e contar). Se lançar exceção, reinicia a sessão e recarrega as predições salvas.

```python
_prob_udf = udf(lambda v: float(v[1]), DoubleType())
```
Cria uma UDF (User Defined Function) que extrai a probabilidade da classe 1 do vetor de probabilidades. A coluna `probability` do Spark MLlib é um vetor `[prob_classe_0, prob_classe_1]`. `v[1]` pega o segundo elemento.

```python
def get_roc_auc(predictions, label_col=TARGET_COL):
    pdf = predictions.select(
        _prob_udf("probability").alias("prob"),
        col(label_col).cast("double").alias("label")
    ).toPandas()
    fpr, tpr, _ = roc_curve(pdf["label"], pdf["prob"])
    return list(zip(fpr, tpr)), sklearn_auc(fpr, tpr)
```
Converte as predições para pandas e usa `sklearn.metrics.roc_curve`. A curva ROC calcula, para cada threshold possível de classificação, qual seria o FPR e TPR resultante. O AUC é a área sob essa curva (1.0 = perfeito, 0.5 = aleatório).

**Por que a LR tem AUC 0,80 mas accuracy 71,8%?**
AUC mede a capacidade discriminativa em todos os thresholds. Uma LR com bom AUC consegue ordenar bem os exemplos por probabilidade de prisão, mesmo que o threshold padrão de 0,5 não seja o ideal. Accuracy mede só no threshold 0,5.

---

### Célula 59 — Matriz de Correlação

```python
corr_cols = ["Hour", "DayOfWeek", "Month", "District", "Domestic_int", "Arrest_label"]
_asm = _VA(inputCols=corr_cols, outputCol="_corr_vec", handleInvalid="skip")
df_corr_input = _asm.transform(df_balanced.select(corr_cols))
corr_matrix = Correlation.corr(df_corr_input, "_corr_vec").collect()[0][0].toArray()
```
O `Correlation.corr` do Spark MLlib precisa que todas as colunas estejam num único vetor. Usa um VectorAssembler temporário só para isso. Calcula a correlação de Pearson entre todas as combinações de colunas.

**O que a correlação mostrou?**
Todas as correlações com `Arrest_label` são próximas de zero (máximo 0,04). Isso indica que as features numéricas individualmente têm pouco poder preditivo linear. O poder preditivo vem das features categóricas (tipo de crime, local) processadas via OHE — que não entram nessa análise.

---

### Célula 60 — Encerramento

```python
spark.stop()
```
Encerra a SparkSession, libera os workers e fecha todas as conexões. Sem isso, o cluster continua ocupado mesmo após o notebook parar de executar.

---

## Resumo das Decisões Técnicas

| Decisão | Alternativa descartada | Motivo |
|---|---|---|
| Parquet | CSV | 70% menor, 5-10x mais rápido de ler |
| Undersampling | SMOTE | 1,2M amostras reais, SMOTE não está no MLlib |
| Remover NARCOTICS | Manter | 99% de taxa criava atalho trivial |
| LR com maxIter=10 | maxIter=100 | RAM insuficiente para dataset completo |
| DT com maxDepth=5 | maxDepth=10-15 | Trade-off tempo/memória |
| MLP em 10% dos dados | Dataset completo | 4 GB de RAM no cluster eram insuficientes |
| Split aleatório 80/20 | Split cronológico | Simplificação; split cronológico seria mais rigoroso para séries temporais |

---

## Glossário Rápido

| Termo | Significado |
|---|---|
| DataFrame | Tabela distribuída do Spark. Como um pandas DataFrame, mas processado em paralelo entre os workers |
| Lazy evaluation | O Spark não executa nada até precisar de um resultado concreto (`.count()`, `.show()`, `.write()`) |
| Parquet | Formato de armazenamento colunar, comprimido e com schema embutido |
| StringIndexer | Transforma texto em número inteiro (label encoding) |
| OneHotEncoder (OHE) | Transforma inteiro em vetor binário (evita relação de ordem entre categorias) |
| VectorAssembler | Junta várias colunas num único vetor de features para o MLlib |
| Pipeline | Encadeia transformações + modelo, garantindo consistência treino/teste |
| Fitting | Treinar: ajustar os parâmetros do modelo/transformação nos dados de treino |
| Transform | Aplicar: usar os parâmetros aprendidos para processar novos dados |
| Accuracy | Proporção de acertos totais |
| Precision | Dos previstos como positivo, quantos são realmente positivos |
| Recall | Dos realmente positivos, quantos foram encontrados |
| F1-Score | Média harmônica entre Precision e Recall |
| AUC | Área sob a curva ROC — mede discriminação em todos os thresholds |
| Gini impurity | Critério usado pela Decision Tree para escolher a melhor divisão |
| Feature importance | Quanto cada feature contribuiu para as divisões da Decision Tree |
| Undersampling | Reduzir a classe majoritária para equilibrar as classes |
| Desbalanceamento | Quando uma classe tem muito mais exemplos que a outra |
| TempView | Tabela virtual do Spark SQL, criada a partir de um DataFrame |
| UDF | User Defined Function: função Python customizada executada dentro do Spark |
| SparkContext | O "motor" interno do Spark. Se morrer, toda a sessão precisa ser reiniciada |
| Checkpoint | Salvar o estado intermediário em disco para poder retomar sem reprocessar |
