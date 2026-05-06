# Especificação de Projeto: Big Data + Machine Learning (Spark)

## 1. Diretrizes de Comportamento (CRÍTICO)
- Seja um Engenheiro de Computação: direto, técnico e focado em performance.
- Gere apenas código, configurações e explicações essenciais.
- O código deve ser escalável, seguindo as melhores práticas do ecossistema Apache.

---

## 2. Contexto do Projeto
Construir um pipeline de Big Data distribuído utilizando Apache Spark para processar o dataset de Crimes de Chicago[cite: 1].

### Dataset e Gestão de Volume
- **Origem:** Arquivos CSV localizados em `/archive`.
- **Restrição de Volume:** Carregar e utilizar apenas uma fatia de dados que totalize **entre 1.1GB e 1.5GB**, atendendo ao requisito de ser > 1GB sem exceder desnecessariamente a memória do cluster simulado[cite: 1].
- **Variável Alvo:** `Arrest` (Classificação Binária).

---

## 3. Parte 1: Ambiente de Cluster e Otimização de Formato
**Requisito:** Simular um ambiente real e otimizar o storage[cite: 1].

- **Docker Compose:** Configurar 1 Spark Master e 2 Spark Workers.
- **Data Lake (Engenharia):** 
  1. O Claude deve gerar um script inicial para ler os CSVs originais de `/archive`.
  2. **Conversão Compulsória:** Converter esses dados para o formato **Parquet** antes de iniciar a análise. 
  3. Justificativa: Parquet é colunar, binário e otimizado para o Spark, reduzindo o tempo de I/O em comparação ao CSV[cite: 1].

---

## 4. Parte 2: Análise e Pré-processamento (ETL)
Seguir rigorosamente o padrão de manipulação de DataFrames do Spark[cite: 1].

### 4.1 Ingestão (Otimizada)
- Leitura dos dados a partir do arquivo **Parquet** gerado na Parte 1.
- **Limpeza:** Remover nulos e tratar colunas temporais.
- **Feature Engineering:** Extrair 'Hour' e 'DayOfWeek' para aumentar o poder preditivo.

### 4.2 Transformações e EDA
- Utilizar `select`, `filter`, `groupBy` e `agg`.
- **Spark SQL:** Realizar consultas complexas via `spark.sql()` para demonstrar domínio[cite: 1].

### 4.3 Tratamento Categórico e Balanceamento
- **Pipeline de Features:** Aplicar `StringIndexer` e `OneHotEncoder`[cite: 1, 2].
- **Balanceamento:** Aplicar **Undersampling** na classe majoritária de `Arrest` para evitar viés no modelo.

---

## 5. Parte 3: Machine Learning (Pipeline + Avaliação)
Referência obrigatória de implementação: `aula_06_parte_2.ipynb`[cite: 2].

### 5.1 Construção do Pipeline
O fluxo deve ser encapsulado em um `pyspark.ml.Pipeline`:
1. `StringIndexer` (com `handleInvalid='skip'`).
2. `OneHotEncoder`.
3. `VectorAssembler` (coluna `features`)[cite: 2].
4. `Modelo`.

### 5.2 Modelos Obrigatórios (Versões de Classificação)[cite: 1]
1. **Logistic Regression** (Baseline).
2. **Decision Tree Classifier**.
3. **Multilayer Perceptron Classifier (MLP):** 
   - Configurar camadas: [Input_Size, 12, 8, 2].

### 5.3 Avaliação de Performance[cite: 1]
Extrair métricas completas via `MulticlassClassificationEvaluator`:
- **Accuracy, Precision, Recall e F1-Score.**

---

## 6. Referência Técnica
**REPRODUZIR** o padrão do arquivo: `aula_06_parte_2.ipynb`[cite: 2].
- Focar na correta vetorização das features e no uso do Pipeline para garantir a reprodutibilidade do modelo.

---

## 7. Estrutura do Projeto
```text
project/
├── docker/          # docker-compose.yml
├── archive/         # CSVs originais (> 1GB)
├── data_parquet/    # Dados otimizados para o projeto
└── notebooks/       # projeto_final.ipynb