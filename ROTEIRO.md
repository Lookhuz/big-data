# Roteiro de Apresentação — Análise de Crimes em Chicago
**Tempo total:** 10 minutos
**Raphael:** Partes 1 e 2 + abertura + EDA + conclusão
**Lucas:** Parte 3 em diante + todas as decisões técnicas

---

## RAPHAEL

---

**[1 — Capa] ~10s**
> "Boa tarde. Nosso projeto analisa crimes em Chicago pra prever se um crime vai resultar em prisão ou não."

---

**[2 — Objetivo] ~30s**
> "A pergunta que queremos responder é: dado um crime — tipo, hora, local, distrito — dá pra prever se a pessoa vai ser presa? Escolhemos Chicago porque o dataset é público, tem 1,9 GB e quase 8 milhões de registros reais. É grande o suficiente pra justificar Big Data."

---

**[3 — Docker/Spark] ~25s**
> "Um computador normal não processa 8 milhões de linhas com eficiência. Então montamos um cluster: três máquinas virtuais dentro do Docker trabalhando em paralelo. O Spark distribui o processamento entre elas automaticamente — você escreve o código uma vez, ele divide o trabalho."

---

**[4 — Dataset] ~20s**
> "Quatro arquivos CSV cobrindo 2001 a 2017. Cada linha é um crime registrado — tipo, data, local, se houve prisão. Usamos 11 dessas colunas como entrada pro modelo."

---

**[5 — Ingestão/Parquet] ~25s**
> "O primeiro passo foi converter os CSVs pra Parquet. Por quê? CSV lê a linha inteira mesmo quando você quer só uma coluna. Parquet lê só o que você pede — e o arquivo ficou 70% menor. Nas etapas seguintes isso economizou muito tempo."

---

**[6 — Limpeza] ~30s**
> "Removemos linhas com dados faltando nas colunas que o modelo ia usar. Removemos coordenadas zeradas — crime no meio do oceano não existe, é erro de preenchimento. E removemos o tipo NARCOTICS: 99% dos crimes de narcóticos resultam em prisão. Se deixássemos, o modelo aprenderia só isso e ignoraria tudo mais."

---

**[7 — Feature Engineering] ~20s**
> "A coluna de data vinha como texto — o modelo não entende texto. Então extraímos hora, dia da semana e mês separadamente. Convertemos prisão de 'True/False' pra 0 e 1. Agora o modelo consegue trabalhar com esses dados."

---

**[8 — EDA: Tipos de Crime] ~20s**
> "Esse gráfico mostra os 15 tipos de crime mais comuns e suas taxas de prisão. O interessante é que volume alto não significa taxa alta — Theft é o mais comum mas só 11% resulta em prisão. CRIMINAL TRESPASS tem bem menos crimes mas taxa muito maior. O modelo precisa aprender essa diferença."

---

**[9 — EDA: Hora do Dia] ~20s**
> "A linha vermelha é a taxa de prisão ao longo do dia. Ela sobe à noite — pico às 20h com 22,7% — e cai de manhã, mínimo às 9h com 14,4%. Isso significa que a hora do crime tem informação útil pro modelo: crimes noturnos têm perfil diferente de crimes diurnos."

---

**[10 — EDA: Distrito] ~20s**
> "Cada barra é um distrito policial. A cor vai de vermelho — menor taxa de prisão — até verde — maior taxa. Quase 12 pontos de diferença entre o pior e o melhor distrito. Ou seja: onde o crime acontece importa na hora de prever se vai resultar em prisão."

---

**[11 — Mapa Geográfico] ~15s**
> "À esquerda, densidade de crimes — centro e sul de Chicago concentram mais. À direita, azul é sem prisão e vermelho é com prisão. Dá pra ver que a distribuição geográfica é diferente entre os dois grupos — localização tem poder preditivo."

---

**[12 — Balanceamento] ~30s**
> "Problema: 80% dos crimes não resultam em prisão. Se o modelo simplesmente dissesse 'não vai preso' pra tudo, acertaria 80% das vezes sem aprender nada. Então reduzimos a classe majoritária pra igualar com a minoritária — de 5,1M e 1,2M passamos pra 1,2M de cada lado. O modelo agora é forçado a aprender a diferença."

---

## LUCAS

---

**[13 — Undersampling vs SMOTE] ~25s**
> "SMOTE inventaria dados falsos pra aumentar a classe minoritária. Não fizemos isso porque já tínhamos 1,2 milhão de casos reais de prisão — não precisamos inventar. Além disso, SMOTE não funciona bem com dados de alta dimensão como os nossos."

---

**[14 — Pipeline ML] ~30s**
> "O Spark MLlib não aceita texto como entrada — tudo tem que virar número. Então o pipeline tem três etapas antes do modelo: StringIndexer converte tipo de crime em número, OneHotEncoder transforma esse número em vetor binário pra não criar relação de ordem falsa, e VectorAssembler junta tudo num único vetor de 203 dimensões que entra no modelo."

---

**[15 — Os 3 Modelos] ~20s**
> "Treinamos três modelos. Regressão Logística como baseline — o mais simples. Decision Tree — que aprende regras em sequência. E MLP — uma rede neural pequena. O MLP precisou ser treinado em só 10% dos dados porque a memória do cluster não aguentou o dataset completo."

---

**[16 — Resultados] ~35s**
> "A Regressão Logística ganhou com 71,8% de acurácia. A Decision Tree ficou em 61% — daqui a pouco explico o porquê. O MLP ficou em 52,5% — treinado em 10% dos dados, não teve informação suficiente pra aprender. Pra contexto: 50% seria chute aleatório num dataset balanceado."

---

**[17 — Confusion Matrix] ~25s**
> "Esse gráfico mostra onde cada modelo erra. VP e VN são acertos, FP e FN são erros. A LR erra de forma equilibrada dos dois lados. A DT tem 38% de falsos negativos — ela deixa passar muitos crimes que deveriam resultar em prisão. O MLP praticamente divide tudo no meio, que é o comportamento de quem não aprendeu."

---

**[18 — Curvas ROC] ~25s**
> "Acurácia mede só com um threshold fixo de 50%. A curva ROC mede o modelo em todos os thresholds possíveis. AUC é a área sob essa curva — 0,5 é chute aleatório, 1,0 é perfeito. Nossa LR chegou em 0,81 — ela consegue separar bem quem vai ser preso de quem não vai, independente do threshold usado."

---

**[19 — Feature Importance] ~25s**
> "Esse gráfico mostra o que a Decision Tree aprendeu. Com profundidade 5, ela só pode fazer 5 perguntas — das 203 features disponíveis, usou apenas 7, todas sobre tipo de crime e localização. Hora, distrito, latitude: ignorados completamente. É como ter 203 pistas e só poder usar 5. Isso explica por que a Logistic Regression ganhou — ela usa todas as 203 ao mesmo tempo."

---

**[20 — Análise Crítica] ~35s**
> "Por que a LR ganhou? Ela tem 203 pesos, um por feature, e os usa todos ao mesmo tempo. A DT com depth 5 faz só 5 perguntas e ignora o resto. Com depth maior a DT provavelmente chegaria perto — mas no nosso cluster inviabilizou por tempo e memória. O MLP com o dataset completo provavelmente seria o melhor dos três — mas 4 GB de RAM não foram suficientes."

---

**[21 — Desafios] ~20s**
> "Três problemas reais durante o projeto. O Spark morria por falta de memória no meio do treino — resolvemos salvando checkpoints em Parquet. O MLP travava sem terminar — resolvemos usando 10% dos dados. Cada execução completa leva 40 a 60 minutos, então cada erro custava caro."

---

**[22 — Limitações] ~20s**
> "Somos honestos: não usamos cross-validation por limitação de tempo de execução. Não testamos depth maior na DT pelo mesmo motivo. O dataset tem viés temporal — crimes de 2001 e de 2017 têm perfis diferentes mas tratamos como se fossem iguais."

---

## RAPHAEL

---

**[23 — Conclusões] ~20s**
> "Pipeline completo de Big Data: do CSV bruto ao modelo treinado. A Regressão Logística com AUC 0,81 mostrou que dá pra prever prisão com 71% de acurácia usando apenas dados do registro do crime. As limitações estão documentadas e entendemos o porquê de cada resultado."

---

**Total estimado: ~9min30s**
