# Autoavaliação

> As seções 1 e 4 dependem dos resultados da execução no Databricks — preencher
> com os números reais após rodar os notebooks. As seções 2 e 3 já refletem
> dificuldades e planos reais do desenvolvimento.

## 1. Atingimento dos objetivos

Para cada uma das 6 perguntas definidas no [Objetivo](./objetivo.md):

| # | Pergunta | Foi respondida? | Observações (preencher com os achados reais) |
|---|---|---|---|
| 1 | Concentração de ocorrências por bairro | ⬜ Sim / ⬜ Parcial / ⬜ Não | Top bairros e estabilidade do ranking ao longo dos anos |
| 2 | Sazonalidade temporal | ⬜ Sim / ⬜ Parcial / ⬜ Não | Dia/mês respondidos; horário com ressalva de cobertura (ver §2) |
| 3 | Tipos de ocorrência por região | ⬜ Sim / ⬜ Parcial / ⬜ Não | Heatmap tipo × bairro nos 5 bairros críticos |
| 4 | Tendência ao longo dos anos | ⬜ Sim / ⬜ Parcial / ⬜ Não | Comparação Jan-Abr; 2026 parcial |
| 5 | Tipo de local × tipo de ocorrência | ⬜ Sim / ⬜ Parcial / ⬜ Não | Limitada a 2025-2026 (`descr_tipolocal`) |
| 6 | Correlação espacial entre tipos | ⬜ Sim / ⬜ Parcial / ⬜ Não | Exploratória; correlação de perfis por bairro |

## 2. Dificuldades encontradas

Dificuldades reais do desenvolvimento (confirmadas na inspeção do dado e na
construção do pipeline):

- **Formato de data não era o suposto.** A inspeção do dado real revelou que as
  datas vêm como *datetime nativo do Excel*, não como texto `M/D/YY`. Um parse com
  o formato errado teria transformado **todas** as datas em nulo silenciosamente —
  quebrando a dimensão de tempo e três das seis perguntas.
- **Join de dimensão com chave nula (`NULL = NULL`).** Como `descr_tipolocal` é nulo
  em todo o período 2022–2024, ligar o fato à dimensão de tipo por igualdade direta
  descartaria a maioria dos registros sem erro aparente. Foi preciso adotar o membro
  sentinela `NÃO INFORMADO` para garantir integridade referencial.
- **Sentinelas heterogêneas da fonte.** Valores de "vazio" aparecem como `0` (em
  latitude/longitude), `'NULL'`, `'(Vazio)'`, `'-'` e string vazia, em colunas
  diferentes — cada um exigiu tratamento específico na camada Silver.
- **Reconciliação de schema entre 5 anos.** Renomeações (`CIDADE`→`NOME_MUNICIPIO`),
  troca de cedilha nas colunas de circunscrição e a entrada de `DESCR_TIPOLOCAL` em
  2025 exigiram reconciliação por nome (nunca por posição).
- **Leitura de `.xlsx` grande em serverless.** Arquivos de ~190 MB não podem ser
  lidos diretamente pelo Spark. A solução final foi openpyxl streaming em lotes de
  50 k linhas → pandas DataFrame → `spark.createDataFrame()` → Delta staging, sem
  nenhum arquivo intermediário acessado pelo Spark. Abordagens anteriores tentadas
  (DBFS, `/tmp` via `file://`) falharam com restrições do Free Edition serverless:
  DBFS público desativado (`DBFS_DISABLED`) e filesystem local bloqueado pelo Spark
  (`LocalFilesystemAccessDeniedException`).
- **Semântica de ano estatístico × ano de ocorrência.** Descobrir que
  `ano_estatistica` é o ano de *registro* evitou tanto uma falsa detecção de "datas
  inválidas" (ocorrências de anos anteriores são legítimas) quanto conclusões erradas
  na análise de tendência.
- **Databricks Free Edition substituiu o Community Edition** durante o desenvolvimento.
  A migração exigiu adaptar toda a estratégia de armazenamento: DBFS desativado,
  compute exclusivamente serverless (sem clusters all-purpose), Unity Catalog obrigatório.
  A orquestração via Jobs nativos (DAG de 4 tasks) eliminou a dependência do GitHub
  Actions como único orquestrador.

## 3. Trabalhos futuros

- Incorporação ao MVP de Machine Learning: predição de ocorrências por
  bairro/período, usando as dimensões já estruturadas como base de features.
- Aprofundamento da pergunta 6 com autocorrelação espacial (índice de Moran) em vez
  de correlação simples de perfis.
- Geocodificação/normalização de bairros contra uma base oficial (IBGE/prefeitura)
  para um mapa de calor mais preciso.
- Investigação da divergência de cardinalidade do campo município em 2022.
- Ampliação para municípios vizinhos (Região Metropolitana de Sorocaba) para análise
  comparativa regional.
- *(Automação da coleta já implementada: Job semanal no Databricks com detecção incremental via Content-Length.)*

## 4. Reflexão geral

*(Preencher ao final, conectando o que foi planejado no objetivo ao que foi
efetivamente entregue. Pontos sugeridos: o problema original — falta de ferramenta
granular de visualização — foi endereçado pela base analítica? Quais achados foram
mais fortes? Que limitações dos dados públicos mais pesaram? O quanto a base Gold
está pronta para sustentar o MVP de ML seguinte?)*
