# MVP de Engenharia de Dados — Criminalidade em Sorocaba (SP)

Pipeline de dados em nuvem que transforma as planilhas anuais de ocorrências criminais do estado de São Paulo em um **data warehouse dimensional** sobre o município de Sorocaba, com análise de qualidade e respostas a oito perguntas de negócio.

**Plataforma:** Google Cloud Platform (Cloud Storage · Dataproc Serverless · BigQuery)
**Período coberto:** 2022 a 2026 · **Volume processado:** 5.348.378 registros do estado, dos quais 73.394 de Sorocaba

> **Sobre a escolha da plataforma.** O trabalho foi desenvolvido na GCP em vez do Databricks, opção prevista no descritivo (*"não haverá limitação de utilização de outras plataformas de dados e provedores de nuvem"*) e alinhada previamente com o professor. Cada componente da arquitetura ensinada em aula tem correspondente direto aqui — inclusive o cluster Hadoop-Spark, que a própria Aula 3 admite ser montado sobre armazenamento de objetos no lugar do HDFS. O mapeamento está na [seção de arquitetura](#arquitetura).

---

## O problema

A Secretaria da Segurança Pública de São Paulo publica os dados criminais do estado inteiro em planilhas anuais de cerca de 200 MB, com mais de um milhão de linhas cada, cobrindo 645 municípios de uma vez. Nesse formato os dados existem, mas não são utilizáveis: não abrem em uma planilha comum e **não podem ser cruzados entre anos**, porque a fonte muda os nomes das colunas, publica a mesma natureza criminal com grafias diferentes e deixa de publicar campos inteiros em alguns anos.

Este MVP resolve isso para Sorocaba, construindo uma base histórica integrada e consultável — e documentando cada problema encontrado no caminho.

## As perguntas respondidas

| # | Pergunta | Resposta resumida |
|---|---|---|
| **P1** | Como evoluiu o volume de ocorrências? | +10,5% em absoluto, mas **+4,9% em taxa por 100 mil habitantes**. Metade do crescimento é população. |
| **P2** | Quais naturezas cresceram e quais caíram? | Roubo **−33%**, furto de veículo **+37%**, lesão corporal dolosa **+44%**. |
| **P3** | Existe sazonalidade mensal? | **Não** de forma relevante — 22% de amplitude, boa parte artefato de calendário. |
| **P4** | Há concentração por horário? | O roubo concentra **36,2%** entre 18h e 23h; o furto se distribui plano pelas 24 horas. |
| **P5** | Que locais concentram cada natureza? | Via pública tem o volume (55%); **residência tem a gravidade (41,9% de crimes violentos)**. |
| **P6** | Como se distribui no território? | O 8º DP tem 18,7% das ocorrências; o 7º DP, o menor em volume, tem a **maior proporção de violência (39,7%)**. |
| **P7** | Crimes contra veículos? | Furto **+30%**, roubo **−28%** em taxa. É o indicador mais confiável da base. |
| **P8** | A geolocalização revela pontos quentes? | Sim — mas a criminalidade é **dispersa**: as 10 células mais densas somam só 5% do total. |

As perguntas foram declaradas em [`docs/01-objetivo.md`](docs/01-objetivo.md) **antes de qualquer coleta**, conforme a orientação do trabalho. A discussão completa está em [`docs/07-analise-e-resultados.md`](docs/07-analise-e-resultados.md).

---

## Arquitetura

```
   FONTES                DATA LAKE (Cloud Storage)          DW (BigQuery)
   ──────                ─────────────────────────          ─────────────

   SSP-SP  ──┐        ┌── bruta/ ────────┐
   (.xlsx)   ├──►     │  formato nativo  │
   IBGE    ──┘        └──────────┬───────┘
   (JSON)                        │ conversão de formato
                      ┌──────────▼───────┐    ┌──────────────┐    ┌──────────────┐
                      │  preparada/      │───►│   PySpark    │───►│    stg.      │
                      │  Parquet         │    │  Dataproc    │    │ (conformado) │
                      └──────────────────┘    │  Serverless  │    └──────┬───────┘
                                              └──────────────┘           │ SQL
                                               extração,                  ▼
                                               conciliação,       ┌──────────────┐
                                               limpeza, filtro    │     dw.      │
                                                                  │   estrela    │
                                                                  └──────────────┘
```

### Do conceito ensinado ao serviço usado

| Conceito (apostilas PUC-Rio) | Implementação |
|---|---|
| Data lake — dados "em seu formato nativo" (Aula 3) | Cloud Storage, zona `bruta/` |
| Cluster Hadoop Spark, com objeto no lugar do HDFS (Aula 3) | Dataproc Serverless (PySpark) sobre Cloud Storage |
| DW em SGBD relacional, abordagem ROLAP (Aula 1) | BigQuery — datasets `stg`, `dw`, `qualidade` |
| Componente de ETL do ambiente de BI (Aula 1) | job PySpark + scripts SQL versionados |
| Repositório de metadados: semânticos, técnicos e de proveniência (Aula 1) | catálogo em `docs/` + `description` em cada tabela e coluna no BigQuery + linhagem em PROV |
| Operadores OLAP: slice/dice, roll-up, drill-down (Aula 1) | consultas com `GROUP BY`/`ROLLUP` sobre visões multidimensionais |
| Dados abertos para enriquecimento (Governança, Aula 3) | IBGE: código do município e população |

Região única para todos os serviços: `southamerica-east1` (São Paulo).

### O modelo dimensional

**Esquema estrela** com sete dimensões desnormalizadas. O grão do fato é **uma natureza criminal apurada em um boletim de ocorrência** — verificado nos dados, não suposto: um mesmo boletim que apura roubo e lesão corporal gera duas linhas.

`fato_populacao_anual` compartilha `dim_municipio` com o fato principal, formando uma **constelação de fatos** — é o que permite converter contagens em taxa por 100 mil habitantes.

Detalhes em [`docs/03-modelagem.md`](docs/03-modelagem.md) · DDL em [`sql/10_ddl_dw.sql`](sql/10_ddl_dw.sql)

---

## Como executar

### Pré-requisitos
- Projeto GCP com faturamento ativo
- Acesso ao Google Colab

### 1. Provisionar a infraestrutura

```bash
export PROJETO_ID="seu-projeto-gcp"
bash infra/00_setup_gcp.sh
```

Cria o bucket com as zonas do data lake, os três datasets do BigQuery, a conta de serviço e habilita o **Acesso privado ao Google** na sub-rede — sem o qual o Dataproc Serverless falha na inicialização.

### 2. Executar os notebooks, em ordem

| Notebook | O que faz | Tempo aproximado |
|---|---|---|
| [`01_coleta_ingestao.ipynb`](notebooks/01_coleta_ingestao.ipynb) | descobre o esquema, coleta ~1 GB da fonte, converte para Parquet | 30–45 min |
| [`02_etl_carga_dw.ipynb`](notebooks/02_etl_carga_dw.ipynb) | submete o job Spark e carrega o esquema estrela | 10–15 min |
| [`03_qualidade_dados.ipynb`](notebooks/03_qualidade_dados.ipynb) | perfila cada atributo e verifica a integridade | 2 min |
| [`04_analise_resultados.ipynb`](notebooks/04_analise_resultados.ipynb) | responde às oito perguntas com consulta, gráfico e discussão | 3 min |

Ajuste `PROJETO_ID` e `REPO_URL` na primeira célula de cada notebook.

Todo o pipeline é **idempotente**: reexecutar qualquer etapa produz o mesmo resultado, sem duplicar dados.

### Custo estimado

Cerca de **US$ 2 a 4** por execução completa — praticamente todo em Dataproc Serverless. O armazenamento (~1,5 GB) e as consultas do BigQuery ficam dentro da camada gratuita.

---

## Estrutura do repositório

```
README.md
infra/00_setup_gcp.sh              provisionamento da GCP
notebooks/                         execução, em ordem numérica
  01_coleta_ingestao.ipynb
  02_etl_carga_dw.ipynb
  03_qualidade_dados.ipynb
  04_analise_resultados.ipynb
spark/etl_ocorrencias.py           job PySpark do Dataproc Serverless
sql/
  10_ddl_dw.sql                    esquema estrela, com PK/FK e descrições
  15_de_para_natureza.sql          classificação por título do Código Penal
  20_carga_dimensoes.sql           carga das dimensões
  21_carga_fato.sql                carga do fato
  25_carga_populacao.sql           carga do fato de população
  30_qualidade.sql                 perfil por atributo e verificações
  40_views_analiticas.sql          visões multidimensionais
  45_perguntas_negocio.sql         consultas que respondem P1..P8
docs/
  01-objetivo.md                   problema e perguntas (escrito antes da coleta)
  02-coleta.md                     fontes, licenças e descoberta de esquema
  03-modelagem.md                  esquema estrela e decisões de modelagem
  04-catalogo-de-dados.md          catálogo completo, com domínios
  05-carga-etl.md                  as oito transformações, com evidência
  06-qualidade-de-dados.md         análise de qualidade
  07-analise-e-resultados.md       respostas e discussão
  08-autoavaliacao.md              autoavaliação
  09-linhagem.md                   linhagem em PROV e registro de decisões
  evidencias/                      capturas de tela da execução
```

> O diretório `references/`, com o descritivo da atividade e o material didático da PUC-Rio, existe apenas na cópia local: é material de aula protegido por direito autoral e não é redistribuído neste repositório público.

---

## Fontes e licenças

| Fonte | Conteúdo | Licença |
|---|---|---|
| [SSP-SP — SPDados](https://www.ssp.sp.gov.br/estatistica/consultas) | ocorrências criminais do estado de São Paulo | dados abertos, publicados oficialmente para download direto |
| [IBGE — Localidades](https://servicodados.ibge.gov.br/api/docs/localidades) | código e hierarquia regional do município | dados abertos |
| [IBGE — SIDRA](https://apisidra.ibge.gov.br/) | população residente (Censo 2022 e estimativas) | dados abertos |

**Coleta:** agosto de 2026. **Ética:** a coleta faz requisições diretas aos arquivos publicados pela própria Secretaria, com *User-Agent* identificado. Não há raspagem de páginas nem contorno de qualquer proteção.

**LGPD:** a base **não contém dados pessoais identificáveis** — não há nome, documento, idade ou sexo de vítimas ou autores. O endereço exato do fato existe na origem e foi **deliberadamente excluído** da carga no data warehouse; a decisão está registrada em [`docs/09-linhagem.md`](docs/09-linhagem.md).

---

## O que este trabalho descobriu sobre os dados

Achados que não estão documentados em lugar nenhum e só apareceram ao perfilar os dados:

- **A fonte troca os nomes das colunas todo ano.** Nove dos trinta campos mudam de grafia; `CIRCUNCRIÇÃO` (com erro e acento) vira `CIRCUNSCRICAO` em 2026; o tipo de local não existe antes de 2025.
- **O código IBGE acompanha a circunscrição**, não o município de registro — é a diferença entre medir crimes ocorridos em Sorocaba e boletins digitados em Sorocaba.
- **A coordenada `0` é ausência disfarçada de número.** Passa por qualquer validação de tipo e desloca qualquer média.
- **A mesma natureza criminal aparece com grafias diferentes:** 28 valores distintos para 23 naturezas reais.
- **41% dos horários caem no minuto 00.** A hora registrada é estimativa da vítima, não medição — o que define a granularidade máxima confiável da análise.
- **O arquivo de cada ano é fechado pelo ano da estatística, não do fato.** Há ocorrências de 1976 no arquivo de 2026.

---

**Autor:** Paulo Musachio · Pós-graduação em Ciência de Dados e Analytics — PUC-Rio
