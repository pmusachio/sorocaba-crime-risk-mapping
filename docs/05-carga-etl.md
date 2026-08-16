# 5. Carga — o processo de ETL

Execução: [`notebooks/02_etl_carga_dw.ipynb`](../notebooks/02_etl_carga_dw.ipynb)
Código: [`spark/etl_ocorrencias.py`](../spark/etl_ocorrencias.py) e [`sql/`](../sql/)

O descritivo pede que se documente "os processos de transformação e carga, principalmente os de transformação, e.g. a junção e conciliação de dois conjuntos de dados diferentes". É o que esta página faz: cada transformação aplicada, o problema que ela resolve e a evidência de que o problema existe.

## Arquitetura do pipeline

```
   FONTES                DATA LAKE (Cloud Storage)          DW (BigQuery)
   ──────                ─────────────────────────          ─────────────

   SSP-SP  ──┐        ┌── bruta/ ────────┐
   (.xlsx)   ├──►     │  formato nativo  │
   IBGE    ──┘        └──────────┬───────┘
   (JSON)                        │ conversão de formato
                      ┌──────────▼───────┐    ┌─────────────┐    ┌──────────────┐
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

A divisão de trabalho entre as duas ferramentas é deliberada:

| Etapa | Ferramenta | Por quê |
|---|---|---|
| Extração, conciliação, limpeza, filtro | PySpark no Dataproc Serverless | 5,3 milhões de registros do estado precisam ser lidos antes que o filtro por município possa ser aplicado |
| Modelagem dimensional e carga | SQL no BigQuery | as 73 mil linhas resultantes cabem folgadamente no SGBD, e a construção do esquema estrela é trabalho de DDL e DML |

Sobre a escolha do Spark, a Aula 3 é explícita quanto à equivalência entre as nuvens: *"em nuvem, todos os fornecedores baseiam suas soluções em clusters Hadoop Spark, ainda que os componentes possam ser substituídos. É possível, por exemplo, utilizar o AWS S3 ou Azure Blob Storage em vez do HDFS."* Aqui, o Cloud Storage substitui o HDFS e o Dataproc Serverless provisiona o cluster Spark sob demanda.

---

## Transformações aplicadas

### T1. Conciliação dos esquemas divergentes entre anos

**Problema.** Nove dos trinta campos mudam de nome ao longo da série, e um deles simplesmente não existe nos três primeiros anos. Sem tratamento, uma leitura conjunta dos cinco arquivos produziria colunas fantasmas: `CIDADE` preenchida só em 2022, `NOME_MUNICIPIO` preenchida só de 2023 em diante.

**Tratamento.** Um de-para explícito associa cada campo canônico do DW a todas as grafias já publicadas pela fonte. A comparação é feita sobre o nome **normalizado** (maiúsculas, sem acento, espaço convertido em underscore), de modo que `COD IBGE` e `CD_IBGE` cheguem ao mesmo campo e uma variação futura de acentuação não quebre o pipeline. Quando mais de uma grafia coexiste na leitura, um `COALESCE` resolve linha a linha.

**Evidência:** a tabela comparativa de cabeçalhos em [`02-coleta.md`](02-coleta.md#o-que-a-descoberta-de-esquema-revelou), gerada pelo notebook 01.

### T2. Conversão das sentinelas em nulo

**Problema.** A fonte representa ausência de dado de quatro formas diferentes, e nenhuma delas é nulo: o texto `NULL`, o texto `(Vazio)`, o hífen `-`, e o número `0` nas coordenadas. Um `COUNT` ingênuo trataria `'NULL'` como um valor válido.

**Tratamento.** Todas as sentinelas viram nulo de verdade no ETL — nunca antes, para que a análise de qualidade consiga medir o tamanho do problema na zona preparada. A coordenada `0` recebe tratamento próprio: ela não é "zero graus", é ausência de geolocalização, e por isso vira nulo com a marcação `tem_geolocalizacao = FALSE`.

**Evidência:** no arquivo de 2026, o campo `HORA_OCORRENCIA_BO` traz `NULL` em 2.692 das 8.069 linhas de Sorocaba, e `DESC_PERIODO` traz `NULL` em 4.319.

### T3. Filtro do município pela chave, não pelo nome

**Problema.** Duas armadilhas ao mesmo tempo. Primeira: filtrar por texto depende da grafia, que varia entre anos. Segunda, e mais grave: os arquivos trazem **dois** municípios por linha — o de registro do boletim e o da circunscrição, que é onde o fato ocorreu.

**Tratamento.** O filtro usa `COD IBGE = '3552205'`, que a verificação mostrou acompanhar a **circunscrição**. É o critério correto para o objetivo declarado: medir a criminalidade *em* Sorocaba, e não os boletins digitados em Sorocaba.

**Evidência.** Em 2026, 8.104 linhas têm registro em Sorocaba e 8.069 têm circunscrição em Sorocaba, mas apenas 8.056 têm as duas coisas. As demais são fatos ocorridos em Sorocaba e registrados em Votorantim ou Franco da Rocha, e fatos registrados em Sorocaba mas ocorridos em Piedade ou Salto de Pirapora. A coluna `registrado_em_outro_municipio` preserva essa informação no fato.

### T4. Padronização das grafias da natureza criminal

**Problema.** A fonte publica a mesma natureza criminal com grafias diferentes. Sem tratamento, uma única natureza apareceria como duas categorias distintas em qualquer agrupamento — e as duas metades seriam somadas separadamente:

| Grafias encontradas | Registros |
|---|---|
| `TRÁFICO DE ENTORPECENTES` | 1.207 |
| `TRAFICO DE ENTORPECENTES` | 376 |
| `LESÃO CORPORAL CULPOSA - OUTRAS` (hífen) | 147 |
| `LESÃO CORPORAL CULPOSA – OUTRAS` (travessão) | 36 |
| `TENTATIVA DE HOMICÍDIO` / `TENTATIVA DE HOMICIDIO` | 206 / 39 |
| `HOMICÍDIO CULPOSO POR ACIDENTE DE TRÂNSITO` / sem acento | 277 / 65 |

**Tratamento.** Padronização para maiúsculas sem acento, com travessões convertidos em hífen simples e espaços colapsados. Os **28 valores distintos da origem se reduzem a 23 naturezas reais**. O valor original é preservado na coluna `natureza_apurada_origem`, para auditoria e para que a análise de qualidade possa medir o efeito.

O mesmo tratamento se aplica ao bairro, pelo mesmo motivo (`VILA ZULMIRA` e `Vila Zulmira` são o mesmo bairro).

### T5. Derivação do tipo de local para os anos que não o publicam

**Problema.** `DESCR_TIPOLOCAL` só existe nos arquivos de 2025 e 2026. Nos três primeiros anos há apenas o subtipo. Sem tratamento, a pergunta P5 só poderia ser respondida para dois dos cinco anos.

**Tratamento.** O subtipo é, pela definição da própria fonte, "subgrupo de tipos de locais, **vinculado ao tipo de local**". A correspondência subtipo → tipo é extraída dos anos que publicam os dois campos — sobre o **estado inteiro**, para maximizar a cobertura de subtipos raros — e aplicada aos anos anteriores. Quando um subtipo aparece com mais de um tipo, prevalece o mais frequente.

**Limitação, declarada.** A derivação não é exata, e medir o quanto exigiu cuidado. A contagem bruta de "subtipos com mais de um tipo associado" dá 42 dos 302 — mas essa medida engana: `Via Pública` aparece 1.151.726 vezes como `Via Pública` e **4 vezes** com outros tipos, o que é erro de digitação da fonte, não ambiguidade. Por isso o critério é a **confiança**: a proporção do tipo predominante dentro do subtipo.

| Confiança do tipo predominante | Subtipos | Registros do estado |
|---|---|---|
| < 100% (qualquer divergência) | 42 | 1.564.329 |
| < 99% | 30 | 126.959 |
| **< 95% (critério adotado)** | **22** | **40.137** |
| < 90% | 16 | 34.986 |

O caso genuinamente ambíguo é `Lojas`, que aparece como `Condomínio Comercial` em 6.012 registros e como `Shopping Center` em 2.602 — escolher o mais frequente erra em quase um terço das vezes. Com o limiar de 95%, **889 dos 40.002 registros derivados de Sorocaba (2,2%) ficam marcados como ambíguos**. Duas colunas acompanham cada linha:

- `origem_tipo_local`: `publicado pela fonte` · `derivado do subtipo` · `não informado`
- `tipo_local_ambiguo`: verdadeiro quando o subtipo de origem é ambíguo

A pergunta P5 é respondida **apenas sobre os anos em que o tipo é publicado**, e o dado derivado fica disponível para quem aceitar a imprecisão em troca da série completa. Sem essas duas colunas, um dado derivado seria indistinguível de um dado publicado.

### T6. Derivação do período do dia a partir da hora

**Problema.** `DESC_PERIODO` está ausente em mais da metade dos registros — mas a hora, em muitos desses casos, está preenchida.

**Tratamento.** Quando o período não é informado e a hora existe, o período é derivado dela usando as mesmas quatro categorias da fonte (madrugada 0h–5h, manhã 6h–11h, tarde 12h–17h, noite 18h–23h). A coluna `origem_periodo` distingue `publicado pela fonte` de `derivado da hora` e de `não informado`.

A categoria `EM HORA INCERTA`, que a fonte usa explicitamente, é preservada: ela é uma informação, não uma ausência.

### T7. Remoção de duplicidades

**Problema.** A fonte republica os arquivos periodicamente, e um mesmo registro pode aparecer em mais de uma guia.

**Tratamento.** Deduplicação pela combinação de boletim, data, hora, rubrica, natureza, conduta e bairro. **Atenção ao que *não* é duplicidade:** um mesmo boletim com duas naturezas apuradas gera duas linhas legítimas — é o grão do fato. Deduplicar por `num_bo` apagaria crimes.

**Evidência:** no arquivo de 2026, 66 boletins de Sorocaba aparecem em mais de uma linha, com até quatro naturezas no mesmo boletim.

### T8. Junção com os dados de referência do IBGE

É a "junção e conciliação de dois conjuntos de dados diferentes" que o descritivo cita como exemplo. Ela acontece em dois pontos:

1. **Validação do município.** O código IBGE presente nos dados criminais é confrontado com a lista oficial da API de Localidades. É o critério de qualidade da Aula 2 de Governança: "a cidade informada precisa ser uma dos cerca de 5.570 municípios existentes no Brasil".

2. **Cálculo da taxa por 100 mil habitantes.** O fato de ocorrências e o fato de população são cruzados pela dimensão conformada `dim_municipio` e pelo ano.

**Conciliação necessária:** a série de população do IBGE tem lacunas. Há Censo para 2022 e estimativas para 2024 e 2025, mas **2023 e 2026 não têm número oficial publicado**. O tratamento está declarado em [`sql/25_carga_populacao.sql`](../sql/25_carga_populacao.sql): 2023 recebe interpolação linear entre os dois anos oficiais vizinhos, marcada como `interpolado` na coluna `origem_populacao`; 2026 fica de fora, por não ter população publicada e por estar incompleto na base de ocorrências. A apostila de Qualidade trata o preenchimento de ausentes como transformação legítima *desde que registrada* — e é a coluna de origem que a registra.

---

## Carga do esquema estrela

A carga é feita em SQL, na ordem dos scripts numerados, e é **idempotente**: cada script esvazia a tabela antes de recarregá-la, de modo que reexecutar o pipeline produz exatamente o mesmo resultado.

| Script | O que faz |
|---|---|
| [`10_ddl_dw.sql`](../sql/10_ddl_dw.sql) | cria as sete dimensões e os dois fatos, com chaves primárias e estrangeiras e a descrição de cada coluna |
| [`15_de_para_natureza.sql`](../sql/15_de_para_natureza.sql) | tabela de referência que classifica cada natureza por título do Código Penal |
| [`20_carga_dimensoes.sql`](../sql/20_carga_dimensoes.sql) | `INSERT ... SELECT DISTINCT` com geração das chaves surrogate |
| [`21_carga_fato.sql`](../sql/21_carga_fato.sql) | `INSERT ... SELECT` com junção às dimensões |
| [`25_carga_populacao.sql`](../sql/25_carga_populacao.sql) | carga do segundo fato e tratamento das lacunas de população |

Três detalhes de implementação valem registro:

**Linha "não informado" em toda dimensão.** Cada dimensão tem uma linha de chave `-1`. Sem ela, um fato sem bairro ficaria órfão e **desapareceria** de qualquer consulta com junção interna — falseando as contagens de forma silenciosa.

**Junções por `LEFT JOIN` com `IFNULL(..., -1)`.** Se um valor não encontrar par na dimensão, o fato continua existindo e aponta para "não informado", em vez de sumir. Quantos fatos caíram nessa situação é medido explicitamente na análise de qualidade.

**Restrições declaradas, verificação explícita.** As chaves primárias e estrangeiras são declaradas no DDL como `NOT ENFORCED`: no BigQuery elas documentam o modelo e informam o otimizador, mas não são verificadas linha a linha na carga. A integridade referencial é, portanto, verificada por consulta em [`sql/30_qualidade.sql`](../sql/30_qualidade.sql) — declarar a restrição sem verificá-la seria pior do que não declará-la.

---

**Anterior:** [4. Catálogo de dados](04-catalogo-de-dados.md) · **Próximo:** [6. Qualidade de dados](06-qualidade-de-dados.md)
