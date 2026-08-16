# 4. Catálogo de dados

O descritivo exige um catálogo "contendo minimamente uma descrição detalhada dos dados e seus domínios, contendo valores mínimos e máximos esperados para dados numéricos, e possíveis categorias para dados categóricos", que descreva também "a linhagem dos dados, de onde os mesmos foram baixados e qual técnica foi utilizada para compor o conjunto de dados".

A estrutura deste catálogo segue **exatamente** a lista de metadados da Aula 2 de Governança de Dados: Nome · Versão · Descrição · Estrutura · Tabelas (colunas, tipos e domínios) · Chaves identificadoras e de ligação · Granularidade · Temporalidade · Origem · Licença de uso · Transformações realizadas · Comentários.

> Todas as descrições e domínios abaixo estão também gravados **dentro do BigQuery**, na cláusula `OPTIONS(description=...)` de cada tabela e de cada coluna ([`sql/10_ddl_dw.sql`](../sql/10_ddl_dw.sql)). O catálogo vive junto do dado, e não apenas neste repositório.

---

# Parte A — Conjunto de dados de origem

## A.1 SPDados Criminais (SSP-SP)

| Metadado | Conteúdo |
|---|---|
| **Nome** | SPDados — Dados Criminais do Estado de São Paulo |
| **Versão** | arquivos anuais 2022 a 2026, coletados em agosto de 2026 (última republicação da fonte: 30/06/2026 para 2022–2025 e 30/07/2026 para 2026) |
| **Descrição** | Registros de ocorrências criminais lavradas em boletim de ocorrência no estado de São Paulo, com a natureza criminal apurada e auditada pela SSP-SP, o local e o momento do fato, e a unidade policial responsável pela área. |
| **Estrutura** | Um arquivo `.xlsx` por ano. Cada arquivo tem uma aba de dicionário de campos e uma ou duas abas de dados (semestrais). 29 colunas até 2024, 30 a partir de 2025. |
| **Granularidade** | Uma natureza criminal apurada em um boletim de ocorrência. Um boletim com duas naturezas gera duas linhas. |
| **Temporalidade** | O arquivo de cada ano é fechado pelo **ano de entrada na estatística oficial**, não pelo ano do fato: ele contém ocorrências de anos anteriores. No arquivo de 2026 há fatos ocorridos desde 2006. |
| **Origem** | `https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ano}.xlsx` — download direto dos arquivos publicados pela Secretaria, sem raspagem de páginas. |
| **Licença de uso** | Dados abertos, publicados oficialmente pela SSP-SP no portal de transparência do Governo do Estado de São Paulo. Uso livre com citação da fonte. |
| **Volume** | 5.348.378 registros nos cinco anos (estado inteiro); 73.395 de Sorocaba |

### Colunas, tipos e domínios

Descrições reproduzidas do dicionário publicado pela própria SSP-SP dentro do arquivo; domínios apurados sobre os dados de Sorocaba.

| Campo (grafia mais recente) | Tipo | Descrição da fonte | Domínio observado |
|---|---|---|---|
| `NOME_DEPARTAMENTO` | texto | Departamento responsável pelo registro | 11 valores; predomina `DIPOL - DEPTO DE INTELIGENCIA` (registros eletrônicos) |
| `NOME_SECCIONAL` | texto | Delegacia seccional responsável pelo registro | 29 valores |
| `NOME_DELEGACIA` | texto | Delegacia responsável pelo registro | 81 valores; predomina `DELEGACIA ELETRONICA 3` |
| `NOME_MUNICIPIO` *(2022: `CIDADE`)* | texto | Município de registro | 6 valores nos registros de Sorocaba (o boletim pode ser registrado em outra cidade) |
| `NUM_BO` | texto | Número do boletim de ocorrência | código alfanumérico de 6 posições |
| `ANO_BO` | inteiro | Ano do BO | 2006 a 2026 |
| `DATA_REGISTRO` *(2022: `DATA_COMUNICACAO_BO`)* | data | Data do registro da ocorrência | dentro do ano do arquivo |
| `DATA_OCORRENCIA_BO` | data | Data da ocorrência | 2006-07-02 a 2026-06-30 |
| `HORA_OCORRENCIA_BO` | hora | Hora da ocorrência | 00:00:00 a 23:59:59; **`NULL` em 33% dos registros** |
| `DESC_PERIODO` *(2022: `DESCR_PERIODO`)* | texto | Período da ocorrência | `De madrugada`, `Pela manhã`, `A tarde`, `A noite`, `Em hora incerta`; **`NULL` em 54% dos registros** |
| `DESCR_TIPOLOCAL` | texto | Grupo de tipos de locais onde se deu o fato | 29 valores; predominam `Via Pública` e `Residência`. **Ausente nos arquivos de 2022 a 2024** |
| `DESCR_SUBTIPOLOCAL` | texto | Subgrupo de tipos de locais, vinculado ao tipo de local | 138 valores em Sorocaba (302 no estado) |
| `BAIRRO` | texto | Bairro da ocorrência | ~900 grafias distintas; sentinelas `-` e vazio |
| `LOGRADOURO` | texto | Endereço dos fatos | **não carregado no DW** (decisão D1); 26% dos valores são `VEDAÇÃO DA DIVULGAÇÃO DOS DADOS RELATIVOS` |
| `NUMERO_LOGRADOURO` | texto | Número do logradouro dos fatos | **não carregado no DW** (decisão D1) |
| `LATITUDE` | decimal | Latitude da ocorrência | -23,60 a -23,35 em Sorocaba; sentinelas `0`, `-` e `NULL` |
| `LONGITUDE` | decimal | Longitude da ocorrência | -47,60 a -47,35 em Sorocaba; mesmas sentinelas |
| `NOME_DELEGACIA_CIRCUNSCRICAO` | texto | Delegacia de circunscrição (local onde se deu o fato) | 11 distritos policiais de Sorocaba |
| `NOME_DEPARTAMENTO_CIRCUNSCRICAO` | texto | Departamento de circunscrição | `DEINTER 7 - SOROCABA` |
| `NOME_SECCIONAL_CIRCUNSCRICAO` | texto | Seccional de circunscrição | `DEL.SEC.SOROCABA` |
| `NOME_MUNICIPIO_CIRCUNSCRICAO` | texto | Município da delegacia de circunscrição | `SOROCABA` |
| `RUBRICA` | texto | Natureza jurídica da ocorrência | ~40 valores, com o artigo do Código Penal |
| `DESCR_CONDUTA` | texto | Parágrafos, incisos ou circunstâncias relacionadas à rubrica | 32 valores; `NULL` em 31% |
| `NATUREZA_APURADA` | texto | Classificação da natureza criminal verificada e validada pela equipe da SPDados, em conjunto com as Polícias Civil e Militar, mediante processo de análise e auditoria dos registros | **28 grafias que representam 23 naturezas** (ver T4) |
| `MES_ESTATISTICA` | inteiro | Mês em que a ocorrência foi inserida na estatística oficial | 1 a 12 |
| `ANO_ESTATISTICA` | inteiro | Ano em que a ocorrência foi inserida na estatística oficial | igual ao ano do arquivo |
| `CMD` | texto | Área do Comando da Polícia Militar | `CPI-7` em Sorocaba |
| `BTL` | texto | Área do Batalhão da Polícia Militar | `7ºBPM/I` e `55ºBPM/I` |
| `CIA` | texto | Área da Companhia da Polícia Militar | 5 valores |
| `COD IBGE` *(2022–2025: `CD_IBGE`)* | texto | Identificador único do código do Município da Federação | `3552205` para Sorocaba; **acompanha a circunscrição** |

### Chaves identificadoras e de ligação

- **Identificadora:** `NUM_BO` + `ANO_BO` identificam o boletim, mas **não a linha** — o mesmo boletim pode ter várias naturezas. A linha é identificada pela combinação do boletim com a rubrica, a natureza e a conduta.
- **De ligação:** `COD IBGE` liga ao conjunto de dados do IBGE.

### Comentários

Duas características da fonte afetam qualquer leitura dos números e não são erro:

1. A base registra **ocorrências comunicadas**, não crimes ocorridos. Variações podem refletir mudança na criminalidade ou na propensão a registrar.
2. O ano do arquivo é o da **estatística**, não o do fato.

---

## A.2 Dados de referência do IBGE

| Metadado | Conteúdo |
|---|---|
| **Nome** | Município de Sorocaba (Localidades) e População residente (SIDRA) |
| **Versão** | consulta de agosto de 2026 |
| **Descrição** | Código, nome e hierarquia regional oficial do município; população residente por ano. |
| **Estrutura** | JSON. Localidades: um objeto aninhado. SIDRA: lista de registros, o primeiro sendo o cabeçalho. |
| **Granularidade** | Localidades: um município. SIDRA: município × ano. |
| **Temporalidade** | Censo: 2022. Estimativas: 2016–2021, 2024, 2025. **Sem publicação para 2023 e 2026.** |
| **Origem** | `servicodados.ibge.gov.br/api/v1/localidades/municipios/3552205` · `apisidra.ibge.gov.br/values/t/6579/...` · `apisidra.ibge.gov.br/values/t/4709/...` |
| **Licença de uso** | Dados abertos do IBGE; uso livre com citação da fonte. |
| **Transformações** | Extração dos campos de interesse; interpolação linear da população de 2023 (decisão D4). |

**Domínios:** `cod_ibge` = `3552205`; `uf` = `SP`; `regiao_imediata` = `Sorocaba`; `populacao` entre 723.682 (Censo 2022) e 762.172 (estimativa 2025).

---

# Parte B — Data warehouse

| Metadado | Conteúdo |
|---|---|
| **Nome** | `dw` — Data warehouse de ocorrências criminais de Sorocaba |
| **Versão** | 1.0 — carga de agosto de 2026 |
| **Descrição** | Esquema estrela com o histórico de ocorrências criminais registradas com circunscrição em Sorocaba entre 2022 e 2026, e a população do município por ano. |
| **Estrutura** | 7 dimensões + 2 tabelas fato + 1 tabela de referência, em BigQuery (`southamerica-east1`) |
| **Granularidade** | `fato_ocorrencia`: uma natureza apurada em um boletim. `fato_populacao_anual`: município × ano. |
| **Temporalidade** | Fatos de 2022 a 2026 pela data da estatística; datas de ocorrência de 2006 a 2026. População de 2022 a 2025. |
| **Origem** | derivado de A.1 e A.2 pelo pipeline documentado em [`05-carga-etl.md`](05-carga-etl.md) |
| **Licença de uso** | herda as licenças das fontes: dados abertos, uso livre com citação |
| **Transformações** | oito transformações documentadas em [`05-carga-etl.md`](05-carga-etl.md), com a linhagem em nível de coluna em [`09-linhagem.md`](09-linhagem.md) |

## B.1 `fato_ocorrencia`

**Granularidade:** uma natureza criminal apurada em um boletim de ocorrência com circunscrição em Sorocaba.

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| `sk_ocorrencia` | INT64 | Chave surrogate do fato | 1 a N, sequencial |
| `sk_tempo_ocorrencia` | INT64 | FK → `dim_tempo`: data do fato | chave válida ou -1 |
| `sk_tempo_estatistica` | INT64 | FK → `dim_tempo`: 1º dia do mês da estatística oficial | chave válida ou -1 |
| `sk_periodo_dia` | INT64 | FK → `dim_periodo_dia` | chave válida ou -1 |
| `sk_natureza` | INT64 | FK → `dim_natureza` | chave válida ou -1 |
| `sk_local` | INT64 | FK → `dim_local` | chave válida ou -1 |
| `sk_bairro` | INT64 | FK → `dim_bairro` | chave válida ou -1 |
| `sk_delegacia` | INT64 | FK → `dim_delegacia` | chave válida ou -1 |
| `sk_area_pm` | INT64 | FK → `dim_area_pm` | chave válida ou -1 |
| `sk_municipio` | INT64 | FK → `dim_municipio` | chave válida ou -1 |
| `num_bo` | STRING | Número do boletim — dimensão degenerada | alfanumérico de 6 posições |
| `ano_bo` | INT64 | Ano do boletim — dimensão degenerada | 2006 a 2026 |
| `latitude` | FLOAT64 | Latitude do local do fato | -23,60 a -23,35; nulo quando ausente |
| `longitude` | FLOAT64 | Longitude do local do fato | -47,60 a -47,35; nulo quando ausente |
| `tem_geolocalizacao` | BOOL | Coordenadas válidas | verdadeiro/falso |
| `registrado_em_outro_municipio` | BOOL | Boletim registrado fora de Sorocaba | verdadeiro/falso |
| `qtd_ocorrencia` | INT64 | **Medida aditiva** | sempre 1 |

**Chave identificadora:** `sk_ocorrencia`. **Chave natural do grão:** `num_bo` + `ano_bo` + todas as chaves estrangeiras.

## B.2 `dim_tempo`

**Granularidade:** um dia. **Cobertura:** 2006-01-01 a 2026-12-31, mais a linha -1.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_tempo` | INT64 | 1 a 7.670; -1 para não informado |
| `data` | DATE | 2006-01-01 a 2026-12-31 |
| `ano` | INT64 | 2006 a 2026 |
| `semestre` | INT64 | 1, 2 |
| `trimestre` | INT64 | 1 a 4 |
| `mes` | INT64 | 1 a 12 |
| `nome_mes` | STRING | Janeiro … Dezembro |
| `ano_mes` | STRING | `AAAA-MM` |
| `dia` | INT64 | 1 a 31 |
| `dia_semana` | INT64 | 1 (domingo) a 7 (sábado) |
| `nome_dia_semana` | STRING | Domingo … Sábado |
| `fim_de_semana` | BOOL | verdadeiro para sábado e domingo |

## B.3 `dim_periodo_dia`

**Granularidade:** combinação de hora cheia e período observada nos dados.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_periodo_dia` | INT64 | sequencial; -1 para não informado |
| `hora` | INT64 | 0 a 23; nulo quando a fonte não informou |
| `faixa_horaria` | STRING | `00h-02h` … `21h-23h`; `Não informada` |
| `periodo` | STRING | `DE MADRUGADA`, `PELA MANHA`, `A TARDE`, `A NOITE`, `EM HORA INCERTA`, `NAO INFORMADO` |
| `hora_informada` | BOOL | verdadeiro quando a fonte publicou a hora |

## B.4 `dim_natureza`

**Granularidade:** combinação de categoria, natureza apurada, rubrica e conduta.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_natureza` | INT64 | sequencial; -1 para não informado |
| `categoria` | STRING | `PATRIMONIO`, `PESSOA`, `DIGNIDADE SEXUAL`, `TRANSITO`, `DROGAS E ARMAS`, `OUTROS` |
| `natureza_apurada` | STRING | 23 valores padronizados (ver lista completa em [`15_de_para_natureza.sql`](../sql/15_de_para_natureza.sql)) |
| `rubrica` | STRING | ~40 valores, com o artigo do Código Penal, na grafia da fonte |
| `conduta` | STRING | 32 valores; `NAO INFORMADO` quando ausente |
| `crime_violento` | BOOL | verdadeiro para naturezas com violência ou grave ameaça |

## B.5 `dim_local`

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_local` | INT64 | sequencial; -1 para não informado |
| `tipo_local` | STRING | 29 valores: `Via Pública`, `Residência`, `Comércio e Serviços`, `Terminal/Estação`, … |
| `subtipo_local` | STRING | 138 valores em Sorocaba |
| `origem_tipo_local` | STRING | `publicado pela fonte`, `derivado do subtipo`, `não informado` |
| `tipo_local_ambiguo` | BOOL | verdadeiro quando o subtipo admite mais de um tipo |

## B.6 `dim_bairro`

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_bairro` | INT64 | sequencial; -1 para não informado |
| `nome_bairro` | STRING | ~900 bairros padronizados em maiúsculas sem acento; `NAO INFORMADO` |
| `bairro_informado` | BOOL | verdadeiro quando a fonte informou o bairro |

## B.7 `dim_delegacia`

**Hierarquia:** departamento → seccional → delegacia. Sempre a **circunscrição** do fato.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_delegacia` | INT64 | sequencial; -1 para não informado |
| `delegacia` | STRING | 11 distritos policiais de Sorocaba |
| `seccional` | STRING | `DEL.SEC.SOROCABA` |
| `departamento` | STRING | `DEINTER 7 - SOROCABA` |

## B.8 `dim_area_pm`

**Hierarquia:** comando → batalhão → companhia.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_area_pm` | INT64 | sequencial; -1 para não informado |
| `companhia` | STRING | 5 companhias |
| `batalhao` | STRING | `7ºBPM/I`, `55ºBPM/I` |
| `comando` | STRING | `CPI-7` |

## B.9 `dim_municipio`

Dimensão **conformada**, compartilhada pelos dois fatos. Carregada do dado de referência do IBGE.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_municipio` | INT64 | sequencial; -1 para não informado |
| `cod_ibge` | STRING | `3552205` |
| `nome_municipio` | STRING | `Sorocaba` |
| `uf` | STRING | `SP` |
| `regiao_imediata` | STRING | `Sorocaba` |
| `regiao_intermediaria` | STRING | `Sorocaba` |
| `mesorregiao` | STRING | `Macro Metropolitana Paulista` |
| `microrregiao` | STRING | `Sorocaba` |

## B.10 `fato_populacao_anual`

**Granularidade:** município × ano.

| Coluna | Tipo | Domínio |
|---|---|---|
| `sk_municipio` | INT64 | FK → `dim_municipio` |
| `ano` | INT64 | 2022 a 2025 |
| `populacao` | INT64 | 723.682 a 762.172 |
| `origem_populacao` | STRING | `censo` (2022), `interpolado` (2023), `estimativa` (2024, 2025) |

## B.11 `de_para_natureza`

Tabela de referência da classificação criminal.

| Coluna | Tipo | Domínio |
|---|---|---|
| `natureza_apurada` | STRING | 23 naturezas padronizadas — chave primária |
| `categoria` | STRING | 5 categorias (ver B.4) |
| `crime_violento` | BOOL | verdadeiro/falso |

---

## Mapa de chaves de ligação do modelo

| De | Para | Chave |
|---|---|---|
| `fato_ocorrencia` | `dim_tempo` | `sk_tempo_ocorrencia` e `sk_tempo_estatistica` |
| `fato_ocorrencia` | demais dimensões | `sk_<dimensão>` |
| `fato_populacao_anual` | `dim_municipio` | `sk_municipio` |
| `dim_natureza` | `de_para_natureza` | `natureza_apurada` |
| DW | dados do IBGE | `cod_ibge` |

Todas as chaves primárias e estrangeiras estão **declaradas no DDL** como `NOT ENFORCED`. O BigQuery não as verifica automaticamente; a verificação é feita explicitamente em [`sql/30_qualidade.sql`](../sql/30_qualidade.sql).

---

**Anterior:** [3. Modelagem](03-modelagem.md) · **Próximo:** [5. Carga](05-carga-etl.md)
