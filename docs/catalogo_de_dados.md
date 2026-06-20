# Catálogo de Dados — Mapa de Risco Criminal em Sorocaba

## 1. Visão geral

Este catálogo documenta o modelo dimensional (Esquema Estrela) construído a partir
dos dados públicos de ocorrências criminais da Secretaria de Segurança Pública do
Estado de São Paulo (SSP-SP), filtrados para o município de Sorocaba.

| | |
|---|---|
| **Fonte original** | Portal de Dados Abertos SP / SSP-SP — dataset "Dados Criminais" (Números Sem Mistério) |
| **Licença** | Creative Commons Attribution 4.0 (CC-BY 4.0) |
| **URL de origem** | `https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ANO}.xlsx` |
| **Período coberto** | 2022-01-01 a 2026-04-30 (5 arquivos anuais; 2026 parcial: Jan-Abr) |
| **Granularidade do dado bruto** | 1 linha = 1 rubrica registrada em 1 Boletim de Ocorrência |
| **Abrangência bruta** | Todo o Estado de SP (o recorte de Sorocaba é aplicado na Silver) |

## 2. Linhagem (visão geral do pipeline)

```
SSP-SP (URL pública, 1 xlsx/ano, todo o Estado de SP)
  -> DOWNLOAD (urllib, /tmp do driver serverless; ~190 MB por arquivo)
  -> CONVERSÃO IN-MEMORY (openpyxl streaming, lotes de 50 k linhas
     → pandas DataFrame → spark.createDataFrame() → Delta staging temporária)
  -> BRONZE (Delta; schema original preservado + colunas de auditoria;
     escrita incremental por _ano_arquivo via replaceWhere)
  -> SILVER (Delta; schema reconciliado por coalesce, tipagem, limpeza de
     sentinelas, filtro município = Sorocaba; particionado por ano_mes_ocorrencia)
  -> GOLD (Delta; Esquema Estrela — 1 fato + 3 dimensões)
```

**Por que não há etapa de Landing em Parquet:** o Databricks Free Edition desativou
o DBFS público e bloqueia acesso do Spark a caminhos locais (`/tmp`). A solução é
converter o xlsx em memória — openpyxl streaming em lotes de 50 k linhas, cada lote
convertido para pandas e ingerido via `spark.createDataFrame()`. O pico de memória
fica limitado a um lote por vez; a escrita intermediária usa Delta staging (tabela
temporária no catálogo, descartada após a carga do ano). O único artefato persistente
é a tabela Bronze Delta no Unity Catalog (`workspace.sorocaba_seguranca.bronze`).

A reconciliação de schema entre anos é necessária porque a fonte alterou nomes de
coluna e adicionou um campo (`DESCR_TIPOLOCAL`) ao longo do tempo — ver Seção 6.

## 3. Dicionário oficial dos campos da fonte

Reproduzido da aba `CAMPOS_DA_TABELA_SPDADOS`, presente em todos os arquivos da SSP-SP.
*A própria fonte documenta: "Resultado Null indica que o campo se encontrava vazio no banco de dados".*

| Campo (origem) | Descrição oficial |
|---|---|
| `NOME_DEPARTAMENTO` | Departamento responsável pelo registro |
| `NOME_SECCIONAL` | Delegacia seccional responsável pelo registro |
| `NOME_DELEGACIA` | Delegacia responsável pelo registro |
| `NOME_MUNICIPIO` (2022: `CIDADE`) | Município de registro |
| `NUM_BO` | Número do boletim de ocorrência |
| `ANO_BO` | Ano do BO |
| `DATA_REGISTRO` (2022: `DATA_COMUNICACAO_BO`) | Data do registro da ocorrência |
| `DATA_OCORRENCIA_BO` | Data da ocorrência |
| `HORA_OCORRENCIA_BO` | Hora da ocorrência |
| `DESC_PERIODO` (2022: `DESCR_PERIODO`) | Período da ocorrência |
| `DESCR_TIPOLOCAL` | Grupo de tipos de locais onde se deu o fato (**só a partir de 2025**) |
| `DESCR_SUBTIPOLOCAL` | Subgrupo de tipos de locais, vinculado ao tipo de local |
| `BAIRRO` | Bairro da ocorrência |
| `LOGRADOURO` | Endereço dos fatos |
| `NUMERO_LOGRADOURO` | Número do logradouro dos fatos |
| `LATITUDE` / `LONGITUDE` | Coordenadas da ocorrência |
| `*_CIRCUNSCRICAO` | Unidade (delegacia/seccional/depto/município) de circunscrição — local do fato |
| `RUBRICA` | Natureza jurídica da ocorrência |
| `DESCR_CONDUTA` | Parágrafos, incisos ou circunstâncias relacionadas à rubrica |
| `NATUREZA_APURADA` | Classificação criminal verificada e auditada pela SPDados + Polícias Civil e Militar |
| `MES_ESTATISTICA` / `ANO_ESTATISTICA` | Mês/ano em que a ocorrência entrou na estatística oficial (= mês/ano do **registro**) |
| `CMD` / `BTL` / `CIA` | Área de Comando / Batalhão / Companhia da Polícia Militar (local do fato) |
| `COD IBGE` | Código IBGE do município |

> **Semântica importante:** `ANO_ESTATISTICA` é o ano do **registro**, não o ano da
> **ocorrência**. Por isso há fatos de dez/2021 (e anteriores) no arquivo de 2022 —
> são ocorrências antigas registradas em 2022. As análises temporais deste MVP usam
> `dt_ocorrencia_bo` (data do fato); a estatística oficial da SSP-SP usa o ano de
> registro. Os dois números podem divergir e isso é esperado.

## 4. Tabela Fato: `fato_ocorrencia`

**Grão:** uma linha por rubrica registrada em um BO envolvendo Sorocaba.

| Coluna | Tipo | Domínio / Regra | Descrição |
|---|---|---|---|
| `num_bo` | string | Alfanumérico, ex.: `AX8110` | Número do BO (não é chave única isolada — ver nota) |
| `ano_bo` | int | 2022–2026 | Ano de emissão do BO |
| `id_data` | int (FK → dim_data) | `yyyyMMdd`, ex.: `20260117` | Data da **ocorrência** do fato |
| `id_local` | long (FK → dim_local) | Surrogate | Bairro da ocorrência |
| `id_tipo_ocorrencia` | long (FK → dim_tipo_ocorrencia) | Surrogate | Classificação do fato |
| `hora_ocorrencia_bo` | string | `HH:MM:SS` ou nulo | Hora do fato (nulo pode ser vedação proposital — ver Seção 7) |
| `desc_periodo` | string | `{De madrugada, Pela manhã, A tarde, A noite, Em hora incerta}` | Período do dia, normalizado |
| `logradouro` | string | Texto livre, pode ser nulo | Endereço do fato (atributo degenerado) |
| `numero_logradouro` | int | ≥ 0 ou nulo | Número do logradouro (atributo degenerado) |
| `latitude` | double | aprox. -23.65 a -23.35 ou nulo | Coordenada do evento (degenerada; nulo = sem geo, inclui sentinela `0` tratado) |
| `longitude` | double | aprox. -47.55 a -47.30 ou nulo | Coordenada do evento (degenerada) |
| `mes_estatistica` | int | 1–12 | Mês de registro na estatística oficial |
| `ano_estatistica` | int | 2022–2026 | Ano de registro (= ano do arquivo) |
| `ano_mes_ocorrencia` | string | `yyyyMM`, ex.: `202603`; `000000` quando nulo | Partição da tabela (derivado de `dt_ocorrencia_bo`; sentinel `000000` evita FK nula) |
| `quantidade` | int | Sempre `1` | Medida aditiva (`SUM(quantidade)` = `COUNT(*)`) |

**Nota de unicidade:** `num_bo` isolado NÃO é chave única — um mesmo BO pode conter
múltiplas rubricas (ex.: roubo + porte ilegal de arma), gerando várias linhas para o
mesmo `num_bo`. É granularidade esperada, não duplicidade (testado no notebook 02).

**Decisão de modelagem (lat/long/logradouro na fato):** coordenadas e endereço são
atributos de altíssima cardinalidade (quase únicos por evento). Mantê-los como
*dimensão degenerada* na fato — em vez de inflar uma `dim_local` a quase 1 linha por
fato — é a prática correta de Data Warehouse.

## 5. Dimensões

### 5.1 `dim_data`

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `id_data` | int (PK) | `yyyyMMdd` | Surrogate baseada na data |
| `data` | date | 2021-* a 2026-04-30 | Data civil da ocorrência |
| `ano` / `mes` / `dia` | int | — / 1–12 / 1–31 | Componentes da data |
| `trimestre` | int | 1–4 | Trimestre civil |
| `dia_semana_num` | int | 1–7 (1=domingo, convenção Spark `dayofweek`) | Dia da semana numérico |
| `dia_semana_nome` | string | `{Domingo..Sábado}` | Dia da semana por extenso |
| `fim_de_semana` | boolean | `{true, false}` | `true` se sábado/domingo |

### 5.2 `dim_local`

**Grão:** bairro (normalizado: sem acento, sem espaços nas pontas, caixa alta).

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `id_local` | long (PK) | Surrogate | Identificador do bairro |
| `bairro` | string | Texto normalizado; `NÃO INFORMADO` quando ausente | Bairro da ocorrência |
| `latitude_centroide` | double | aprox. -23.65 a -23.35 | Média das coordenadas válidas do bairro (ponto representativo p/ mapa) |
| `longitude_centroide` | double | aprox. -47.55 a -47.30 | Idem longitude |
| `qtd_ocorrencias` | long | ≥ 1 | Volume de ocorrências do bairro (pré-agregado, conveniência) |

**Normalização de `bairro`:** aplicada na Gold para unir variações de grafia
(`Centro` vs `CENTRO`). A Silver mantém o `bairro` cru, de propósito, para que a
análise de qualidade (notebook 02, Seção 7) consiga evidenciar as variações.

### 5.3 `dim_tipo_ocorrencia`

| Coluna | Tipo | Domínio | Descrição |
|---|---|---|---|
| `id_tipo_ocorrencia` | long (PK) | Surrogate | Identificador do tipo |
| `rubrica` | string | Ex.: `Furto (art. 155)`, `Roubo (art. 157)` | Natureza jurídica/penal |
| `natureza_apurada` | string | Ex.: `FURTO - OUTROS` | Classificação auditada (mais confiável que `rubrica` p/ estatística) |
| `descr_tipolocal` | string | Ex.: `Via Pública`, `Residência`, `Shopping Center`, `Comércio e Serviços`, `Centro Comercial/Empresarial`; `NÃO INFORMADO` antes de 2025 | Grupo de tipo de local |
| `descr_subtipolocal` | string | Subtipo, mais granular; `NÃO INFORMADO` quando ausente | Subtipo de local |

**Membro `NÃO INFORMADO`:** chaves naturais nulas são substituídas por este membro
sentinela ANTES de construir a dimensão e ligar o fato. Sem isso, o join `NULL = NULL`
não casaria no Spark e descartaria silenciosamente todos os registros de 2022–2024
(onde `descr_tipolocal` é nulo) — ver decisão na Seção 7.

## 6. Mapa de reconciliação de schema (linhagem detalhada)

Confirmado empiricamente sobre os 5 arquivos (inspeção de cabeçalhos + amostras em
[`data/schema_samples/`](../data/schema_samples)). A reconciliação é feita por **nome**
de coluna, nunca por posição — a ordem muda entre anos (`DESCR_TIPOLOCAL` entrou em
2025, deslocando as colunas seguintes). Implementada na Silver via `coalesce`.

| Coluna canônica | 2022 | 2023–2024 | 2025–2026 |
|---|---|---|---|
| `municipio` | `CIDADE` | `NOME_MUNICIPIO` | `NOME_MUNICIPIO` |
| `dt_registro_bo` | `DATA_COMUNICACAO_BO` | `DATA_REGISTRO` | `DATA_REGISTRO` |
| `desc_periodo` | `DESCR_PERIODO` | `DESC_PERIODO` | `DESC_PERIODO` |
| `descr_tipolocal` | *(inexistente)* | *(inexistente)* | `DESCR_TIPOLOCAL` |
| `municipio_circunscricao` | `NOME_MUNICIPIO_CIRCUNSCRIÇÃO` | `NOME_MUNICIPIO_CIRCUNSCRIÇÃO` | `NOME_MUNICIPIO_CIRCUNSCRICAO` (sem cedilha) |
| *(demais ~24 colunas)* | nomes idênticos em todos os anos | | |

Confirmações da inspeção: 2022–2024 têm **29 colunas**, 2025–2026 têm **30** (entrada
de `DESCR_TIPOLOCAL`). As colunas de circunscrição mudam a grafia da cedilha
(`Ç` → `C`) a partir de 2025.

## 7. Qualidade de dados — problemas identificados e tratamento

| Problema | Onde | Tratamento (camada Silver) |
|---|---|---|
| Sentinela `0` (int) em `LATITUDE`/`LONGITUDE` | Todos os anos | `0`/`0.0` → nulo (Sorocaba nunca tem lat 0) |
| Sentinelas textuais `'NULL'`, `'-'`, `'(Vazio)'`, `''` | `LATITUDE`, `LONGITUDE`, `HORA_OCORRENCIA_BO`, `COD IBGE` etc. | Convertidos para nulo real antes da tipagem |
| Espaço à direita em `"SOROCABA "` | Guias `JAN-JUN_2022`, `JUL-DEZ_2023` | `strip` + `upper` + remoção de acento antes do filtro de município |
| Grafia de `DESC_PERIODO` (`EM HORA INCERTA` vs `Em hora incerta`) | 2022 vs 2025+ | Mapeado para conjunto canônico |
| Variação de grafia de `BAIRRO` (`Centro`/`CENTRO`) | Vários | Normalizado na construção da `dim_local` |
| Coluna ausente entre anos (`DESCR_TIPOLOCAL`) | 2022–2024 | `mergeSchema` na Bronze (vira nulo) + membro `NÃO INFORMADO` na Gold |
| FK nula por `NULL = NULL` não casar em join | Gold (todos < 2025) | Membro sentinela `NÃO INFORMADO` nas chaves antes do join |

> **Correção importante de uma suposição inicial:** a inspeção do dado real mostrou
> que as datas vêm como **datetime nativo do Excel** (renderizadas para `yyyy-MM-dd`
> na conversão), e **não** no formato `M/D/YY` que se supunha de início. O parse na
> Silver usa `to_date(col, "yyyy-MM-dd")`. Da mesma forma, latitude/longitude já vêm
> com **ponto** decimal (lidas como número), tornando desnecessária a troca de vírgula
> por ponto que se previu inicialmente — o problema real era o sentinela `0`.

Detalhamento estatístico (percentuais por coluna, contagem de duplicidade, outliers)
fica no notebook [`02_qualidade_dados`](../notebooks/02_qualidade_dados.py) — o catálogo
referencia a metodologia; o notebook contém os números vivos.

## 7.1 Relevância das colunas para o objetivo do MVP

| Coluna | Pergunta(s) de negócio que sustenta |
|---|---|
| `bairro` (dim_local) | 1, 3, 6 |
| `dim_data` (ano, mes, dia_semana) | 1, 2, 4 |
| `hora_ocorrencia_bo` / `desc_periodo` | 2 (com ressalva de cobertura) |
| `rubrica` / `natureza_apurada` (dim_tipo) | 3, 4, 6 |
| `descr_tipolocal` | 5 (só a partir de 2025) |
| `latitude` / `longitude` (fato) | 1, 6 (mapas de calor) |

## 8. Glossário de termos do domínio

| Termo | Significado |
|---|---|
| BO | Boletim de Ocorrência |
| Rubrica | Classificação penal/jurídica atribuída no registro do BO |
| Natureza apurada | Classificação final, auditada pela SSP-SP + Polícias — mais confiável que a rubrica inicial |
| Circunscrição | Unidade responsável territorialmente pelo local do fato (pode diferir de quem registrou) |
| Ano estatística | Ano em que a ocorrência entrou na estatística oficial (= ano do registro) |
| Dimensão degenerada | Atributo de alta cardinalidade mantido na fato (ex.: coordenadas), sem dimensão própria |
| Membro sentinela | Valor explícito (`NÃO INFORMADO`) que substitui nulo em chave de dimensão |

---

*Catálogo mantido em sincronia com os notebooks do pipeline. Os números estatísticos
vivem nos notebooks; este documento descreve estrutura, domínio e linhagem.*
