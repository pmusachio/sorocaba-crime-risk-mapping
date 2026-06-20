<div align="center">

# 🗺️ Mapa de Risco Criminal — Sorocaba/SP

**Pipeline de dados em nuvem para estruturação de ocorrências criminais públicas,
da coleta à análise, como base de um sistema de inteligência territorial de
segurança pública.**

[![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://www.databricks.com/try-databricks)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat-square&logo=delta&logoColor=white)](https://delta.io/)
[![License: CC BY 4.0](https://img.shields.io/badge/Data%20License-CC%20BY%204.0-lightgrey.svg?style=flat-square)](https://creativecommons.org/licenses/by/4.0/)

</div>

---

## Sobre o projeto

Sorocaba não possui uma ferramenta pública e granular para visualização da
distribuição espaço-temporal de ocorrências criminais. Este projeto constrói
um pipeline completo — coleta, modelagem dimensional, carga e análise — sobre
os dados abertos da Secretaria de Segurança Pública do Estado de São Paulo
(SSP-SP), filtrados para o município de Sorocaba.

O resultado é uma base analítica estruturada em **Esquema Estrela**, pronta
para consulta SQL direta, e que serve como ponto de partida para um projeto
subsequente de Machine Learning voltado à predição de ocorrências.

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────────┐
│  Databricks Jobs (schedule semanal — toda segunda 06h BRT)          │
│  Orquestração nativa — sem cluster a gerenciar (serverless)          │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
          ┌─────────────────────▼──────────────────────┐
          │  Notebook 00 — Coleta Incremental          │
          │  • HEAD request → detecta arquivo novo/    │
          │    alterado (Content-Length)                │
          │  • Download xlsx → UC Volume               │
          │    /Volumes/workspace/sorocoba_seguranca/  │
          │  • Converte xlsx → Parquet (openpyxl,      │
          │    streaming em lotes, sem OOM)             │
          │  • Grava/atualiza Bronze Delta              │
          │    (replaceWhere por _ano_arquivo)          │
          └─────────────────────┬──────────────────────┘
                                │  (Job gerencia sequência)
          ┌─────────────────────▼──────────────────────┐
          │  Notebook 01 — Silver + Gold               │
          │  • Reconciliação de schema (coalesce)      │
          │  • Tipagem, sentinelas → nulo, filtro SP   │
          │  • + coluna ano_mes_ocorrencia (yyyyMM)    │
          │  • Silver particionado por ano_mes         │
          │  • Gold: Esquema Estrela (1 fato + 3 dim)  │
          │    Fato particionado por ano_mes_ocorrencia│
          └────────────────────────────────────────────┘
```

O Spark não lê `.xlsx` nativamente; openpyxl streaming em lotes de 100 k linhas
evita OOM (pico de memória limitado a um lote por vez). Arquivos intermediários
ficam em `/tmp` do driver — o único artefato persistente é o Bronze Delta Table
no Unity Catalog (`workspace.sorocaba_seguranca`).

**Plataforma:** Databricks Free Edition (Serverless) · **Catálogo:** Unity Catalog ·
**Formato:** Delta Lake · **Particionamento:** por `ano_mes_ocorrencia` (yyyyMM derivado de `dt_ocorrencia_bo`)

## Estrutura do repositório

```
.
├── docs/
│   ├── objetivo.md                       # Problema e perguntas de negócio
│   ├── catalogo_de_dados.md              # Domínio, linhagem e regras de cada tabela
│   ├── autoavaliacao.md                  # Avaliação crítica do trabalho realizado
│   ├── RUNBOOK_DATABRICKS.md             # Passo a passo de execução no Databricks
│   └── evidencias/                       # Screenshots/vídeos de execução no Databricks
├── .github/
│   └── workflows/
│       └── pipeline_semanal.yml          # Disparador manual via GitHub Actions (backup)
├── notebooks/
│   ├── 00_coleta_incremental.py          # Coleta + Bronze (download /tmp + replaceWhere)
│   ├── 01_pipeline_bronze_silver_gold.py # Silver + Gold (esquema estrela)
│   ├── 02_qualidade_dados.py             # Análise de qualidade por atributo
│   └── 03_analise_perguntas_negocio.py   # Resposta às 6 perguntas + EDA
├── scripts/
│   ├── coletar_dados.py                  # Utilitário local (download manual, opcional)
│   ├── converter_para_parquet.py         # Utilitário local (conversão manual, opcional)
│   └── validar_municipio.py              # Utilitário de validação de schema/grafia
└── data/
    └── schema_samples/                   # Amostras pequenas de schema (dado completo não versionado)
```

## Fonte de dados

| | |
|---|---|
| **Origem** | [SSP-SP — Dados Abertos](https://www.ssp.sp.gov.br/estatistica/consultas), dataset "Números Sem Mistério" |
| **Licença** | Creative Commons Attribution 4.0 (CC-BY 4.0) |
| **Período** | 2022 a 2026 (ano corrente parcial, dados até abril/2026) |
| **Granularidade** | 1 linha = 1 rubrica registrada em 1 Boletim de Ocorrência |
| **Volume total** | ≈ 890 MB (5 arquivos anuais) |

> O dado bruto é estadual (todo o Estado de São Paulo); o recorte para
> Sorocaba é aplicado na camada Silver, não na coleta — ver
> [Catálogo de Dados](docs/catalogo_de_dados.md) para detalhes da decisão.

## Modelo de dados

```mermaid
erDiagram
    dim_data ||--o{ fato_ocorrencia : id_data
    dim_local ||--o{ fato_ocorrencia : id_local
    dim_tipo_ocorrencia ||--o{ fato_ocorrencia : id_tipo_ocorrencia

    fato_ocorrencia {
        string num_bo
        int    id_data FK
        long   id_local FK
        long   id_tipo_ocorrencia FK
        string hora_ocorrencia_bo
        string desc_periodo
        double latitude
        double longitude
        int    ano_estatistica
        int    quantidade
    }
    dim_data {
        int     id_data PK
        date    data
        int     ano
        int     mes
        string  dia_semana_nome
        boolean fim_de_semana
    }
    dim_local {
        long   id_local PK
        string bairro
        double latitude_centroide
        double longitude_centroide
    }
    dim_tipo_ocorrencia {
        long   id_tipo_ocorrencia PK
        string rubrica
        string natureza_apurada
        string descr_tipolocal
        string descr_subtipolocal
    }
```

**Fato:** `fato_ocorrencia` — grão de 1 rubrica por BO. Coordenadas e logradouro são
dimensões *degeneradas* (alta cardinalidade → ficam na fato).

**Dimensões:** `dim_data` (calendário), `dim_local` (grão de **bairro**, com centroide
para mapas) e `dim_tipo_ocorrencia` (rubrica, natureza apurada, tipo/subtipo de local).
Chaves naturais nulas usam o membro `NÃO INFORMADO` para evitar FK nula.

Documentação completa de domínio, valores esperados e linhagem em
[`docs/catalogo_de_dados.md`](docs/catalogo_de_dados.md).

## Principais desafios técnicos resolvidos

- **Reconciliação de schema entre 5 anos de arquivos**, com nomes de coluna
  divergentes (`CIDADE` vs `NOME_MUNICIPIO`, cedilha em colunas de circunscrição)
  e uma coluna inexistente antes de 2025 (`DESCR_TIPOLOCAL`) — resolvido por
  `coalesce` por nome de coluna (nunca por posição).
- **FK nula por `NULL = NULL` não casar em join** — o caso mais perigoso: como
  `descr_tipolocal` é nulo em todo 2022–2024, um join ingênuo descartaria a maioria
  dos registros. Resolvido com o membro sentinela `NÃO INFORMADO`.
- **Sentinelas da fonte** (`0` em lat/long, `'NULL'`, `'(Vazio)'`, `'-'`) tratadas
  como nulo real, com evidência da limpeza na análise de qualidade.
- **Formato real das datas** (datetime do Excel, não `M/D/YY`) e a distinção entre
  ano de ocorrência e ano de registro (`ano_estatistica`).
- **Inconsistência de grafia** em `"SOROCABA "` (espaço à direita) e em `desc_periodo`.
- **Cobertura parcial de 2026**: comparação Jan-Abr entre anos para evitar viés.

## Como reproduzir

**Setup único (Databricks Repos + Jobs nativos):**

1. No Databricks: `Repos → Add Repo` → cole a URL deste repositório.
2. O Job `885399393946221` já está configurado com schedule semanal (segunda 06h BRT) e
   executa via serverless (sem cluster a criar).
3. Para a carga inicial: dispare o Job manualmente em `Workflows → Jobs → Run now`.

**Execução manual da análise:**
- Execute `02_qualidade_dados` e `03_analise_perguntas_negocio` após o Job concluir.

Passo a passo detalhado em [`docs/RUNBOOK_DATABRICKS.md`](docs/RUNBOOK_DATABRICKS.md).

## Próximos passos

Este projeto é a primeira etapa de um trabalho em duas partes. O MVP
seguinte utiliza a camada Gold aqui construída como base de features para
um modelo preditivo de ocorrências criminais por bairro e período.

---

<div align="center">
<sub>Dados públicos sob licença CC-BY 4.0 · SSP-SP</sub>
</div>
