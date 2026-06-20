# Dados

Os arquivos de dados brutos (~890 MB no total, 2022-2026) e o landing em Parquet
**não são versionados** neste repositório — apenas o código que os processa e
amostras pequenas de schema.

## Fluxo de obtenção e preparação

```bash
# 1. Download dos .xlsx oficiais (gera data/SPDadosCriminais_*.xlsx)
python3 scripts/coletar_dados.py

# 2. Conversão para Parquet (gera data/parquet/, que sobe ao Databricks)
python3 scripts/converter_para_parquet.py
```

Fonte oficial (substitua `{ANO}` por 2022..2026):

```
https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ANO}.xlsx
```

**Licença:** Creative Commons Attribution 4.0 (CC-BY 4.0)

## `schema_samples/`

Amostras pequenas (cabeçalho + 12 linhas) de cada ano, em CSV — versionadas para
documentar o schema e a evolução das colunas entre anos (ex.: `DESCR_TIPOLOCAL`
aparecendo só a partir de 2025). Não contêm o dataset completo.

## Por que não versionar os dados

- Volume (~890 MB de `.xlsx`) fora do escopo do Git.
- Arquivos individuais de 187–209 MB **excedem o limite de 100 MB/arquivo do GitHub**
  (o push seria rejeitado).
- Os dados são públicos e reprodutíveis a partir da fonte oficial via os scripts acima.
