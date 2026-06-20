# Runbook — execução no Databricks

Passo a passo para reproduzir o pipeline completo e gerar as evidências exigidas
pelo enunciado. Os passos 1–2 rodam na sua máquina; os demais, no Databricks.

## Pré-requisitos locais

```bash
python3 -m pip install openpyxl pyarrow
```

## 1. Coleta (local)

```bash
python3 scripts/coletar_dados.py        # baixa SPDadosCriminais_2022..2026.xlsx para data/
```

> Se já tem os `.xlsx` em `data/`, o script detecta e pula o download.

## 2. Conversão para Parquet (local)

```bash
python3 scripts/converter_para_parquet.py   # gera data/parquet/
```

Confira que `data/parquet/` tem 9 arquivos `.parquet` (2 guias/ano × 4 anos + 1 de 2026).

## 3. Upload do Parquet para o Databricks

- **Com Unity Catalog (Volumes):** crie um Volume e suba a pasta:
  `Catalog → main → default → Create Volume → sorocaba_seguranca`, e envie
  `data/parquet/` para `/Volumes/main/default/sorocaba_seguranca/parquet`.
- **Sem Volumes (DBFS clássico):** `Data → DBFS → Upload` para
  `dbfs:/FileStore/sorocaba_seguranca/parquet`.

📸 **Evidência:** screenshot da pasta Parquet no Databricks (`05` na convenção de nomes).

## 4. Importar os notebooks

`Workspace → Import` e selecione os três arquivos de `notebooks/`. O formato `.py`
com marcadores `# COMMAND ----------` é reconhecido como notebook Databricks.

## 5. Configurar e rodar o notebook 01 (pipeline)

1. Anexe um cluster (no Community Edition, o cluster único já serve).
   📸 **Evidência:** cluster ativo (`01_cluster_ativo.png`).
2. Na 1ª célula, ajuste:
   - `RAW_PARQUET_PATH` para o caminho do passo 3.
   - `SCHEMA` (padrão `sorocaba_seguranca`) se quiser outro nome.
3. Rode todas as células. Ao final, a Seção 4 imprime as contagens e a verificação
   de integridade referencial (FK nula deve ser **0** em todas).
   📸 **Evidência:** saída de Bronze/Silver/Gold + integridade (`02`, `03`, `04`).

## 6. Rodar o notebook 02 (qualidade)

Rode todas as células e **anote os números reais** para preencher:
- a tabela-síntese da Seção 8 do próprio notebook;
- a coluna "Magnitude" da tabela de qualidade no Catálogo de Dados.

📸 **Evidência:** `06_qualidade_dados.png` (sentinelas, nulos, hipótese da hora).

## 7. Rodar o notebook 03 (análise das 6 perguntas)

Para cada pergunta, rode a célula SQL/Python, observe o gráfico e **escreva a
discussão** logo abaixo (célula markdown "Discussão (completar...)").

📸 **Evidência:** um gráfico por pergunta (`07`..`12`) + síntese final.

## 8. Evidência da fonte e licença

📸 Screenshot da página de download da SSP-SP mostrando origem e licença CC-BY 4.0
(`13_fonte_origem_ssp.png`).

## 9. Fechar a documentação

- Preencher [`docs/autoavaliacao.md`](autoavaliacao.md) com os resultados reais.
- Conferir que `docs/evidencias/` tem todos os arquivos do checklist.

## Checklist final (mapeado aos critérios de avaliação)

- [ ] Pipeline roda fim a fim sem erro (Carga)
- [ ] FK nula = 0 no fato (Modelagem)
- [ ] Notebook 02 com números reais + síntese (Análise — qualidade)
- [ ] Notebook 03 com 6 discussões + síntese geral (Análise — solução)
- [ ] Catálogo com magnitudes preenchidas (Modelagem — catálogo)
- [ ] Autoavaliação preenchida (Autoavaliação)
- [ ] Todas as evidências em `docs/evidencias/` (Coleta, Carga, Análise, Capricho)
- [ ] Repositório público no GitHub, **sem** os `.xlsx`/`.parquet` (Capricho)
