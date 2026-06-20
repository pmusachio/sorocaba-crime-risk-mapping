# Runbook — execução no Databricks

Passo a passo para reproduzir o pipeline completo e gerar as evidências
exigidas pelo enunciado. **Todo processamento de dados acontece na nuvem**
(Databricks); não há etapa local obrigatória.

---

## Visão geral do fluxo

```
GitHub (código)
    │
    ├─ GitHub Actions (cron semanal) ──→ Databricks API
    │                                          │
    └─ Databricks Repos (sync)            notebook 00
                                          (download + Bronze)
                                               │
                                          notebook 01
                                          (Silver + Gold)
```

---

## 1. Conectar o repositório ao Databricks (Repos)

1. No workspace Databricks: **Repos → Add Repo**.
2. Informe a URL do repositório GitHub.
3. Databricks clona o repositório em
   `/Repos/<seu-email>/sorocaba-crime-risk-mapping/`.
4. Para atualizar o código após commits: botão **Pull** na barra do Repo.

📸 **Evidência:** screenshot do Repo conectado no Databricks.

---

## 2. Criar ou confirmar o cluster

No Community Edition existe um único cluster. Confirme que está ativo ou
crie um com as configurações padrão.

📸 **Evidência:** `01_cluster_ativo.png` — screenshot do cluster ativo.

---

## 3. Carga inicial — executar o notebook 00 manualmente

Abra `/Repos/<email>/sorocaba-crime-risk-mapping/notebooks/00_coleta_incremental`
e execute **Run All**.

O notebook irá:
- Baixar os 5 arquivos `.xlsx` da SSP-SP diretamente para o DBFS
  (`dbfs:/FileStore/sorocaba_seguranca/xlsx/`)
- Converter cada guia para Parquet via openpyxl streaming (sem OOM)
- Criar a tabela Delta **Bronze** no schema `sorocaba_seguranca`
- Encadear automaticamente o notebook 01 (Silver + Gold)

⏱ Tempo estimado na carga inicial: 60–90 min (5 arquivos × ~190 MB cada).

📸 **Evidências:**
- `02_bronze_execucao.png` — saída da Seção 2 (anos processados + contagem Bronze)
- `03_silver_execucao.png` — saída da Seção 2 do notebook 01 (Silver)
- `04_gold_execucao.png` — Seção 4 do notebook 01: contagens + integridade referencial

> A **FK nula deve ser 0 em todas** as linhas da Seção 4. Se não for, há
> bug no join — investigue antes de continuar.

---

## 4. Configurar o pipeline semanal (GitHub Actions)

### 4.1 Gerar token no Databricks

`User Settings → Access Tokens → Generate New Token`
Copie o token — ele só é exibido uma vez.

### 4.2 Obter o Cluster ID

URL do cluster no Databricks:
`https://community.cloud.databricks.com/#setting/clusters/<CLUSTER_ID>/configuration`

O `CLUSTER_ID` é a sequência após `/clusters/`.

### 4.3 Criar segredos no GitHub

`Repositório → Settings → Secrets and variables → Actions → New repository secret`

| Segredo | Valor |
|---|---|
| `DATABRICKS_HOST` | `https://community.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | token do passo 4.1 |
| `DATABRICKS_CLUSTER_ID` | cluster ID do passo 4.2 |
| `DATABRICKS_USER` | seu e-mail no Databricks (ex.: `paulomusachio@gmail.com`) |

### 4.4 Verificar o workflow

O arquivo `.github/workflows/pipeline_semanal.yml` já está no repositório.
Ele dispara todo semanal (segunda às 6h UTC) e pode ser disparado manualmente
em `Actions → Pipeline Semanal SSP-SP → Run workflow`.

O workflow submete o notebook 00 via API, aguarda conclusão com polling a
cada 60 s e falha a execução no GitHub se o Databricks retornar erro.

📸 **Evidência:** screenshot do workflow verde no GitHub Actions.

---

## 5. Executar notebooks de análise

Após o notebook 01 concluir (encadeado pelo 00 ou rodado manualmente):

### Notebook 02 — qualidade de dados

Abra `02_qualidade_dados` e execute **Run All**.
- Anote os números reais para preencher:
  - Tabela-síntese da Seção 8 do próprio notebook
  - Coluna "Magnitude" no Catálogo de Dados (`docs/catalogo_de_dados.md`)

📸 **Evidência:** `06_qualidade_dados.png`

### Notebook 03 — perguntas de negócio

Execute **Run All** e, em cada célula markdown "Discussão (completar...)",
escreva a interpretação dos resultados reais.

📸 **Evidências:** `07`..`12` — um gráfico por pergunta de negócio.

---

## 6. Verificar tabelas registradas

```sql
-- rode em qualquer notebook ou no SQL Editor
USE sorocaba_seguranca;
SHOW TABLES;
```

📸 **Evidência:** `05_tabelas_persistidas.png` — saída do SHOW TABLES.

---

## 7. Evidência da fonte e licença

📸 Screenshot da página de download da SSP-SP mostrando a origem e a
licença CC-BY 4.0 (`13_fonte_origem_ssp.png`).

---

## 8. Fechar a documentação

- Preencher [`docs/autoavaliacao.md`](autoavaliacao.md) com resultados reais.
- Confirmar que `docs/evidencias/` tem todos os arquivos do checklist.

---

## Checklist final (mapeado aos critérios de avaliação)

- [ ] Notebook 00 roda do início ao fim sem erro (Coleta)
- [ ] Bronze contém dados de todos os anos (Coleta)
- [ ] GitHub Actions verde com execução semanal configurada (Coleta)
- [ ] FK nula = 0 na Seção 4 do notebook 01 (Modelagem)
- [ ] Tabelas Delta registradas no schema `sorocaba_seguranca` (Carga)
- [ ] Notebook 02 com números reais + síntese preenchida (Análise — qualidade)
- [ ] Notebook 03 com 6 discussões + síntese geral (Análise — solução)
- [ ] Catálogo com magnitudes preenchidas (Modelagem — catálogo)
- [ ] Autoavaliação preenchida (Autoavaliação)
- [ ] Todas as evidências em `docs/evidencias/` (Capricho)
- [ ] Repositório público no GitHub, **sem** `.xlsx`/`.parquet` (Capricho)
