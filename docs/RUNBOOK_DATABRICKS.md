# Runbook — execução no Databricks Free Edition

Passo a passo para reproduzir o pipeline completo e gerar as evidências
exigidas pelo enunciado. **Todo processamento de dados acontece na nuvem**
(Databricks); não há etapa local obrigatória.

---

## Visão geral do fluxo

```
GitHub (código)
    │
    └─ Databricks Repos (sync automático via Job)
              │
         Databricks Job (schedule semanal — segunda 06h BRT)
         Serverless compute — sem cluster a gerenciar
              │
         ┌────▼──────────────────────────────┐
         │ Task 1: notebook 00               │
         │   coleta + Bronze                 │
         │   armazena xlsx/parquet em        │
         │   Unity Catalog Volume            │
         └────┬──────────────────────────────┘
              │
         ┌────▼──────────────────────────────┐
         │ Task 2: notebook 01               │
         │   Silver + Gold (esquema estrela) │
         └────┬──────────────────────────────┘
              │
     ┌────────┴────────┐
     ▼                 ▼
Task 3: 02_qualidade   Task 4: 03_analise
(em paralelo após 01)
```

---

## 1. Conectar o repositório ao Databricks (Repos)

1. No workspace Databricks: **Repos → Add Repo**.
2. Informe a URL: `https://github.com/pmusachio/sorocaba-crime-risk-mapping`
3. Databricks clona em `/Repos/paulomusachio@gmail.com/sorocaba-crime-risk-mapping/`.
4. O Job já usa `git_source` — puxa a branch `main` automaticamente em cada execução.
   Para atualizar manualmente: botão **Pull** na barra do Repo.

📸 **Evidência:** screenshot do Repo conectado.

---

## 2. Infraestrutura (já criada, sem ação necessária)

| Recurso | Identificador |
|---|---|
| Job | ID `885399393946221` — "Pipeline TCC Sorocaba — Coleta + ETL" |
| Schedule | Segunda 06h00 America/Sao_Paulo |
| UC Volume | `workspace.sorocoba_seguranca.dados` |
| Compute | Serverless (sem cluster — Free Edition) |

O Job e o Volume já foram criados. Basta disparar a primeira execução.

---

## 3. Carga inicial — disparar o Job manualmente

1. No Databricks: `Workflows → Jobs`.
2. Localize "Pipeline TCC Sorocaba — Coleta + ETL".
3. Clique em **Run now**.

O Job irá:
- **Task 1 (coleta_bronze):** baixar os 5 arquivos `.xlsx` da SSP-SP para o
  Volume `/Volumes/workspace/sorocoba_seguranca/dados/xlsx/`, converter para Parquet
  e escrever a tabela Delta **Bronze** no schema `workspace.sorocoba_seguranca`.
- **Task 2 (silver_gold):** transformar Bronze em Silver + tabelas Gold (Esquema Estrela).
- **Tasks 3 e 4 (qualidade + analise):** análise de qualidade e perguntas de negócio,
  executadas em paralelo após o silver_gold.

⏱ Tempo estimado na carga inicial: 60–90 min (5 arquivos × ~190 MB cada).

📸 **Evidências:**
- `02_bronze_execucao.png` — saída da Seção 2 do notebook 00 (anos processados + contagem Bronze)
- `03_silver_execucao.png` — saída da Seção 2 do notebook 01 (Silver)
- `04_gold_execucao.png` — Seção 4 do notebook 01: contagens + integridade referencial

> A **FK nula deve ser 0 em todas** as linhas da Seção 4. Se não for, há
> bug no join — investigue antes de continuar.

---

## 4. Pipeline semanal — automático

O Job já tem schedule configurado. Nenhuma ação adicional necessária.

O workflow `.github/workflows/pipeline_semanal.yml` existe como disparador
manual de emergência via GitHub Actions, mas é redundante ao schedule nativo do Job.

---

## 5. Executar notebooks de análise

Após o Job concluir (tasks 3 e 4 executam automaticamente), os notebooks já terão rodado.
Para re-executar manualmente:

### Notebook 02 — qualidade de dados

Abra `02_qualidade_dados` no Repo e execute **Run All**.
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
-- rode no SQL Editor ou em qualquer notebook
USE CATALOG workspace;
USE sorocoba_seguranca;
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

- [ ] Job roda do início ao fim sem erro (Coleta)
- [ ] Bronze contém dados de todos os anos (Coleta)
- [ ] Job com schedule semanal configurado — print do histórico de execuções (Coleta)
- [ ] FK nula = 0 na Seção 4 do notebook 01 (Modelagem)
- [ ] Tabelas Delta registradas no schema `workspace.sorocoba_seguranca` (Carga)
- [ ] Notebook 02 com números reais + síntese preenchida (Análise — qualidade)
- [ ] Notebook 03 com 6 discussões + síntese geral (Análise — solução)
- [ ] Catálogo com magnitudes preenchidas (Modelagem — catálogo)
- [ ] Autoavaliação preenchida (Autoavaliação)
- [ ] Todas as evidências em `docs/evidencias/` (Capricho)
- [ ] Repositório público no GitHub, **sem** `.xlsx`/`.parquet` (Capricho)
