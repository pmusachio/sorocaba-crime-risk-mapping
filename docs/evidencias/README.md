# Evidências de execução

Esta pasta contém as evidências visuais exigidas pelo enunciado do MVP:

> *"Algumas tarefas das etapas do trabalho podem ser feitas a partir de
> componentes visuais da plataforma de nuvem. Desta forma, deve se gerar
> evidência da execução destes passos através de screenshots ou vídeos."*
>
> *"Deve se gerar evidência dos resultados das respostas às perguntas que
> definem o problema do MVP através de screenshots ou vídeos."*

---

## Checklist de evidências

### Infraestrutura e orquestração

- [ ] `01_job_configurado.png` — Job "Pipeline TCC Sorocaba" configurado no Databricks
      (aba Tasks mostrando o DAG: coleta_bronze → silver_gold → qualidade + analise)
- [ ] `02_job_schedule.png` — Schedule semanal configurado (segunda 06h BRT)
- [ ] `03_repo_conectado.png` — Databricks Repos mostrando o repo GitHub conectado
      (`/Repos/paulomusachio@gmail.com/sorocaba-crime-risk-mapping`)

### Execução do pipeline

- [ ] `04_coleta_bronze_execucao.png` — Saída do notebook 00 concluído:
      anos processados, contagem Bronze total, histórico de downloads
- [ ] `05_silver_execucao.png` — Saída do notebook 01: contagem Silver (filtrado Sorocaba)
- [ ] `06_gold_execucao.png` — Saída do notebook 01: contagens Gold + **FK nula = 0**
      (verificação de integridade referencial — crítico para nota)
- [ ] `07_job_sucesso.png` — Job run com todas as 4 tasks em verde (SUCCEEDED)

### Tabelas e modelo

- [ ] `08_tabelas_persistidas.png` — `SHOW TABLES` no schema `sorocaba_seguranca`
      (deve listar: bronze, silver, fato_ocorrencia, dim_data, dim_local, dim_tipo_ocorrencia)
- [ ] `09_bronze_sample.png` — `SELECT * FROM bronze LIMIT 5` (dado bruto estadual)
- [ ] `10_silver_sample.png` — `SELECT * FROM silver LIMIT 5` (Sorocaba filtrado, tipado)

### Qualidade de dados

- [ ] `11_qualidade_geral.png` — Notebook 02, Seção 1: resumo geral de qualidade
- [ ] `12_qualidade_sintese.png` — Notebook 02, Seção 8: tabela-síntese com magnitudes reais

### Respostas às perguntas de negócio (Notebook 03)

- [ ] `13_pergunta1_bairros.png` — Top bairros por volume de ocorrências + evolução temporal
- [ ] `14_pergunta2_sazonalidade.png` — Distribuição por dia da semana e por mês
- [ ] `15_pergunta3_tipos_bairro.png` — Tipos de ocorrência por bairro/região
- [ ] `16_pergunta4_tendencia.png` — Tendência anual de volume (2022–2026 parcial)
- [ ] `17_pergunta5_tipo_local.png` — Tipo de local vs tipo de ocorrência (2025+)
- [ ] `18_pergunta6_correlacao.png` — Correlação espacial entre tipos de ocorrência

### Fonte e licença

- [ ] `19_fonte_ssp.png` — Página de download da SSP-SP evidenciando:
      URL do dataset, licença CC-BY 4.0 e data de acesso

---

## Convenção de nomenclatura

Prefixo numérico para ordenação, sufixo descritivo, extensão `.png`.
Vídeos curtos aceitos em `.mp4` ou `.gif` (máx. 10 MB por arquivo).

## Como capturar

### Screenshots do Databricks

1. Abra o Job em `Workflows → Jobs → Pipeline TCC Sorocaba — Coleta + ETL`
2. Clique no run mais recente para ver o DAG de execução
3. Clique em cada task para ver o output do notebook
4. Use `⌘ + Shift + 4` (macOS) para recorte ou `⌘ + Shift + 3` para tela inteira

### SHOW TABLES via SQL Editor

```sql
USE CATALOG workspace;
USE sorocaba_seguranca;
SHOW TABLES;
```

### Verificação de integridade (FK nula = 0)

```sql
USE CATALOG workspace;
USE sorocaba_seguranca;
SELECT
  SUM(CASE WHEN id_data IS NULL THEN 1 ELSE 0 END)             AS fk_data_nula,
  SUM(CASE WHEN id_local IS NULL THEN 1 ELSE 0 END)            AS fk_local_nula,
  SUM(CASE WHEN id_tipo_ocorrencia IS NULL THEN 1 ELSE 0 END)  AS fk_tipo_nula,
  COUNT(*)                                                       AS total
FROM fato_ocorrencia;
```
