# Evidências de execução

O descritivo pede evidências da execução das etapas na plataforma de nuvem e dos
resultados das perguntas, por meio de capturas de tela ou vídeos.

As saídas dos notebooks são commitadas junto com eles e já servem como evidência
dos resultados. Este diretório reúne as capturas dos passos executados por
componentes visuais do console da GCP, que não aparecem nos notebooks:

| Arquivo sugerido | O que deve mostrar |
|---|---|
| `01-bucket-zonas.png` | o bucket do data lake com as zonas `bruta/` e `preparada/` |
| `02-dataproc-batch.png` | o batch do Dataproc Serverless com estado `Succeeded` |
| `03-bigquery-datasets.png` | os datasets `stg`, `dw` e `qualidade` no BigQuery |
| `04-bigquery-esquema.png` | o esquema de `dw.fato_ocorrencia`, com as descrições das colunas |
| `05-consulta-resultado.png` | o resultado de uma das consultas de negócio no console |
