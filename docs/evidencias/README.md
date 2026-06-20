# Evidências de execução

Esta pasta deve conter as evidências visuais exigidas pelo enunciado do MVP:

> *"Algumas tarefas das etapas do trabalho podem ser feitas a partir de
> componentes visuais da plataforma de nuvem. Desta forma, deve se gerar
> evidência da execução destes passos através de screenshots ou vídeos."*
>
> *"Deve se gerar evidência dos resultados das respostas às perguntas que
> definem o problema do MVP através de screenshots ou vídeos."*

## Checklist do que incluir

- [ ] Screenshot do cluster Databricks Community Edition ativo
- [ ] Screenshot da pasta Parquet enviada ao Volume/DBFS (coleta persistida na nuvem)
- [ ] Screenshot da execução do notebook `01_pipeline_bronze_silver_gold`
      (contagens de Bronze/Silver/Gold **e a verificação de integridade: FK nula = 0**)
- [ ] Screenshot das tabelas Delta registradas (`SHOW TABLES` no schema `sorocaba_seguranca`)
- [ ] Screenshot ou vídeo curto da execução do notebook `02_qualidade_dados`
- [ ] Screenshot de cada visualização gerada no notebook `03_analise_perguntas_negocio`
      (uma por pergunta de negócio, no mínimo)
- [ ] Screenshot do dado de origem (página de download da SSP-SP), evidenciando
      a fonte e a licença

## Convenção de nomes sugerida

```
01_cluster_ativo.png
02_bronze_execucao.png
03_silver_execucao.png
04_gold_execucao.png
05_tabelas_persistidas.png
06_qualidade_dados.png
07_pergunta1_bairros.png
08_pergunta2_sazonalidade.png
09_pergunta3_tipos_por_bairro.png
10_pergunta4_tendencia.png
11_pergunta5_tipo_local.png
12_pergunta6_correlacao.png
13_fonte_origem_ssp.png
```
