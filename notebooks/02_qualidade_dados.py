# Databricks notebook source
# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Análise de Qualidade de Dados
# =============================================================================
#
# Objetivo: para CADA atributo, identificar problemas (nulos, duplicidade,
# outliers, inconsistência) e discutir se/como isso afeta as perguntas de
# negócio. Roda sobre a camada Silver (já limpa/reconciliada) e faz uma
# verificação pontual na Bronze para EVIDENCIAR as sentinelas brutas que a
# Silver tratou.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Carregamento

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA  = "sorocaba_seguranca"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

df = spark.table("silver")
total_linhas = df.count()
print(f"Total de registros (Sorocaba, 2022-2026): {total_linhas:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Evidência das sentinelas brutas (camada Bronze)
# MAGIC
# MAGIC Antes de analisar a Silver (já limpa), mostramos o que a fonte traz de "sujo"
# MAGIC e que a Silver converteu para nulo — isso comprova que a limpeza foi necessária
# MAGIC e correta. Sentinelas identificadas na inspeção: `'NULL'`, `'(Vazio)'`, `'-'`
# MAGIC e `0` (latitude/longitude sem geolocalização).

# COMMAND ----------

bronze = spark.table("bronze")

def col_bronze(nome):
    return F.col(f"`{nome}`")

# Conta sentinelas no recorte de Sorocaba dentro da bronze (todo o Estado tem nomes
# de município variados; aqui contamos no dataset inteiro só para evidenciar o padrão)
amostra_sentinelas = bronze.select(
    F.sum((col_bronze("LATITUDE") == "0").cast("int")).alias("lat_zero"),
    F.sum((col_bronze("LATITUDE") == "-").cast("int")).alias("lat_traco"),
    F.sum((col_bronze("LATITUDE") == "NULL").cast("int")).alias("lat_null_str"),
    F.sum((col_bronze("HORA_OCORRENCIA_BO") == "NULL").cast("int")).alias("hora_null_str"),
    F.sum((col_bronze("COD_IBGE") == "(Vazio)").cast("int")).alias("ibge_vazio"),
)
print("Contagem de sentinelas brutas na Bronze (todo o Estado de SP):")
amostra_sentinelas.show(truncate=False)
print("Todas essas foram convertidas para NULL real na Silver — ver notebook 01, Seção 2.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Completude — percentual de nulos por coluna (Silver)
# MAGIC
# MAGIC Nulos não são necessariamente "erro": alguns são esperados pela natureza do
# MAGIC dado (ex.: `descr_tipolocal` não existia antes de 2025; `hora_ocorrencia_bo`
# MAGIC nula em crimes sexuais pode ser vedação proposital de dado sensível).

# COMMAND ----------

def relatorio_nulos(df, total):
    agregacoes = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns if not c.startswith("_")
    ]
    linha = df.agg(*agregacoes).collect()[0].asDict()
    resultado = [(c, q, round(100 * q / total, 2) if total else 0) for c, q in linha.items()]
    resultado.sort(key=lambda x: x[2], reverse=True)
    return resultado

print(f"{'Coluna':<28} {'Nulos':>12} {'% total':>10}")
print("-" * 52)
for c, q, pct in relatorio_nulos(df, total_linhas):
    print(f"{c:<28} {q:>12,} {pct:>9}%")

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar com os números acima):**
# MAGIC - `descr_tipolocal`: nulo ~100% em 2022–2024 (coluna inexistente nesses anos);
# MAGIC   não é defeito, é diferença de schema. Limita a pergunta 5 ao período 2025+.
# MAGIC - `latitude`/`longitude`: nulos = registros sem geolocalização (inclui o
# MAGIC   sentinela `0` já tratado). Afeta mapas por coordenada; NÃO afeta a análise
# MAGIC   por bairro (texto), que é o eixo principal das perguntas 1, 3 e 6.
# MAGIC - `hora_ocorrencia_bo`: ver investigação direcionada na Seção 6.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Unicidade — granularidade de Boletim de Ocorrência
# MAGIC
# MAGIC `num_bo` isolado NÃO é chave única: um mesmo BO pode conter várias rubricas
# MAGIC (1 linha = 1 rubrica). Testamos a chave composta para distinguir granularidade
# MAGIC esperada de duplicidade real.

# COMMAND ----------

df_dup = (
    df.groupBy("num_bo", "ano_bo", "nome_delegacia")
    .agg(F.count("*").alias("qtd"))
    .filter(F.col("qtd") > 1)
)
qtd_dup = df_dup.count()
print(f"Combinações (num_bo, ano_bo, delegacia) com mais de 1 registro: {qtd_dup:,}")
if qtd_dup > 0:
    print("Exemplos (maiores contagens) — esperado: múltiplas rubricas no mesmo BO:")
    df_dup.orderBy(F.col("qtd").desc()).show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validade — consistência temporal
# MAGIC
# MAGIC ATENÇÃO à semântica confirmada na inspeção: `ano_estatistica` é o ano do
# MAGIC **registro** (estatística oficial), que pode diferir do ano da **ocorrência**
# MAGIC (`dt_ocorrencia_bo`). Por isso há fatos de dez/2021 (e anteriores) no arquivo
# MAGIC de 2022 — isso é legítimo, não erro. A validação abaixo só sinaliza datas
# MAGIC **impossíveis**: futuras (após a coleta) ou absurdamente antigas.

# COMMAND ----------

from datetime import date

DATA_MAX_COLETA = date(2026, 4, 30)   # limite superior dos dados coletados
DATA_PISO = date(2015, 1, 1)          # piso defensivo p/ ocorrência muito antiga

df_data_impossivel = df.filter(
    (F.col("dt_ocorrencia_bo") > F.lit(DATA_MAX_COLETA)) |
    (F.col("dt_ocorrencia_bo") < F.lit(DATA_PISO))
)
print(f"dt_ocorrencia_bo impossível (futura ou < 2015): {df_data_impossivel.count():,}")
print(f"dt_ocorrencia_bo nula: {df.filter(F.col('dt_ocorrencia_bo').isNull()).count():,}")

# Inconsistência lógica: ocorrência DEPOIS do registro do BO
df_ordem = df.filter(F.col("dt_ocorrencia_bo") > F.col("dt_registro_bo"))
print(f"\ndt_ocorrencia_bo > dt_registro_bo (inconsistencia logica): {df_ordem.count():,}")
if df_ordem.count() > 0:
    df_ordem.select("num_bo", "dt_ocorrencia_bo", "dt_registro_bo", "rubrica").show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Validade — coordenadas fora do município

# COMMAND ----------

# Bounding box aproximado de Sorocaba/SP (detecção grosseira de outlier geográfico)
LAT_MIN, LAT_MAX = -23.65, -23.35
LON_MIN, LON_MAX = -47.55, -47.30

df_geo = df.filter(
    F.col("latitude").isNotNull() & F.col("longitude").isNotNull() &
    ((F.col("latitude") < LAT_MIN) | (F.col("latitude") > LAT_MAX) |
     (F.col("longitude") < LON_MIN) | (F.col("longitude") > LON_MAX))
)
qtd_geo = df.filter(F.col("latitude").isNotNull()).count()
print(f"Registros com lat/long preenchida: {qtd_geo:,}")
print(f"Fora do bounding box de Sorocaba: {df_geo.count():,} "
      f"(podem ser bairros limítrofes, erro de digitação ou lat/long trocadas)")
if df_geo.count() > 0:
    df_geo.select("num_bo", "latitude", "longitude").show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Investigação direcionada — nulo de hora em ocorrências sensíveis
# MAGIC
# MAGIC Hipótese: `hora_ocorrencia_bo` é nula com mais frequência em rubricas sensíveis
# MAGIC (crimes sexuais), sugerindo vedação proposital de dado para proteção da vítima,
# MAGIC e não falha de preenchimento.

# COMMAND ----------

sensivel = F.lower(F.col("rubrica")).rlike("estupro|vulnerav|sexual")
df_flag = df.withColumn("rubrica_sensivel", sensivel)

(
    df_flag.groupBy("rubrica_sensivel")
    .agg(F.count("*").alias("total"),
         F.sum(F.col("hora_ocorrencia_bo").isNull().cast("int")).alias("hora_nula"))
    .withColumn("pct_hora_nula", F.round(100 * F.col("hora_nula") / F.col("total"), 2))
    .show(truncate=False)
)
print("Se o % de hora nula for muito maior no grupo sensivel, reforca a hipotese de "
      "vedacao proposital — dado a ser PRESERVADO como nulo, nao 'corrigido'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Consistência categórica — cardinalidade e grafia

# COMMAND ----------

for col_nome in ["rubrica", "natureza_apurada", "bairro", "desc_periodo", "descr_tipolocal"]:
    qtd = df.select(col_nome).distinct().count()
    print(f"\n{col_nome}: {qtd} valores unicos")
    valores = [r[0] for r in df.select(col_nome).distinct().collect() if r[0] is not None]
    norm = {}
    for v in valores:
        norm.setdefault(v.strip().upper(), []).append(v)
    suspeitos = {k: v for k, v in norm.items() if len(v) > 1}
    if suspeitos:
        print(f"  Possíveis duplicatas por grafia:")
        for variantes in list(suspeitos.values())[:5]:
            print(f"    {variantes}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Síntese — resumo executivo da qualidade de dados
# MAGIC
# MAGIC Preencher com os números reais das células acima:
# MAGIC
# MAGIC | Problema | Magnitude | Causa provável | Afeta qual pergunta? | Tratamento |
# MAGIC |---|---|---|---|---|
# MAGIC | Sentinela `0` em lat/long | (ver Seção 1) | "Sem geolocalização" exportado como 0 | Mapas por coordenada | Convertido p/ nulo na Silver |
# MAGIC | `'NULL'`/`'(Vazio)'`/`'-'` | (ver Seção 1) | Vazio exportado como texto | Diversas | Convertido p/ nulo na Silver |
# MAGIC | Nulo em `descr_tipolocal` (2022-24) | ~100% nesses anos | Coluna inexistente | Pergunta 5 (só 2025+) | Membro "NÃO INFORMADO" |
# MAGIC | `"SOROCABA "` com espaço | 2 de 9 guias | Exportação da fonte | Filtro de município | Normalização na Silver |
# MAGIC | Grafia de `desc_periodo` | 2 variantes de caixa | Mudança de padrão entre anos | Pergunta 2 (período) | Mapeado p/ canônico |
# MAGIC | Hora nula em crimes sensíveis | (ver Seção 6) | Vedação proposital | Pergunta 2 (horário) | Preservado como nulo |
