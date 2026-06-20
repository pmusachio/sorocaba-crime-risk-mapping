# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Notebook 01 — Silver + Gold (Esquema Estrela)
#
# Pré-requisito: notebook 00_coleta_incremental já escreveu a tabela Bronze.
# Este notebook lê a Bronze, aplica transformações de qualidade (Silver) e
# constrói o Esquema Estrela (Gold), particionado por ano-mês da ocorrência.
#
# Pode ser executado diretamente (após o 00) ou encadeado via
# dbutils.notebook.run() pelo notebook 00 no pipeline semanal.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuração geral

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

SCHEMA    = "sorocaba_seguranca"
MUNICIPIO = "SOROCABA"
SENTINELA = "NÃO INFORMADO"   # membro de dimensão para chaves naturais ausentes

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Camada Bronze — leitura
# MAGIC
# MAGIC A Bronze é escrita pelo notebook `00_coleta_incremental`, que baixa os xlsx
# MAGIC da SSP-SP direto para o DBFS e converte via openpyxl streaming (sem OOM).
# MAGIC Aqui lemos a tabela já existente para seguir para Silver + Gold.
# MAGIC
# MAGIC A tabela preserva os nomes de coluna **originais** de cada ano (`CIDADE` em 2022
# MAGIC vs `NOME_MUNICIPIO` em 2023+, `DESCR_TIPOLOCAL` só a partir de 2025) e tudo como
# MAGIC string, incluindo sentinelas da fonte (`'NULL'`, `'(Vazio)'`, `'-'`, `'0'`).
# MAGIC A reconciliação de nomes e a tipagem acontecem na Silver.

# COMMAND ----------

df_b = spark.table("bronze")

print(f"Bronze: {df_b.count():,} linhas, {len(df_b.columns)} colunas (todo o Estado de SP)")
print("Colunas:", df_b.columns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Camada Silver — reconciliação, tipagem, limpeza e filtro
# MAGIC
# MAGIC Transformações (documentadas no Catálogo de Dados, Seção 7):
# MAGIC
# MAGIC 1. **Reconciliação de schema por `coalesce`** — colunas variantes entre anos
# MAGIC    unificadas por NOME, nunca por posição.
# MAGIC 2. **Tipagem com formato correto** — datas vêm como `yyyy-MM-dd` (datetime
# MAGIC    nativo do Excel renderizado na conversão do notebook 00), NÃO `M/d/yy`.
# MAGIC 3. **Tratamento de sentinelas** — `'NULL'`, `'(Vazio)'`, `'-'`, `''` e `0`
# MAGIC    em lat/long viram nulo real.
# MAGIC 4. **Normalização + filtro de município** — corrige `"SOROCABA "` com espaço.
# MAGIC 5. **Normalização de `desc_periodo`** — unifica grafias entre anos.
# MAGIC 6. **`ano_mes_ocorrencia`** — coluna de particionamento derivada de
# MAGIC    `dt_ocorrencia_bo` no formato `yyyyMM` (ex.: `'202204'`). Registros sem
# MAGIC    data recebem sentinela `'000000'` para não serem descartados.

# COMMAND ----------


def col_ou_nulo(df: DataFrame, nome: str):
    """Retorna a coluna se existir; senão um literal nulo."""
    return F.col(f"`{nome}`") if nome in df.columns else F.lit(None)


def normaliza_texto_col(col):
    """Remove acentuação, espaços nas pontas e aplica caixa alta."""
    de   = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇáàâãäéèêëíìîïóòôõöúùûüç"
    para = "AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc"
    return F.upper(F.trim(F.translate(col, de, para)))


def to_null_se(col, valores):
    """Converte valores-sentinela em nulo real."""
    return F.when(F.trim(col).isin(*valores), None).otherwise(col)


# --- (1) Reconciliação para nomes canônicos ---
df_s = df_b.select(
    F.coalesce(col_ou_nulo(df_b, "CIDADE"), col_ou_nulo(df_b, "NOME_MUNICIPIO")).alias("municipio"),
    col_ou_nulo(df_b, "NUM_BO").alias("num_bo"),
    col_ou_nulo(df_b, "ANO_BO").alias("ano_bo"),
    col_ou_nulo(df_b, "DATA_OCORRENCIA_BO").alias("dt_ocorrencia_bo"),
    F.coalesce(col_ou_nulo(df_b, "DATA_COMUNICACAO_BO"),
               col_ou_nulo(df_b, "DATA_REGISTRO")).alias("dt_registro_bo"),
    col_ou_nulo(df_b, "HORA_OCORRENCIA_BO").alias("hora_ocorrencia_bo"),
    F.coalesce(col_ou_nulo(df_b, "DESCR_PERIODO"),
               col_ou_nulo(df_b, "DESC_PERIODO")).alias("desc_periodo"),
    col_ou_nulo(df_b, "DESCR_TIPOLOCAL").alias("descr_tipolocal"),
    col_ou_nulo(df_b, "DESCR_SUBTIPOLOCAL").alias("descr_subtipolocal"),
    col_ou_nulo(df_b, "BAIRRO").alias("bairro"),
    col_ou_nulo(df_b, "LOGRADOURO").alias("logradouro"),
    col_ou_nulo(df_b, "NUMERO_LOGRADOURO").alias("numero_logradouro"),
    col_ou_nulo(df_b, "LATITUDE").alias("latitude"),
    col_ou_nulo(df_b, "LONGITUDE").alias("longitude"),
    col_ou_nulo(df_b, "RUBRICA").alias("rubrica"),
    col_ou_nulo(df_b, "DESCR_CONDUTA").alias("descr_conduta"),
    col_ou_nulo(df_b, "NATUREZA_APURADA").alias("natureza_apurada"),
    col_ou_nulo(df_b, "NOME_DEPARTAMENTO").alias("nome_departamento"),
    col_ou_nulo(df_b, "NOME_SECCIONAL").alias("nome_seccional"),
    col_ou_nulo(df_b, "NOME_DELEGACIA").alias("nome_delegacia"),
    F.coalesce(col_ou_nulo(df_b, "NOME_MUNICIPIO_CIRCUNSCRIÇÃO"),
               col_ou_nulo(df_b, "NOME_MUNICIPIO_CIRCUNSCRICAO")).alias("municipio_circunscricao"),
    col_ou_nulo(df_b, "MES_ESTATISTICA").alias("mes_estatistica"),
    col_ou_nulo(df_b, "ANO_ESTATISTICA").alias("ano_estatistica"),
    col_ou_nulo(df_b, "COD IBGE").alias("cod_ibge"),
    F.col("_arquivo_origem"),
    F.col("_guia_origem"),
)

# --- (2) Tipagem ---
df_s = (
    df_s
    .withColumn("dt_ocorrencia_bo", F.to_date("dt_ocorrencia_bo", "yyyy-MM-dd"))
    .withColumn("dt_registro_bo",   F.to_date("dt_registro_bo",   "yyyy-MM-dd"))
    .withColumn("ano_bo",           F.col("ano_bo").cast(IntegerType()))
    .withColumn("mes_estatistica",  F.col("mes_estatistica").cast(IntegerType()))
    .withColumn("ano_estatistica",  F.col("ano_estatistica").cast(IntegerType()))
)

# --- (3) Sentinelas → nulo ---
for c in ["latitude", "longitude"]:
    df_s = df_s.withColumn(c, to_null_se(F.col(c), ["-", "NULL", ""]).cast(DoubleType()))
    df_s = df_s.withColumn(c, F.when(F.col(c) == 0, None).otherwise(F.col(c)))

df_s = df_s.withColumn("cod_ibge", to_null_se(F.col("cod_ibge"), ["(Vazio)", "NULL", ""]).cast(IntegerType()))
df_s = df_s.withColumn("numero_logradouro", to_null_se(F.col("numero_logradouro"), ["", "NULL", "0"]).cast(IntegerType()))
df_s = df_s.withColumn("hora_ocorrencia_bo", to_null_se(F.col("hora_ocorrencia_bo"), ["NULL", "", "-"]))

for c in ["municipio", "num_bo", "desc_periodo", "descr_tipolocal", "descr_subtipolocal",
          "bairro", "logradouro", "rubrica", "descr_conduta", "natureza_apurada",
          "nome_departamento", "nome_seccional", "nome_delegacia", "municipio_circunscricao"]:
    df_s = df_s.withColumn(c, to_null_se(F.col(c), ["NULL", ""]))

# --- (4) Normalização + filtro de município ---
df_s = df_s.withColumn("_municipio_norm", normaliza_texto_col(F.col("municipio")))
df_s = df_s.filter(F.col("_municipio_norm") == MUNICIPIO)

# --- (5) Normalização de desc_periodo ---
pn = normaliza_texto_col(F.col("desc_periodo"))
df_s = df_s.withColumn(
    "desc_periodo",
    F.when(pn == "DE MADRUGADA",   "De madrugada")
     .when(pn == "PELA MANHA",     "Pela manhã")
     .when(pn == "A TARDE",        "A tarde")
     .when(pn == "A NOITE",        "A noite")
     .when(pn == "EM HORA INCERTA","Em hora incerta")
     .otherwise(F.col("desc_periodo"))
)

# --- (6) Coluna de particionamento: ano-mês da ocorrência ---
# Formato yyyyMM (ex.: '202204'). Registros sem data recebem '000000'.
df_s = df_s.withColumn(
    "ano_mes_ocorrencia",
    F.coalesce(F.date_format(F.col("dt_ocorrencia_bo"), "yyyyMM"), F.lit("000000"))
)

df_silver = df_s.drop("_municipio_norm")

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("ano_mes_ocorrencia").saveAsTable("silver")

print(f"Silver: {df_silver.count():,} linhas (Sorocaba) — particionado por ano_mes_ocorrencia")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Camada Gold — Esquema Estrela
# MAGIC
# MAGIC **Fato:** `fato_ocorrencia` — grão = 1 rubrica registrada em 1 BO.
# MAGIC **Dimensões:** `dim_data`, `dim_local` (grão = bairro), `dim_tipo_ocorrencia`.
# MAGIC
# MAGIC Decisões de modelagem:
# MAGIC
# MAGIC - **Sem FK nula** — toda chave natural nula é substituída por `'NÃO INFORMADO'`
# MAGIC   **antes** de construir a dimensão e ligar o fato. Evita o problema clássico de
# MAGIC   `NULL = NULL` não casar em join (crítico: `descr_tipolocal` é nulo em 2022–2024).
# MAGIC - **`dim_local` no grão de bairro** — lat/long ficam na fato como dimensão
# MAGIC   degenerada; a dim_local guarda centroide para mapas.
# MAGIC - **Particionamento por `ano_mes_ocorrencia`** — permite reprocessar apenas os
# MAGIC   meses afetados por cada atualização incremental (replaceWhere).

# COMMAND ----------

silver = spark.table("silver")

# Prepara chaves naturais sem nulo (sentinela) e bairro normalizado
chaves_tipo = ["rubrica", "natureza_apurada", "descr_tipolocal", "descr_subtipolocal"]
g = silver
for c in chaves_tipo:
    g = g.withColumn(c, F.coalesce(F.col(c), F.lit(SENTINELA)))
g = g.withColumn("bairro_norm",
                 F.coalesce(normaliza_texto_col(F.col("bairro")), F.lit(SENTINELA)))

# --- dim_data ---
df_dim_data = (
    g.select(F.col("dt_ocorrencia_bo").alias("data"))
     .where(F.col("data").isNotNull()).distinct()
     .withColumn("id_data",       F.date_format("data", "yyyyMMdd").cast(IntegerType()))
     .withColumn("ano",           F.year("data"))
     .withColumn("mes",           F.month("data"))
     .withColumn("dia",           F.dayofmonth("data"))
     .withColumn("trimestre",     F.quarter("data"))
     .withColumn("dia_semana_num",F.dayofweek("data"))
     .withColumn("dia_semana_nome", F.array(
         F.lit("Domingo"), F.lit("Segunda"), F.lit("Terça"),  F.lit("Quarta"),
         F.lit("Quinta"),  F.lit("Sexta"),   F.lit("Sábado"))[F.col("dia_semana_num") - 1])
     .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
     .select("id_data", "data", "ano", "mes", "dia", "trimestre",
             "dia_semana_num", "dia_semana_nome", "fim_de_semana")
)

# --- dim_local (grão = bairro, com centroide) ---
df_dim_local = (
    g.groupBy("bairro_norm").agg(
        F.avg("latitude").alias("latitude_centroide"),
        F.avg("longitude").alias("longitude_centroide"),
        F.count(F.lit(1)).alias("qtd_ocorrencias"),
    )
    .withColumnRenamed("bairro_norm", "bairro")
    .withColumn("id_local", F.monotonically_increasing_id())
    .select("id_local", "bairro", "latitude_centroide", "longitude_centroide", "qtd_ocorrencias")
)

# --- dim_tipo_ocorrencia ---
df_dim_tipo = (
    g.select(*chaves_tipo).distinct()
     .withColumn("id_tipo_ocorrencia", F.monotonically_increasing_id())
     .select("id_tipo_ocorrencia", *chaves_tipo)
)

# Persiste dimensões e relê para fixar surrogate keys antes de construir o fato
df_dim_data.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_data")
df_dim_local.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_local")
df_dim_tipo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_tipo_ocorrencia")

dim_local = spark.table("dim_local").select("id_local", "bairro")
dim_tipo  = spark.table("dim_tipo_ocorrencia")

# --- fato_ocorrencia ---
df_fato = (
    g
    .join(dim_local, g["bairro_norm"] == dim_local["bairro"], "left")
    .join(dim_tipo,  on=chaves_tipo, how="left")
    .withColumn("id_data", F.date_format("dt_ocorrencia_bo", "yyyyMMdd").cast(IntegerType()))
    .select(
        "num_bo", "ano_bo", "id_data", "id_local", "id_tipo_ocorrencia",
        "hora_ocorrencia_bo", "desc_periodo",
        "logradouro", "numero_logradouro", "latitude", "longitude",
        "mes_estatistica", "ano_estatistica", "ano_mes_ocorrencia",
        F.lit(1).alias("quantidade"),
    )
)

df_fato.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("ano_mes_ocorrencia").saveAsTable("fato_ocorrencia")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validação pós-carga
# MAGIC
# MAGIC Contagens de cada tabela e **ausência de FK nula no fato** — comprova que a
# MAGIC correção de join por sentinela funcionou.

# COMMAND ----------

print("Linhas por tabela:")
for t in ["bronze", "silver", "dim_data", "dim_local", "dim_tipo_ocorrencia", "fato_ocorrencia"]:
    print(f"  {t:<25}: {spark.table(t).count():,}")

fato = spark.table("fato_ocorrencia")
print("\nIntegridade referencial (deve ser 0 em todas):")
print(f"  id_local nulo          : {fato.filter(F.col('id_local').isNull()).count():,}")
print(f"  id_tipo_ocorrencia nulo: {fato.filter(F.col('id_tipo_ocorrencia').isNull()).count():,}")
print(f"  id_data nulo (s/data)  : {fato.filter(F.col('id_data').isNull()).count():,}")

print("\nDistribuição de partições (ano_mes_ocorrencia):")
spark.sql("""
    SELECT ano_mes_ocorrencia, COUNT(*) AS total
    FROM fato_ocorrencia
    GROUP BY ano_mes_ocorrencia
    ORDER BY ano_mes_ocorrencia
""").show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Próximos notebooks
# MAGIC
# MAGIC - `02_qualidade_dados` — análise de qualidade por atributo (sobre a Silver).
# MAGIC - `03_analise_perguntas_negocio` — respostas às 6 perguntas (sobre a Gold).

# COMMAND ----------
dbutils.notebook.exit("ok")
