# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Pipeline: Bronze (raw) -> Silver (limpo/reconciliado) -> Gold (Esquema Estrela)
# Plataforma: Databricks Community Edition · Formato: Delta Lake
# Fonte: SSP-SP / Dados Abertos SP — licença CC-BY 4.0
# =============================================================================
#
# Pré-requisito (rodar UMA vez, localmente, fora do Databricks):
#   1. python3 scripts/coletar_dados.py          # baixa os .xlsx da SSP-SP
#   2. python3 scripts/converter_para_parquet.py # gera data/parquet/ (landing)
#   3. Suba o diretório data/parquet/ para um Volume/DBFS do Databricks.
#
# Por que Parquet e não .xlsx direto no Databricks:
#   - O Spark não lê .xlsx nativamente; ler ~190 MB com pandas no driver do
#     Community Edition estoura a memória. Parquet é colunar, comprimido e lido
#     nativa e distribuidamente pelo Spark. Ver justificativa no Catálogo de Dados.
#
# Como usar este notebook:
#   - Importe como notebook (.py: cada "# COMMAND ----------" vira uma célula).
#   - Ajuste RAW_PARQUET_PATH e SCHEMA na célula de configuração.
#   - Rode célula a célula na ordem.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuração geral

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Caminho do landing em Parquet, já enviado ao Volume/DBFS (ajuste ao seu ambiente)
RAW_PARQUET_PATH = "/Volumes/main/default/sorocaba_seguranca/parquet"
# Caso não use Unity Catalog Volumes no Community Edition, use DBFS clássico:
# RAW_PARQUET_PATH = "dbfs:/FileStore/sorocaba_seguranca/parquet"

# Schema (database) onde as tabelas Delta gerenciadas serão registradas — permite
# consulta SQL direta (FROM sorocaba_seguranca.fato_ocorrencia) e gera evidência limpa.
SCHEMA = "sorocaba_seguranca"

MUNICIPIO_ALVO = "SOROCABA"
SENTINELA = "NÃO INFORMADO"   # membro de dimensão para chaves naturais ausentes (evita FK nula)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Camada Bronze — ingestão fiel à origem
# MAGIC
# MAGIC O landing em Parquet preserva cada guia de cada ano com os nomes de coluna
# MAGIC **originais** e tudo como string (inclusive sentinelas da fonte: `'NULL'`,
# MAGIC `'(Vazio)'`, `'-'`, `'0'` em lat/long). A reconciliação de nomes e a tipagem
# MAGIC só acontecem na Silver.
# MAGIC
# MAGIC `mergeSchema=true` une anos com schemas diferentes: como 2022 usa `CIDADE` e
# MAGIC 2023+ usa `NOME_MUNICIPIO`, e `DESCR_TIPOLOCAL` só existe a partir de 2025,
# MAGIC o schema final é a UNIÃO das colunas — cada uma fica nula nos anos em que
# MAGIC não existia. Isso é o comportamento correto para fidelidade ao dado bruto.

# COMMAND ----------

df_bronze = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(RAW_PARQUET_PATH)
)

df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze")

print(f"Bronze: {df_bronze.count():,} linhas, {len(df_bronze.columns)} colunas (todo o Estado de SP)")
print("Colunas:", df_bronze.columns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Camada Silver — reconciliação, tipagem, limpeza e filtro de Sorocaba
# MAGIC
# MAGIC Transformações aplicadas (documentadas em detalhe no Catálogo de Dados, Seção 7):
# MAGIC
# MAGIC 1. **Reconciliação de schema por `coalesce`** das colunas variantes entre anos
# MAGIC    (ex.: `municipio = coalesce(CIDADE, NOME_MUNICIPIO)`). Feita por NOME, nunca
# MAGIC    por posição.
# MAGIC 2. **Tipagem com formato correto**: as datas vêm como `yyyy-MM-dd` (datetime
# MAGIC    nativo do Excel, renderizado na conversão) — NÃO no formato `M/d/yy`.
# MAGIC 3. **Tratamento de sentinelas**: `'NULL'`, `'(Vazio)'`, `'-'`, `''` e `0`/`0.0`
# MAGIC    em latitude/longitude viram nulo real.
# MAGIC 4. **Normalização de texto** para o filtro de município (remove acento, espaços
# MAGIC    e caixa) — corrige o `"SOROCABA "` com espaço à direita que existe em parte
# MAGIC    das guias e que quebraria um filtro ingênuo por igualdade.
# MAGIC 5. **Normalização de `desc_periodo`** para um conjunto canônico (a fonte grava
# MAGIC    `EM HORA INCERTA` e `Em hora incerta` em anos diferentes).

# COMMAND ----------

def col_ou_nulo(df: DataFrame, nome: str):
    """Retorna a coluna se existir no DataFrame; senão, um literal nulo.
    Torna a reconciliação robusta a colunas ausentes em algum ano."""
    return F.col(f"`{nome}`") if nome in df.columns else F.lit(None)


def normaliza_texto_col(col):
    """Remove acentuação, espaços nas pontas e aplica caixa alta."""
    de = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇáàâãäéèêëíìîïóòôõöúùûüç"
    para = "AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc"
    return F.upper(F.trim(F.translate(col, de, para)))


def to_null_se(col, valores):
    """Converte valores-sentinela em nulo real."""
    return F.when(F.trim(col).isin(*valores), None).otherwise(col)


df_b = spark.table("bronze")

# --- (1) Reconciliação para nomes canônicos via coalesce das variantes por ano ---
df_s = df_b.select(
    F.coalesce(col_ou_nulo(df_b, "CIDADE"), col_ou_nulo(df_b, "NOME_MUNICIPIO")).alias("municipio"),
    col_ou_nulo(df_b, "NUM_BO").alias("num_bo"),
    col_ou_nulo(df_b, "ANO_BO").alias("ano_bo"),
    col_ou_nulo(df_b, "DATA_OCORRENCIA_BO").alias("dt_ocorrencia_bo"),
    F.coalesce(col_ou_nulo(df_b, "DATA_COMUNICACAO_BO"), col_ou_nulo(df_b, "DATA_REGISTRO")).alias("dt_registro_bo"),
    col_ou_nulo(df_b, "HORA_OCORRENCIA_BO").alias("hora_ocorrencia_bo"),
    F.coalesce(col_ou_nulo(df_b, "DESCR_PERIODO"), col_ou_nulo(df_b, "DESC_PERIODO")).alias("desc_periodo"),
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

# --- (2) Tipagem com formatos corretos ---
df_s = (
    df_s
    .withColumn("dt_ocorrencia_bo", F.to_date("dt_ocorrencia_bo", "yyyy-MM-dd"))
    .withColumn("dt_registro_bo", F.to_date("dt_registro_bo", "yyyy-MM-dd"))
    .withColumn("ano_bo", F.col("ano_bo").cast(IntegerType()))
    .withColumn("mes_estatistica", F.col("mes_estatistica").cast(IntegerType()))
    .withColumn("ano_estatistica", F.col("ano_estatistica").cast(IntegerType()))
)

# --- (3) Sentinelas -> nulo ---
# Latitude/longitude: '-', 'NULL', '' já caem no cast; o 0/0.0 é sentinela de "sem geo"
for c in ["latitude", "longitude"]:
    df_s = df_s.withColumn(c, to_null_se(F.col(c), ["-", "NULL", ""]).cast(DoubleType()))
    df_s = df_s.withColumn(c, F.when(F.col(c) == 0, None).otherwise(F.col(c)))

df_s = df_s.withColumn("cod_ibge", to_null_se(F.col("cod_ibge"), ["(Vazio)", "NULL", ""]).cast(IntegerType()))
df_s = df_s.withColumn("numero_logradouro", to_null_se(F.col("numero_logradouro"), ["", "NULL", "0"]).cast(IntegerType()))
df_s = df_s.withColumn("hora_ocorrencia_bo", to_null_se(F.col("hora_ocorrencia_bo"), ["NULL", "", "-"]))

# 'NULL'/'' -> nulo nas demais colunas de texto
colunas_texto = ["municipio", "num_bo", "desc_periodo", "descr_tipolocal", "descr_subtipolocal",
                 "bairro", "logradouro", "rubrica", "descr_conduta", "natureza_apurada",
                 "nome_departamento", "nome_seccional", "nome_delegacia", "municipio_circunscricao"]
for c in colunas_texto:
    df_s = df_s.withColumn(c, to_null_se(F.col(c), ["NULL", ""]))

# --- (4) Normalização + filtro de município ---
df_s = df_s.withColumn("municipio_normalizado", normaliza_texto_col(F.col("municipio")))
df_s = df_s.filter(F.col("municipio_normalizado") == MUNICIPIO_ALVO)

# --- (5) Normalização de desc_periodo para conjunto canônico ---
periodo_norm = normaliza_texto_col(F.col("desc_periodo"))
df_s = df_s.withColumn(
    "desc_periodo",
    F.when(periodo_norm == "DE MADRUGADA", "De madrugada")
     .when(periodo_norm == "PELA MANHA", "Pela manhã")
     .when(periodo_norm == "A TARDE", "A tarde")
     .when(periodo_norm == "A NOITE", "A noite")
     .when(periodo_norm == "EM HORA INCERTA", "Em hora incerta")
     .otherwise(F.col("desc_periodo"))
)

df_silver = df_s.drop("municipio_normalizado")

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("ano_estatistica").saveAsTable("silver")

print(f"Silver: {df_silver.count():,} linhas (filtradas para Sorocaba)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Camada Gold — Esquema Estrela
# MAGIC
# MAGIC **Fato:** `fato_ocorrencia` — grão = 1 rubrica registrada em 1 BO.
# MAGIC **Dimensões:** `dim_data`, `dim_local` (grão = bairro), `dim_tipo_ocorrencia`.
# MAGIC
# MAGIC Duas decisões de modelagem importantes:
# MAGIC
# MAGIC - **Sem FK nula (membro "NÃO INFORMADO").** Toda chave natural nula (ex.:
# MAGIC   `descr_tipolocal` em 2022–2024) é substituída por um membro-sentinela ANTES
# MAGIC   de construir a dimensão e de ligar o fato. Isso evita o problema clássico de
# MAGIC   `NULL = NULL` não casar em join, que descartaria silenciosamente a maioria
# MAGIC   dos registros anteriores a 2025.
# MAGIC - **`dim_local` no grão de bairro** (não por coordenada exata). Latitude e
# MAGIC   longitude são atributos do evento e ficam na própria fato (dimensão
# MAGIC   degenerada); a `dim_local` guarda o bairro normalizado e um centroide
# MAGIC   (média das coordenadas válidas), útil para mapas por bairro.

# COMMAND ----------

silver = spark.table("silver")

# Prepara chaves naturais sem nulo (sentinela) e bairro normalizado
chaves_tipo = ["rubrica", "natureza_apurada", "descr_tipolocal", "descr_subtipolocal"]
g = silver
for c in chaves_tipo:
    g = g.withColumn(c, F.coalesce(F.col(c), F.lit(SENTINELA)))
g = g.withColumn("bairro_norm", F.coalesce(normaliza_texto_col(F.col("bairro")), F.lit(SENTINELA)))

# --- dim_data ---
df_dim_data = (
    g.select(F.col("dt_ocorrencia_bo").alias("data")).where(F.col("data").isNotNull()).distinct()
    .withColumn("id_data", F.date_format("data", "yyyyMMdd").cast(IntegerType()))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))  # 1=domingo .. 7=sábado (padrão Spark)
    .withColumn("dia_semana_nome", F.array(
        F.lit("Domingo"), F.lit("Segunda"), F.lit("Terça"), F.lit("Quarta"),
        F.lit("Quinta"), F.lit("Sexta"), F.lit("Sábado"))[F.col("dia_semana_num") - 1])
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

# Persiste as dimensões e relê para fixar os surrogate keys antes de ligar o fato
df_dim_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_data")
df_dim_local.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_local")
df_dim_tipo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_tipo_ocorrencia")

dim_local = spark.table("dim_local").select("id_local", "bairro")
dim_tipo = spark.table("dim_tipo_ocorrencia")

# --- fato_ocorrencia ---
df_fato = (
    g
    .join(dim_local, g["bairro_norm"] == dim_local["bairro"], "left")
    .join(dim_tipo, on=chaves_tipo, how="left")
    .withColumn("id_data", F.date_format("dt_ocorrencia_bo", "yyyyMMdd").cast(IntegerType()))
    .select(
        "num_bo", "ano_bo", "id_data", "id_local", "id_tipo_ocorrencia",
        "hora_ocorrencia_bo", "desc_periodo",
        "logradouro", "numero_logradouro", "latitude", "longitude",
        "mes_estatistica", "ano_estatistica",
        F.lit(1).alias("quantidade"),
    )
)

df_fato.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .partitionBy("ano_estatistica").saveAsTable("fato_ocorrencia")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validação pós-carga
# MAGIC
# MAGIC Confirma a persistência e a corretude do esquema estrela: contagens e,
# MAGIC sobretudo, a **ausência de FK nula** no fato (prova de que a correção do
# MAGIC join por sentinela funcionou).

# COMMAND ----------

print("Linhas por tabela:")
for t in ["bronze", "silver", "dim_data", "dim_local", "dim_tipo_ocorrencia", "fato_ocorrencia"]:
    print(f"  {t:<22}: {spark.table(t).count():,}")

fato = spark.table("fato_ocorrencia")
print("\nIntegridade referencial (deve ser 0 em todas):")
print(f"  id_local nulo        : {fato.filter(F.col('id_local').isNull()).count():,}")
print(f"  id_tipo nulo         : {fato.filter(F.col('id_tipo_ocorrencia').isNull()).count():,}")
print(f"  id_data nulo (s/data): {fato.filter(F.col('id_data').isNull()).count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Próximos notebooks
# MAGIC
# MAGIC - `02_qualidade_dados` — análise de qualidade por atributo (sobre a Silver).
# MAGIC - `03_analise_perguntas_negocio` — respostas às 6 perguntas (sobre a Gold).
