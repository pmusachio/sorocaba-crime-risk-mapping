# Databricks notebook source
# =============================================================================
# MVP de Engenharia de Dados — Ocorrências registradas em Sorocaba
# Notebook 01 — Silver e Gold (esquema estrela)
#
# Pré-requisito: executar 00_coleta_bronze.py. Esta carga é integral,
# determinística e executada manualmente no workspace.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver e Gold
# MAGIC
# MAGIC A Silver concilia as variações anuais do XLSX, converte sentinelas em
# MAGIC nulos, aplica tipagem tolerante, confirma o código IBGE `3552205`, padroniza
# MAGIC classificações, deriva período somente quando a fonte não o informou e
# MAGIC deduplica exclusivamente pelo hash da linha original. A Gold mantém uma
# MAGIC linha para cada registro Silver deduplicado.

# COMMAND ----------

from datetime import datetime, timezone
import re
import unicodedata

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "workspace"
SCHEMA = "sorocaba_seguranca"
COD_IBGE_SOROCABA = 3552205
NAO_INFORMADO = "NÃO INFORMADO"

TABELA_BRONZE = "bronze_recorte_sorocaba"
TABELA_MANIFESTO = "bronze_manifesto"
TABELA_SILVER = "silver_ocorrencias"
TABELA_DIM_TEMPO = "dim_tempo"
TABELA_DIM_PERIODO = "dim_periodo_dia"
TABELA_DIM_NATUREZA = "dim_natureza"
TABELA_FATO = "fato_ocorrencia"
TABELA_TRANSFORMACOES = "matriz_transformacoes"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{TABELA_BRONZE}") or not spark.catalog.tableExists(
    f"{CATALOG}.{SCHEMA}.{TABELA_MANIFESTO}"
):
    raise RuntimeError("Execute 00_coleta_bronze.py antes deste notebook.")

bronze = spark.table(TABELA_BRONZE)
manifesto = spark.table(TABELA_MANIFESTO)
total_bronze = bronze.count()
if total_bronze == 0:
    raise RuntimeError("A Bronze está vazia; revise os arquivos e o filtro municipal.")

print(f"Bronze de entrada: {total_bronze:,} linhas; {len(bronze.columns)} colunas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Funções de reconciliação e limpeza

# COMMAND ----------

SENTINELAS = ["", "NULL", "(VAZIO)", "N/A", "NA", "-"]


def coluna_existente(df: DataFrame, *candidatos: str):
    """Retorna `coalesce` dos aliases presentes ou um nulo textual."""
    disponiveis = {nome.upper(): nome for nome in df.columns}
    colunas = [F.col(f"`{disponiveis[nome.upper()]}`") for nome in candidatos
               if nome.upper() in disponiveis]
    if not colunas:
        return F.lit(None).cast("string")
    return F.coalesce(*colunas) if len(colunas) > 1 else colunas[0]


def limpar_ausencia(coluna):
    texto = F.trim(coluna.cast("string"))
    return F.when(
        coluna.isNull() | F.upper(texto).isin(*SENTINELAS),
        F.lit(None).cast("string"),
    ).otherwise(texto)


def padronizar_texto(coluna):
    return F.when(
        coluna.isNull(), F.lit(None).cast("string")
    ).otherwise(F.upper(F.regexp_replace(F.trim(coluna), r"\s+", " ")))


def sem_acento_maiusculo(coluna):
    origem = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    destino = "AAAAAEEEEIIIIOOOOOUUUUC"
    return F.translate(F.upper(F.trim(coluna)), origem, destino)


def periodo_por_hora(coluna_hora):
    return (
        F.when(coluna_hora.between(0, 5), F.lit("MADRUGADA"))
        .when(coluna_hora.between(6, 11), F.lit("MANHÃ"))
        .when(coluna_hora.between(12, 17), F.lit("TARDE"))
        .when(coluna_hora.between(18, 23), F.lit("NOITE"))
    )


def sql_literal(texto: str) -> str:
    return "'" + texto.replace("'", "''") + "'"


dt_execucao = datetime.now(timezone.utc).replace(tzinfo=None)
transformacoes = []


def registrar_transformacao(
    ordem, regra, motivo, campos, antes, depois, afetadas
):
    transformacoes.append((
        int(ordem),
        regra,
        motivo,
        campos,
        int(antes),
        int(depois),
        int(afetadas),
        dt_execucao,
    ))


total_estadual = int(
    manifesto.agg(F.sum("linhas_estaduais_lidas").alias("total")).first()["total"] or 0
)
registrar_transformacao(
    1,
    "Normalizar apenas os cabeçalhos da captura",
    "Usar identificadores Delta estáveis sem alterar os valores originais.",
    "todas as colunas fonte da Bronze",
    total_bronze,
    total_bronze,
    0,
)
registrar_transformacao(
    2,
    "Filtrar Sorocaba durante a leitura",
    "Evitar persistir o conjunto estadual completo no ambiente gratuito.",
    "CD_IBGE/COD_IBGE",
    total_estadual,
    total_bronze,
    total_estadual - total_bronze,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver: reconciliação, tipagem e deduplicação

# COMMAND ----------

# Campos com nomes distintos entre edições anuais são conciliados por nome, nunca
# por posição. Endereço, coordenadas, delegacias e tipos de local não seguem para
# a Silver por não responderem às três perguntas do MVP.
reconciliada = bronze.select(
    F.col("id_registro_fonte"),
    coluna_existente(bronze, "CD_IBGE", "COD_IBGE").alias("cod_ibge_raw"),
    coluna_existente(bronze, "NUM_BO").alias("num_bo_raw"),
    coluna_existente(bronze, "ANO_BO").alias("ano_bo_raw"),
    coluna_existente(bronze, "DATA_OCORRENCIA_BO").alias("dt_ocorrencia_bo_raw"),
    coluna_existente(bronze, "HORA_OCORRENCIA_BO").alias("hora_ocorrencia_bo_raw"),
    coluna_existente(bronze, "DESCR_PERIODO", "DESC_PERIODO").alias("periodo_raw"),
    coluna_existente(bronze, "RUBRICA").alias("rubrica_raw"),
    coluna_existente(bronze, "NATUREZA_APURADA").alias("natureza_apurada_raw"),
    coluna_existente(bronze, "DESCR_CONDUTA").alias("descr_conduta_raw"),
    coluna_existente(bronze, "MES_ESTATISTICA").alias("mes_estatistica_raw"),
    coluna_existente(bronze, "ANO_ESTATISTICA").alias("ano_estatistica_raw"),
    F.col("_arquivo_origem"),
    F.col("_guia_origem"),
    F.col("_ano_arquivo"),
    F.col("_dt_ingestao"),
)

aliases_alternativos = bronze.filter(
    coluna_existente(bronze, "DESCR_PERIODO").isNull()
    & coluna_existente(bronze, "DESC_PERIODO").isNotNull()
).count()
registrar_transformacao(
    3,
    "Conciliar aliases anuais por nome",
    "Unificar variações de schema sem depender da posição das colunas.",
    "DESCR_PERIODO/DESC_PERIODO e campos canônicos selecionados",
    total_bronze,
    reconciliada.count(),
    aliases_alternativos,
)

campos_textuais_raw = [
    "cod_ibge_raw", "num_bo_raw", "ano_bo_raw", "dt_ocorrencia_bo_raw",
    "hora_ocorrencia_bo_raw", "periodo_raw", "rubrica_raw",
    "natureza_apurada_raw", "descr_conduta_raw", "mes_estatistica_raw",
    "ano_estatistica_raw",
]
condicao_sentinela = F.lit(False)
for nome in campos_textuais_raw:
    condicao_sentinela = condicao_sentinela | F.upper(
        F.trim(F.col(nome).cast("string"))
    ).isin(*SENTINELAS)
linhas_com_sentinela = reconciliada.filter(condicao_sentinela).count()

limpa = reconciliada
for nome in campos_textuais_raw:
    limpa = limpa.withColumn(nome, limpar_ausencia(F.col(nome)))
registrar_transformacao(
    4,
    "Converter sentinelas em nulos reais",
    "Representar ausência sem manter códigos textuais ambíguos.",
    ", ".join(campos_textuais_raw),
    reconciliada.count(),
    limpa.count(),
    linhas_com_sentinela,
)

# `try_cast` evita que um valor inválido interrompa a carga e permite que o perfil
# meça a perda de tipagem. Hora só é aceita nos formatos HH ou HH:MM[:SS].
tipada = (
    limpa
    .withColumn(
        "cod_ibge",
        F.expr("try_cast(try_cast(cod_ibge_raw AS DECIMAL(20,3)) AS INT)"),
    )
    .withColumn(
        "ano_bo",
        F.expr("try_cast(try_cast(ano_bo_raw AS DECIMAL(20,3)) AS INT)"),
    )
    .withColumn(
        "dt_ocorrencia_bo",
        F.expr("try_cast(substring(dt_ocorrencia_bo_raw, 1, 10) AS DATE)"),
    )
    .withColumn(
        "hora_ocorrencia_bo",
        F.expr("""
            CASE
              WHEN trim(hora_ocorrencia_bo_raw) RLIKE '^([01]?[0-9]|2[0-3])$'
                THEN try_cast(trim(hora_ocorrencia_bo_raw) AS INT)
              WHEN trim(hora_ocorrencia_bo_raw) RLIKE '^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$'
                THEN try_cast(split(trim(hora_ocorrencia_bo_raw), ':')[0] AS INT)
              ELSE NULL
            END
        """),
    )
    .withColumn(
        "mes_estatistica",
        F.expr("try_cast(try_cast(mes_estatistica_raw AS DECIMAL(20,3)) AS INT)"),
    )
    .withColumn(
        "ano_estatistica",
        F.expr("try_cast(try_cast(ano_estatistica_raw AS DECIMAL(20,3)) AS INT)"),
    )
    .withColumn("num_bo", F.col("num_bo_raw"))
)

falhas_tipagem = tipada.filter(
    (F.col("cod_ibge_raw").isNotNull() & F.col("cod_ibge").isNull())
    | (F.col("ano_bo_raw").isNotNull() & F.col("ano_bo").isNull())
    | (F.col("dt_ocorrencia_bo_raw").isNotNull() & F.col("dt_ocorrencia_bo").isNull())
    | (F.col("hora_ocorrencia_bo_raw").isNotNull() & F.col("hora_ocorrencia_bo").isNull())
    | (F.col("mes_estatistica_raw").isNotNull() & F.col("mes_estatistica").isNull())
    | (F.col("ano_estatistica_raw").isNotNull() & F.col("ano_estatistica").isNull())
).count()
registrar_transformacao(
    5,
    "Aplicar tipagem tolerante",
    "Converter datas, hora e inteiros sem abortar por valores inválidos.",
    "cod_ibge, ano_bo, dt_ocorrencia_bo, hora_ocorrencia_bo, mes_estatistica, ano_estatistica",
    limpa.count(),
    tipada.count(),
    falhas_tipagem,
)

fora_municipio = tipada.filter(
    F.col("cod_ibge").isNull() | (F.col("cod_ibge") != COD_IBGE_SOROCABA)
).count()
municipal = tipada.filter(F.col("cod_ibge") == COD_IBGE_SOROCABA)
registrar_transformacao(
    6,
    "Confirmar o código municipal",
    "Garantir que nenhum registro alheio a Sorocaba avance para a Silver.",
    "cod_ibge",
    tipada.count(),
    municipal.count(),
    fora_municipio,
)

periodo_norm = sem_acento_maiusculo(F.col("periodo_raw"))
periodo_fonte = (
    F.when(periodo_norm.isin("DE MADRUGADA", "MADRUGADA"), F.lit("MADRUGADA"))
    .when(periodo_norm.isin("PELA MANHA", "MANHA"), F.lit("MANHÃ"))
    .when(periodo_norm.isin("A TARDE", "TARDE"), F.lit("TARDE"))
    .when(periodo_norm.isin("A NOITE", "NOITE"), F.lit("NOITE"))
    .when(
        periodo_norm.isin("EM HORA INCERTA", "HORA INCERTA", "INCERTO"),
        F.lit("HORA INCERTA"),
    )
    .when(F.col("periodo_raw").isNotNull(), padronizar_texto(F.col("periodo_raw")))
)

padronizada = (
    municipal
    .withColumn("rubrica", padronizar_texto(F.col("rubrica_raw")))
    .withColumn("natureza_apurada", padronizar_texto(F.col("natureza_apurada_raw")))
    .withColumn("descr_conduta", padronizar_texto(F.col("descr_conduta_raw")))
    .withColumn("periodo_fonte", periodo_fonte)
)

linhas_texto_alterado = padronizada.filter(
    (
        F.col("rubrica_raw").isNotNull()
        & (F.col("rubrica") != F.col("rubrica_raw"))
    )
    | (
        F.col("natureza_apurada_raw").isNotNull()
        & (F.col("natureza_apurada") != F.col("natureza_apurada_raw"))
    )
    | (
        F.col("descr_conduta_raw").isNotNull()
        & (F.col("descr_conduta") != F.col("descr_conduta_raw"))
    )
    | (
        F.col("periodo_raw").isNotNull()
        & (F.col("periodo_fonte") != F.upper(F.trim(F.col("periodo_raw"))))
    )
).count()
registrar_transformacao(
    7,
    "Padronizar classificações textuais",
    "Evitar categorias distintas apenas por caixa, margens ou grafia conhecida.",
    "rubrica, natureza_apurada, descr_conduta, periodo_dia",
    municipal.count(),
    padronizada.count(),
    linhas_texto_alterado,
)

derivada = (
    padronizada
    .withColumn(
        "periodo_dia",
        F.coalesce(F.col("periodo_fonte"), periodo_por_hora(F.col("hora_ocorrencia_bo"))),
    )
    .withColumn(
        "origem_periodo",
        F.when(F.col("periodo_fonte").isNotNull(), F.lit("FONTE"))
        .when(F.col("hora_ocorrencia_bo").isNotNull(), F.lit("DERIVADO DA HORA"))
        .otherwise(F.lit(NAO_INFORMADO)),
    )
)
qtd_periodo_derivado = derivada.filter(F.col("origem_periodo") == "DERIVADO DA HORA").count()
registrar_transformacao(
    8,
    "Derivar período somente quando ausente",
    "Aumentar cobertura sem substituir a classificação informada pela fonte.",
    "periodo_dia, origem_periodo",
    padronizada.count(),
    derivada.count(),
    qtd_periodo_derivado,
)

silver_pre_dedup = derivada.select(
    "id_registro_fonte",
    "cod_ibge",
    "num_bo",
    "ano_bo",
    "dt_ocorrencia_bo",
    "hora_ocorrencia_bo",
    "periodo_dia",
    "origem_periodo",
    "rubrica",
    "natureza_apurada",
    "descr_conduta",
    "mes_estatistica",
    "ano_estatistica",
    "_arquivo_origem",
    "_guia_origem",
    "_ano_arquivo",
    "_dt_ingestao",
)

janela_deduplicacao = Window.partitionBy("id_registro_fonte").orderBy(
    "_ano_arquivo", "_arquivo_origem", "_guia_origem"
)
df_silver = (
    silver_pre_dedup
    .withColumn("_ordem_hash", F.row_number().over(janela_deduplicacao))
    .filter(F.col("_ordem_hash") == 1)
    .drop("_ordem_hash")
)
total_pre_dedup = silver_pre_dedup.count()
total_silver = df_silver.count()
registrar_transformacao(
    9,
    "Deduplicar exclusivamente pelo hash da origem",
    "Remover apenas linhas integralmente idênticas sem colapsar BOs legítimos.",
    "id_registro_fonte",
    total_pre_dedup,
    total_silver,
    total_pre_dedup - total_silver,
)

(
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_SILVER)
)
silver = spark.table(TABELA_SILVER)
print(f"Silver: {total_silver:,} registros deduplicados")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Gold: dimensões com chaves determinísticas

# COMMAND ----------

# Dimensão tempo: yyyyMMdd é estável e independe da ordem/particionamento Spark.
dim_tempo_validos = (
    silver.select(F.col("dt_ocorrencia_bo").alias("data"))
    .where(F.col("data").isNotNull())
    .distinct()
    .withColumn("sk_tempo", F.date_format("data", "yyyyMMdd").cast("long"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn(
        "nome_mes",
        F.element_at(
            F.array(*[F.lit(nome) for nome in [
                "JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO",
                "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO",
            ]]),
            F.col("mes"),
        ),
    )
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))
    .withColumn(
        "dia_semana_nome",
        F.element_at(
            F.array(*[F.lit(nome) for nome in [
                "DOMINGO", "SEGUNDA-FEIRA", "TERÇA-FEIRA", "QUARTA-FEIRA",
                "QUINTA-FEIRA", "SEXTA-FEIRA", "SÁBADO",
            ]]),
            F.col("dia_semana_num"),
        ),
    )
    .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    .select(
        "sk_tempo", "data", "ano", "mes", "nome_mes", "dia",
        "dia_semana_num", "dia_semana_nome", "fim_de_semana",
    )
)

schema_dim_tempo = T.StructType([
    T.StructField("sk_tempo", T.LongType(), False),
    T.StructField("data", T.DateType(), True),
    T.StructField("ano", T.IntegerType(), False),
    T.StructField("mes", T.IntegerType(), False),
    T.StructField("nome_mes", T.StringType(), False),
    T.StructField("dia", T.IntegerType(), False),
    T.StructField("dia_semana_num", T.IntegerType(), False),
    T.StructField("dia_semana_nome", T.StringType(), False),
    T.StructField("fim_de_semana", T.BooleanType(), True),
])
sentinela_tempo = spark.createDataFrame(
    [(-1, None, -1, -1, NAO_INFORMADO, -1, -1, NAO_INFORMADO, None)],
    schema_dim_tempo,
)
dim_tempo = sentinela_tempo.unionByName(dim_tempo_validos)

# Período: uma chave positiva para cada combinação observada de hora e período;
# apenas a ausência simultânea de ambos recebe a sentinela -1.
periodos_naturais = (
    silver.select("hora_ocorrencia_bo", "periodo_dia")
    .distinct()
    .withColumn("periodo_chave", F.coalesce(F.col("periodo_dia"), F.lit(NAO_INFORMADO)))
    .withColumn("hora_chave", F.coalesce(F.col("hora_ocorrencia_bo"), F.lit(-1)))
)
periodos_validos = periodos_naturais.filter(
    ~((F.col("hora_chave") == -1) & (F.col("periodo_chave") == NAO_INFORMADO))
)
janela_periodo = Window.orderBy("hora_chave", "periodo_chave")
dim_periodo_validos = (
    periodos_validos
    .withColumn("sk_periodo_dia", F.row_number().over(janela_periodo).cast("long"))
    .withColumn("hora", F.col("hora_ocorrencia_bo"))
    .withColumn(
        "faixa_horaria",
        F.when(F.col("hora_chave").between(0, 5), F.lit("00:00–05:59"))
        .when(F.col("hora_chave").between(6, 11), F.lit("06:00–11:59"))
        .when(F.col("hora_chave").between(12, 17), F.lit("12:00–17:59"))
        .when(F.col("hora_chave").between(18, 23), F.lit("18:00–23:59"))
        .otherwise(F.lit(NAO_INFORMADO)),
    )
    .withColumn("periodo_dia", F.col("periodo_chave"))
    .withColumn("hora_informada", F.col("hora_ocorrencia_bo").isNotNull())
    .select("sk_periodo_dia", "hora", "faixa_horaria", "periodo_dia", "hora_informada")
)
schema_dim_periodo = T.StructType([
    T.StructField("sk_periodo_dia", T.LongType(), False),
    T.StructField("hora", T.IntegerType(), True),
    T.StructField("faixa_horaria", T.StringType(), False),
    T.StructField("periodo_dia", T.StringType(), False),
    T.StructField("hora_informada", T.BooleanType(), False),
])
sentinela_periodo = spark.createDataFrame(
    [(-1, None, NAO_INFORMADO, NAO_INFORMADO, False)], schema_dim_periodo
)
dim_periodo = sentinela_periodo.unionByName(dim_periodo_validos)

# Natureza: nulos individuais tornam-se membros descritivos desconhecidos. O trio
# integralmente ausente usa somente a sentinela -1.
naturezas_naturais = (
    silver.select("natureza_apurada", "rubrica", "descr_conduta")
    .fillna(NAO_INFORMADO, subset=["natureza_apurada", "rubrica", "descr_conduta"])
    .distinct()
)
naturezas_validas = naturezas_naturais.filter(
    ~(
        (F.col("natureza_apurada") == NAO_INFORMADO)
        & (F.col("rubrica") == NAO_INFORMADO)
        & (F.col("descr_conduta") == NAO_INFORMADO)
    )
)
janela_natureza = Window.orderBy("natureza_apurada", "rubrica", "descr_conduta")
dim_natureza_validas = naturezas_validas.withColumn(
    "sk_natureza", F.row_number().over(janela_natureza).cast("long")
).select("sk_natureza", "natureza_apurada", "rubrica", "descr_conduta")
schema_dim_natureza = T.StructType([
    T.StructField("sk_natureza", T.LongType(), False),
    T.StructField("natureza_apurada", T.StringType(), False),
    T.StructField("rubrica", T.StringType(), False),
    T.StructField("descr_conduta", T.StringType(), False),
])
sentinela_natureza = spark.createDataFrame(
    [(-1, NAO_INFORMADO, NAO_INFORMADO, NAO_INFORMADO)], schema_dim_natureza
)
dim_natureza = sentinela_natureza.unionByName(dim_natureza_validas)

for nome, df in [
    (TABELA_DIM_TEMPO, dim_tempo),
    (TABELA_DIM_PERIODO, dim_periodo),
    (TABELA_DIM_NATUREZA, dim_natureza),
]:
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(nome)

dim_tempo = spark.table(TABELA_DIM_TEMPO)
dim_periodo = spark.table(TABELA_DIM_PERIODO)
dim_natureza = spark.table(TABELA_DIM_NATUREZA)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Fato e conservação do grão

# COMMAND ----------

base_fato = (
    silver
    .withColumn("natureza_chave", F.coalesce("natureza_apurada", F.lit(NAO_INFORMADO)))
    .withColumn("rubrica_chave", F.coalesce("rubrica", F.lit(NAO_INFORMADO)))
    .withColumn("conduta_chave", F.coalesce("descr_conduta", F.lit(NAO_INFORMADO)))
    .withColumn("periodo_chave", F.coalesce("periodo_dia", F.lit(NAO_INFORMADO)))
)

p = dim_periodo.alias("p")
n = dim_natureza.alias("n")
t = dim_tempo.alias("t")
b = base_fato.alias("b")

fato_com_chaves = (
    b
    .join(t, F.col("b.dt_ocorrencia_bo").eqNullSafe(F.col("t.data")), "left")
    .join(
        p,
        F.col("b.hora_ocorrencia_bo").eqNullSafe(F.col("p.hora"))
        & (F.col("b.periodo_chave") == F.col("p.periodo_dia")),
        "left",
    )
    .join(
        n,
        (F.col("b.natureza_chave") == F.col("n.natureza_apurada"))
        & (F.col("b.rubrica_chave") == F.col("n.rubrica"))
        & (F.col("b.conduta_chave") == F.col("n.descr_conduta")),
        "left",
    )
)

# Datas nulas não casam com a sentinela por conteúdo descritivo; as três chaves
# recebem -1 como salvaguarda explícita. Para período/natureza, o join encontra a
# sentinela quando todos os componentes estão ausentes.
df_fato = fato_com_chaves.select(
    F.col("b.id_registro_fonte").alias("id_registro_fonte"),
    F.col("b.num_bo").alias("num_bo"),
    F.col("b.ano_bo").alias("ano_bo"),
    F.coalesce(F.col("t.sk_tempo"), F.lit(-1)).cast("long").alias("sk_tempo"),
    F.coalesce(F.col("p.sk_periodo_dia"), F.lit(-1)).cast("long").alias("sk_periodo_dia"),
    F.coalesce(F.col("n.sk_natureza"), F.lit(-1)).cast("long").alias("sk_natureza"),
    F.col("b.mes_estatistica").alias("mes_estatistica"),
    F.col("b.ano_estatistica").alias("ano_estatistica"),
    F.lit(1).cast("int").alias("qtd_ocorrencia"),
)

total_fato = df_fato.count()
if total_fato != total_silver:
    raise AssertionError(
        f"O join dimensional alterou o grão: Silver={total_silver}, fato={total_fato}."
    )
if df_fato.filter(
    F.col("sk_tempo").isNull()
    | F.col("sk_periodo_dia").isNull()
    | F.col("sk_natureza").isNull()
).count():
    raise AssertionError("A fato contém chave estrangeira nula.")

(
    df_fato.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_FATO)
)
registrar_transformacao(
    10,
    "Resolver chaves dimensionais e criar medida",
    "Preservar todas as linhas Silver com FKs não nulas e medida aditiva unitária.",
    "sk_tempo, sk_periodo_dia, sk_natureza, qtd_ocorrencia",
    total_silver,
    total_fato,
    df_fato.filter(
        (F.col("sk_tempo") == -1)
        | (F.col("sk_periodo_dia") == -1)
        | (F.col("sk_natureza") == -1)
    ).count(),
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Matriz de transformações e catálogo Unity Catalog

# COMMAND ----------

schema_transformacoes = T.StructType([
    T.StructField("ordem", T.IntegerType(), False),
    T.StructField("regra", T.StringType(), False),
    T.StructField("motivo", T.StringType(), False),
    T.StructField("campos_afetados", T.StringType(), False),
    T.StructField("linhas_antes", T.LongType(), False),
    T.StructField("linhas_depois", T.LongType(), False),
    T.StructField("linhas_afetadas", T.LongType(), False),
    T.StructField("dt_execucao", T.TimestampType(), False),
])
df_transformacoes = spark.createDataFrame(transformacoes, schema_transformacoes)
df_transformacoes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABELA_TRANSFORMACOES)

comentarios = {
    TABELA_SILVER: (
        "Ocorrências de Sorocaba reconciliadas, tipadas, normalizadas e deduplicadas pelo hash da linha original.",
        {
            "id_registro_fonte": "SHA-256 da linha original; chave técnica única após deduplicação.",
            "cod_ibge": "Código IBGE tipado e confirmado como 3552205.",
            "num_bo": "Número do BO preservado como texto; não é chave única isoladamente.",
            "ano_bo": "Ano informado do BO, convertido de forma tolerante.",
            "dt_ocorrencia_bo": "Data informada da ocorrência, convertida de forma tolerante.",
            "hora_ocorrencia_bo": "Hora inteira de 0 a 23, extraída quando o formato é válido.",
            "periodo_dia": "Período padronizado da fonte ou derivado da hora quando ausente.",
            "origem_periodo": "FONTE, DERIVADO DA HORA ou NÃO INFORMADO.",
            "rubrica": "Rubrica normalizada em caixa alta e espaços simples.",
            "natureza_apurada": "Natureza apurada normalizada em caixa alta e espaços simples.",
            "descr_conduta": "Descrição de conduta normalizada em caixa alta e espaços simples.",
            "mes_estatistica": "Mês estatístico convertido para inteiro.",
            "ano_estatistica": "Ano estatístico convertido para inteiro.",
            "_arquivo_origem": "XLSX que forneceu o registro preservado.",
            "_guia_origem": "Guia que forneceu o registro preservado.",
            "_ano_arquivo": "Ano nominal do arquivo de origem.",
            "_dt_ingestao": "Instante UTC da captura Bronze.",
        },
    ),
    TABELA_DIM_TEMPO: (
        "Dimensão calendário no grão de uma data, acrescida de uma sentinela -1.",
        {
            "sk_tempo": "Chave determinística yyyyMMdd; -1 representa data desconhecida.",
            "data": "Data civil; nula somente na sentinela.",
            "ano": "Ano civil ou -1 na sentinela.",
            "mes": "Mês de 1 a 12 ou -1 na sentinela.",
            "nome_mes": "Nome do mês em português ou NÃO INFORMADO.",
            "dia": "Dia do mês ou -1 na sentinela.",
            "dia_semana_num": "Número Spark do dia: domingo=1 a sábado=7; -1 na sentinela.",
            "dia_semana_nome": "Nome do dia em português ou NÃO INFORMADO.",
            "fim_de_semana": "Verdadeiro para domingo/sábado; nulo na sentinela.",
        },
    ),
    TABELA_DIM_PERIODO: (
        "Dimensão de hora e período, incluindo observações sem hora e uma sentinela -1.",
        {
            "sk_periodo_dia": "Chave sequencial determinística; -1 quando hora e período são desconhecidos.",
            "hora": "Hora de 0 a 23; nula quando não informada.",
            "faixa_horaria": "Faixa de seis horas ou NÃO INFORMADO.",
            "periodo_dia": "MADRUGADA, MANHÃ, TARDE, NOITE, HORA INCERTA ou NÃO INFORMADO.",
            "hora_informada": "Indica se uma hora válida estava disponível.",
        },
    ),
    TABELA_DIM_NATUREZA: (
        "Dimensão das combinações de natureza, rubrica e conduta, com sentinela -1.",
        {
            "sk_natureza": "Chave sequencial determinística; -1 para classificação integralmente desconhecida.",
            "natureza_apurada": "Natureza padronizada ou NÃO INFORMADO.",
            "rubrica": "Rubrica padronizada ou NÃO INFORMADO.",
            "descr_conduta": "Conduta padronizada ou NÃO INFORMADO.",
        },
    ),
    TABELA_FATO: (
        "Fato no grão de uma natureza por registro-fonte Silver deduplicado.",
        {
            "id_registro_fonte": "Chave degenerada e linhagem até Bronze.",
            "num_bo": "Número do BO; pode repetir e não determina o grão sozinho.",
            "ano_bo": "Ano informado do BO.",
            "sk_tempo": "FK não nula para dim_tempo.",
            "sk_periodo_dia": "FK não nula para dim_periodo_dia.",
            "sk_natureza": "FK não nula para dim_natureza.",
            "mes_estatistica": "Mês estatístico informado na fonte.",
            "ano_estatistica": "Ano estatístico informado na fonte.",
            "qtd_ocorrencia": "Medida aditiva constante igual a 1.",
        },
    ),
    TABELA_TRANSFORMACOES: (
        "Matriz auditável das regras Silver/Gold e de seus impactos quantitativos.",
        {
            "ordem": "Ordem lógica da transformação.",
            "regra": "Nome conciso da regra aplicada.",
            "motivo": "Justificativa técnica ou analítica.",
            "campos_afetados": "Atributos envolvidos.",
            "linhas_antes": "Quantidade de linhas antes da regra.",
            "linhas_depois": "Quantidade de linhas depois da regra.",
            "linhas_afetadas": "Linhas alteradas, removidas ou encaminhadas à sentinela.",
            "dt_execucao": "Instante UTC da execução.",
        },
    ),
}

for tabela, (descricao, colunas) in comentarios.items():
    qualificada = f"`{CATALOG}`.`{SCHEMA}`.`{tabela}`"
    spark.sql(f"COMMENT ON TABLE {qualificada} IS {sql_literal(descricao)}")
    for coluna, descricao_coluna in colunas.items():
        spark.sql(
            f"ALTER TABLE {qualificada} ALTER COLUMN `{coluna}` COMMENT "
            + sql_literal(descricao_coluna)
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Evidências imediatas

# COMMAND ----------

print("Linhas por objeto:")
for tabela in [
    TABELA_BRONZE,
    TABELA_SILVER,
    TABELA_DIM_TEMPO,
    TABELA_DIM_PERIODO,
    TABELA_DIM_NATUREZA,
    TABELA_FATO,
    TABELA_TRANSFORMACOES,
]:
    print(f"  {tabela:<30} {spark.table(tabela).count():,}")

display(spark.table(TABELA_TRANSFORMACOES).orderBy("ordem"))
display(spark.table(TABELA_DIM_TEMPO).orderBy("sk_tempo").limit(20))
display(spark.table(TABELA_DIM_PERIODO).orderBy("sk_periodo_dia"))
display(spark.table(TABELA_DIM_NATUREZA).orderBy("sk_natureza").limit(20))

print("Silver e Gold concluídas sem alteração do grão.")
dbutils.notebook.exit("ok")
