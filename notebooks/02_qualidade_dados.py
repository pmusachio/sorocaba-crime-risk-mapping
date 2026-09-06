# Databricks notebook source
# =============================================================================
# MVP de Engenharia de Dados — Ocorrências registradas em Sorocaba
# Notebook 02 — Qualidade de Dados
#
# Este notebook:
#   1. perfila literalmente todas as colunas das camadas Bronze e Silver;
#   2. registra, para cada atributo, completude, consistência, unicidade,
#      acurácia contextual e outliers (ou N/A com justificativa);
#   3. compara atributos equivalentes antes e depois do tratamento;
#   4. valida escopo, domínios, hashes, dimensões, FKs e conservação da medida.
#
# Pré-requisito: executar 00_coleta_bronze e 01_pipeline_silver_gold.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # Qualidade de Dados
# MAGIC
# MAGIC A avaliação separa **qualidade observada na fonte** de **invariantes do
# MAGIC pipeline**. Nulos e valores incomuns podem ser características legítimas do
# MAGIC dado; já perda de registros, chave órfã ou fato multiplicado são falhas da
# MAGIC construção e interrompem a execução ao final.

# COMMAND ----------

from datetime import datetime, timezone
import re
import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "workspace"
SCHEMA = "sorocaba_seguranca"
COD_IBGE_SOROCABA = 3552205
ANOS_FONTE = (2022, 2023, 2024, 2025)

TABELA_MANIFESTO = "bronze_manifesto"
TABELA_BRONZE = "bronze_recorte_sorocaba"
TABELA_SILVER = "silver_ocorrencias"
TABELA_DIM_TEMPO = "dim_tempo"
TABELA_DIM_PERIODO = "dim_periodo_dia"
TABELA_DIM_NATUREZA = "dim_natureza"
TABELA_FATO = "fato_ocorrencia"
TABELA_TRANSFORMACOES = "matriz_transformacoes"
TABELA_PERFIL_BRONZE = "perfil_qualidade_bronze"
TABELA_PERFIL_SILVER = "perfil_qualidade_silver"
TABELA_VALIDACOES = "validacoes_qualidade"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

tabelas_necessarias = [
    TABELA_MANIFESTO,
    TABELA_BRONZE,
    TABELA_SILVER,
    TABELA_DIM_TEMPO,
    TABELA_DIM_PERIODO,
    TABELA_DIM_NATUREZA,
    TABELA_FATO,
    TABELA_TRANSFORMACOES,
]
tabelas_ausentes = [
    nome
    for nome in tabelas_necessarias
    if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{nome}")
]
if tabelas_ausentes:
    raise RuntimeError(
        "Execute os notebooks 00 e 01 antes deste notebook. "
        f"Tabelas ausentes: {', '.join(tabelas_ausentes)}"
    )

manifesto = spark.table(TABELA_MANIFESTO)
bronze = spark.table(TABELA_BRONZE)
silver = spark.table(TABELA_SILVER)
dim_tempo = spark.table(TABELA_DIM_TEMPO)
dim_periodo = spark.table(TABELA_DIM_PERIODO)
dim_natureza = spark.table(TABELA_DIM_NATUREZA)
fato = spark.table(TABELA_FATO)
transformacoes = spark.table(TABELA_TRANSFORMACOES)

print(f"Bronze: {bronze.count():,} linhas e {len(bronze.columns)} colunas")
print(f"Silver: {silver.count():,} linhas e {len(silver.columns)} colunas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Perfil completo de atributos
# MAGIC
# MAGIC Os perfis persistidos possuem uma linha para cada coluna da tabela analisada.
# MAGIC As cinco dimensões são sempre declaradas. Quando uma dimensão não é adequada
# MAGIC ao papel semântico do atributo, o resultado é `N/A` e a justificativa explica
# MAGIC por que aplicar aquela métrica produziria uma conclusão enganosa.

# COMMAND ----------

SENTINELAS_BRUTAS = {"", "NULL", "(VAZIO)", "N/A", "NA", "-"}
CAMPOS_OBRIGATORIOS_SILVER = {
    "id_registro_fonte",
    "cod_ibge",
    "_arquivo_origem",
    "_guia_origem",
    "_ano_arquivo",
    "_dt_ingestao",
}


def nome_normalizado(valor: str) -> str:
    """Normaliza um identificador apenas para comparação de nomes."""
    sem_acento = "".join(
        caractere
        for caractere in unicodedata.normalize("NFKD", valor)
        if not unicodedata.combining(caractere)
    )
    return re.sub(r"[^a-z0-9]+", "_", sem_acento.lower()).strip("_")


def col_segura(nome: str):
    """Referencia inclusive colunas de origem com espaços ou caracteres especiais."""
    return F.col(f"`{nome.replace('`', '``')}`")


def dividir_em_lotes(valores, tamanho=10):
    for inicio in range(0, len(valores), tamanho):
        yield valores[inicio:inicio + tamanho]


def metricas_basicas(df: DataFrame):
    """Calcula métricas exatas em lotes para evitar um plano Spark excessivamente largo."""
    total = df.count()
    resultados = {}

    for lote in dividir_em_lotes(df.schema.fields):
        expressoes = []
        for indice, campo in enumerate(lote):
            coluna = col_segura(campo.name)
            eh_texto = isinstance(campo.dataType, T.StringType)
            ausente = coluna.isNull()
            if eh_texto:
                ausente = ausente | (F.trim(coluna) == "")

            expressoes.extend([
                F.sum(ausente.cast("long")).alias(f"nulos_{indice}"),
                F.countDistinct(coluna).alias(f"distintos_{indice}"),
                F.min(coluna).cast("string").alias(f"minimo_{indice}"),
                F.max(coluna).cast("string").alias(f"maximo_{indice}"),
            ])

            if eh_texto:
                texto = F.upper(F.trim(coluna))
                expressoes.extend([
                    F.sum(texto.isin(*SENTINELAS_BRUTAS).cast("long"))
                    .alias(f"sentinelas_{indice}"),
                    F.sum(
                        (coluna.isNotNull() & (F.trim(coluna) != coluna)).cast("long")
                    ).alias(f"espacos_{indice}"),
                ])
            else:
                expressoes.extend([
                    F.lit(0).cast("long").alias(f"sentinelas_{indice}"),
                    F.lit(0).cast("long").alias(f"espacos_{indice}"),
                ])

        agregado = df.agg(*expressoes).first().asDict()
        for indice, campo in enumerate(lote):
            qtd_nulos = int(agregado[f"nulos_{indice}"] or 0)
            qtd_distintos = int(agregado[f"distintos_{indice}"] or 0)
            nao_nulos = max(total - qtd_nulos, 0)
            resultados[campo.name] = {
                "tipo_dado": campo.dataType.simpleString(),
                "total_linhas": total,
                "qtd_nulos": qtd_nulos,
                "pct_nulos": round(100.0 * qtd_nulos / total, 4) if total else 0.0,
                "qtd_distintos": qtd_distintos,
                "qtd_duplicados": max(nao_nulos - qtd_distintos, 0),
                "valor_min": agregado[f"minimo_{indice}"],
                "valor_max": agregado[f"maximo_{indice}"],
                "qtd_sentinelas": int(agregado[f"sentinelas_{indice}"] or 0),
                "qtd_espacos": int(agregado[f"espacos_{indice}"] or 0),
            }
    return resultados


def contagens_regras_silver(df: DataFrame):
    """Mede somente domínios sustentados pelo contrato do pipeline."""
    periodo_valido = ["MADRUGADA", "MANHÃ", "TARDE", "NOITE", "HORA INCERTA"]
    origem_valida = ["FONTE", "DERIVADO DA HORA", "NÃO INFORMADO"]
    ano_atual = datetime.now(timezone.utc).year

    hora = F.col("hora_ocorrencia_bo")
    periodo = F.upper(F.trim(F.col("periodo_dia")))
    origem = F.upper(F.trim(F.col("origem_periodo")))
    periodo_esperado = (
        F.when(hora.between(0, 5), F.lit("MADRUGADA"))
        .when(hora.between(6, 11), F.lit("MANHÃ"))
        .when(hora.between(12, 17), F.lit("TARDE"))
        .when(hora.between(18, 23), F.lit("NOITE"))
    )

    regras = {
        "id_registro_fonte": ~F.col("id_registro_fonte").rlike("^[0-9a-fA-F]{64}$"),
        "cod_ibge": F.col("cod_ibge").isNull() | (F.col("cod_ibge") != COD_IBGE_SOROCABA),
        "hora_ocorrencia_bo": hora.isNotNull() & ~hora.between(0, 23),
        "periodo_dia": F.col("periodo_dia").isNotNull() & ~periodo.isin(*periodo_valido),
        "origem_periodo": F.col("origem_periodo").isNull() | ~origem.isin(*origem_valida),
        "mes_estatistica": (
            F.col("mes_estatistica").isNotNull()
            & ~F.col("mes_estatistica").between(1, 12)
        ),
        "ano_estatistica": (
            F.col("ano_estatistica").isNotNull()
            & ~F.col("ano_estatistica").isin(*ANOS_FONTE)
        ),
        "_ano_arquivo": F.col("_ano_arquivo").isNull() | ~F.col("_ano_arquivo").isin(*ANOS_FONTE),
        "ano_bo": (
            F.col("ano_bo").isNotNull()
            & ~F.col("ano_bo").between(1900, ano_atual)
        ),
        "dt_ocorrencia_bo": (
            F.col("dt_ocorrencia_bo").isNotNull()
            & (
                (F.col("dt_ocorrencia_bo") < F.lit("1900-01-01").cast("date"))
                | (F.col("dt_ocorrencia_bo") > F.to_date(F.col("_dt_ingestao")))
            )
        ),
    }

    regras["periodo_dia"] = regras["periodo_dia"] | (
        (origem == "DERIVADO DA HORA")
        & (hora.isNull() | periodo_esperado.isNull() | (periodo != periodo_esperado))
    )

    agregado = df.agg(*[
        F.sum(condicao.cast("long")).alias(nome)
        for nome, condicao in regras.items()
    ]).first().asDict()
    return {nome: int(valor or 0) for nome, valor in agregado.items()}


def regra_impacto(nome: str) -> str:
    impactos = {
        "dt_ocorrencia_bo": "Perguntas 1 e 3: define ano, mês e dia da semana.",
        "hora_ocorrencia_bo": "Pergunta 3: define cobertura e período derivado.",
        "periodo_dia": "Pergunta 3: distribuição por período do dia.",
        "origem_periodo": "Pergunta 3: distingue informação da fonte de derivação.",
        "natureza_apurada": "Perguntas 2 e 3: agrupamento principal de natureza.",
        "rubrica": "Perguntas 2 e 3: detalha a classificação da ocorrência.",
        "descr_conduta": "Perguntas 2 e 3: complementa a classificação.",
        "id_registro_fonte": "Pipeline: linhagem, deduplicação e conservação do grão.",
        "cod_ibge": "Escopo: confirma que o recorte representa Sorocaba.",
        "num_bo": "Modelagem: identificador degenerado; não é único isoladamente.",
        "ano_bo": "Modelagem: complementa a identificação do BO.",
        "ano_estatistica": "Contexto: identifica o ano estatístico do arquivo oficial.",
        "mes_estatistica": "Contexto: identifica o mês estatístico do arquivo oficial.",
    }
    if nome in impactos:
        return impactos[nome]
    if nome.startswith("_"):
        return "Auditoria: rastreia arquivo, guia, ano e momento de ingestão."
    return "Fonte/linhagem: preservado na Bronze; não participa diretamente das três perguntas."


def montar_perfil(df: DataFrame, camada: str):
    basicas = metricas_basicas(df)
    inconsistencias_especificas = contagens_regras_silver(df) if camada == "SILVER" else {}
    agora = datetime.now(timezone.utc).replace(tzinfo=None)
    linhas = []

    for nome, metrica in basicas.items():
        qtd_nulos = metrica["qtd_nulos"]
        obrigatorio = camada == "SILVER" and nome in CAMPOS_OBRIGATORIOS_SILVER
        if qtd_nulos == 0:
            completude_status = "OK"
            completude_justificativa = "Não foram observados valores ausentes."
        elif obrigatorio:
            completude_status = "ERRO"
            completude_justificativa = "Atributo técnico obrigatório contém valor ausente."
        else:
            completude_status = "ATENÇÃO"
            completude_justificativa = (
                "Ausência registrada para análise de cobertura; não foi imputado valor sem evidência."
            )

        qtd_inconsistentes = inconsistencias_especificas.get(
            nome, metrica["qtd_sentinelas"] + metrica["qtd_espacos"]
        )
        if camada == "BRONZE":
            consistencia_status = "ATENÇÃO" if qtd_inconsistentes else "OK"
            consistencia_justificativa = (
                "Valores são preservados como recebidos. A contagem sinaliza sentinelas textuais "
                "ou espaços nas extremidades a tratar na Silver."
                if qtd_inconsistentes
                else "Nenhuma sentinela textual ou margem de espaço foi detectada."
            )
        elif nome in inconsistencias_especificas:
            consistencia_status = "ERRO" if qtd_inconsistentes else "OK"
            consistencia_justificativa = "Regra de domínio ou coerência definida pelo contrato do pipeline."
        elif metrica["tipo_dado"] == "string":
            consistencia_status = "ERRO" if qtd_inconsistentes else "OK"
            consistencia_justificativa = (
                "Verificação de sentinelas textuais e espaços nas extremidades após a limpeza."
            )
        else:
            consistencia_status = "N/A"
            consistencia_justificativa = (
                "Não existe regra de domínio adicional sustentada para este atributo; "
                "tipo e completude são avaliados separadamente."
            )

        if nome == "id_registro_fonte":
            unicidade_status = "OK" if metrica["qtd_duplicados"] == 0 else (
                "ATENÇÃO" if camada == "BRONZE" else "ERRO"
            )
            unicidade_justificativa = (
                "Hash de todas as colunas da linha de origem; duplicatas na Bronze são "
                "observações idênticas da fonte e devem desaparecer na Silver."
            )
        else:
            unicidade_status = "N/A"
            unicidade_justificativa = (
                "O atributo não é uma chave isolada e pode repetir legitimamente entre ocorrências."
            )

        if nome == "cod_ibge":
            sem_confirmacao = inconsistencias_especificas.get(nome, 0)
            acuracia_status = "OK" if camada == "SILVER" and sem_confirmacao == 0 else "ATENÇÃO"
            acuracia_justificativa = (
                f"Comparado ao código oficial usado no recorte de Sorocaba ({COD_IBGE_SOROCABA})."
            )
        elif nome in {"_ano_arquivo", "ano_estatistica"} and camada == "SILVER":
            sem_confirmacao = inconsistencias_especificas.get(nome, 0)
            acuracia_status = "OK" if sem_confirmacao == 0 else "ATENÇÃO"
            acuracia_justificativa = "Comparado ao período declarado dos quatro arquivos (2022–2025)."
        else:
            sem_confirmacao = None
            acuracia_status = "N/A"
            acuracia_justificativa = (
                "Não há gabarito externo independente no conjunto entregue para confirmar "
                "o valor individual sem criar uma suposição."
            )

        qtd_outliers = None
        outliers_status = "N/A"
        if nome == "dt_ocorrencia_bo" and camada == "SILVER":
            qtd_outliers = inconsistencias_especificas.get(nome, 0)
            outliers_status = "OK" if qtd_outliers == 0 else "ATENÇÃO"
            outliers_justificativa = (
                "IQR não se aplica a datas históricas. Foram verificadas somente datas "
                "impossíveis (< 1900 ou posteriores à ingestão)."
            )
        elif nome == "hora_ocorrencia_bo" and camada == "SILVER":
            qtd_outliers = inconsistencias_especificas.get(nome, 0)
            outliers_status = "OK" if qtd_outliers == 0 else "ATENÇÃO"
            outliers_justificativa = (
                "Hora é componente temporal discreto; foi validado o domínio fechado de 0 a 23."
            )
        else:
            outliers_justificativa = (
                "Atributo categórico, identificador, metadado ou componente temporal discreto; "
                "uma regra estatística de outlier não teria interpretação válida."
            )

        linhas.append((
            camada,
            nome,
            metrica["tipo_dado"],
            metrica["total_linhas"],
            qtd_nulos,
            metrica["pct_nulos"],
            metrica["qtd_distintos"],
            metrica["valor_min"],
            metrica["valor_max"],
            completude_status,
            completude_justificativa,
            consistencia_status,
            qtd_inconsistentes,
            consistencia_justificativa,
            unicidade_status,
            metrica["qtd_duplicados"],
            unicidade_justificativa,
            acuracia_status,
            sem_confirmacao,
            acuracia_justificativa,
            outliers_status,
            qtd_outliers,
            outliers_justificativa,
            regra_impacto(nome),
            agora,
        ))
    return linhas


schema_perfil = T.StructType([
    T.StructField("camada", T.StringType(), False),
    T.StructField("atributo", T.StringType(), False),
    T.StructField("tipo_dado", T.StringType(), False),
    T.StructField("total_linhas", T.LongType(), False),
    T.StructField("qtd_nulos", T.LongType(), False),
    T.StructField("pct_nulos", T.DoubleType(), False),
    T.StructField("qtd_distintos", T.LongType(), False),
    T.StructField("valor_min", T.StringType(), True),
    T.StructField("valor_max", T.StringType(), True),
    T.StructField("completude_status", T.StringType(), False),
    T.StructField("completude_justificativa", T.StringType(), False),
    T.StructField("consistencia_status", T.StringType(), False),
    T.StructField("qtd_inconsistentes", T.LongType(), False),
    T.StructField("consistencia_justificativa", T.StringType(), False),
    T.StructField("unicidade_status", T.StringType(), False),
    T.StructField("qtd_duplicados", T.LongType(), False),
    T.StructField("unicidade_justificativa", T.StringType(), False),
    T.StructField("acuracia_contextual_status", T.StringType(), False),
    T.StructField("qtd_sem_confirmacao", T.LongType(), True),
    T.StructField("acuracia_contextual_justificativa", T.StringType(), False),
    T.StructField("outliers_status", T.StringType(), False),
    T.StructField("qtd_outliers", T.LongType(), True),
    T.StructField("outliers_justificativa", T.StringType(), False),
    T.StructField("impacto_analitico", T.StringType(), False),
    T.StructField("dt_perfil", T.TimestampType(), False),
])

perfil_bronze = spark.createDataFrame(montar_perfil(bronze, "BRONZE"), schema_perfil)
perfil_silver = spark.createDataFrame(montar_perfil(silver, "SILVER"), schema_perfil)

perfil_bronze.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABELA_PERFIL_BRONZE)
perfil_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABELA_PERFIL_SILVER)

print("Perfil Bronze — uma linha por coluna:")
display(perfil_bronze.orderBy(F.desc("pct_nulos"), "atributo"))
print("Perfil Silver — uma linha por coluna:")
display(perfil_silver.orderBy(F.desc("pct_nulos"), "atributo"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Comparação Bronze × Silver
# MAGIC
# MAGIC A comparação usa somente pares com equivalência semântica conhecida. Os
# MAGIC demais campos Bronze continuam presentes no perfil completo, mesmo quando
# MAGIC foram deliberadamente excluídos da Silver por não participarem do MVP.

# COMMAND ----------


def encontrar_coluna(df: DataFrame, candidatos):
    disponiveis = {nome_normalizado(nome): nome for nome in df.columns}
    for candidato in candidatos:
        encontrado = disponiveis.get(nome_normalizado(candidato))
        if encontrado:
            return encontrado
    return None


pares_linhagem = {
    "cod_ibge": ["CD_IBGE", "COD_IBGE", "COD IBGE"],
    "num_bo": ["NUM_BO", "NUM BO"],
    "ano_bo": ["ANO_BO", "ANO BO"],
    "dt_ocorrencia_bo": ["DATA_OCORRENCIA_BO", "DATA OCORRENCIA BO"],
    "hora_ocorrencia_bo": ["HORA_OCORRENCIA_BO", "HORA OCORRENCIA BO"],
    "periodo_dia": ["DESCR_PERIODO", "DESC_PERIODO", "DESCR PERIODO", "DESC PERIODO"],
    "rubrica": ["RUBRICA"],
    "natureza_apurada": ["NATUREZA_APURADA", "NATUREZA APURADA"],
    "descr_conduta": ["DESCR_CONDUTA", "DESCR CONDUTA"],
    "mes_estatistica": ["MES_ESTATISTICA", "MES ESTATISTICA"],
    "ano_estatistica": ["ANO_ESTATISTICA", "ANO ESTATISTICA"],
    "_arquivo_origem": ["_arquivo_origem"],
    "_guia_origem": ["_guia_origem"],
    "_ano_arquivo": ["_ano_arquivo"],
}

metricas_b = {r["atributo"]: r.asDict() for r in perfil_bronze.collect()}
metricas_s = {r["atributo"]: r.asDict() for r in perfil_silver.collect()}
comparacao = []
for atributo_silver, candidatos in pares_linhagem.items():
    atributo_bronze = encontrar_coluna(bronze, candidatos)
    if atributo_bronze and atributo_silver in metricas_s:
        b = metricas_b[atributo_bronze]
        s = metricas_s[atributo_silver]
        comparacao.append((
            atributo_bronze,
            atributo_silver,
            b["pct_nulos"],
            s["pct_nulos"],
            b["qtd_inconsistentes"],
            s["qtd_inconsistentes"],
            b["qtd_distintos"],
            s["qtd_distintos"],
        ))

schema_comparacao = """
    atributo_bronze string,
    atributo_silver string,
    pct_ausente_bronze double,
    pct_ausente_silver double,
    inconsistencias_bronze long,
    inconsistencias_silver long,
    distintos_bronze long,
    distintos_silver long
"""
df_comparacao = spark.createDataFrame(comparacao, schema_comparacao)
display(df_comparacao.orderBy("atributo_silver"))

tratamentos_com_reducao = df_comparacao.filter(
    F.col("inconsistencias_silver") < F.col("inconsistencias_bronze")
).count()
print(
    f"Foram comparados {df_comparacao.count()} pares semânticos; "
    f"{tratamentos_com_reducao} reduziram sentinelas/espaços ou violações de domínio."
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Validações do pipeline
# MAGIC
# MAGIC `ERRO` significa quebra de um contrato que o próprio pipeline deve garantir.
# MAGIC `ATENÇÃO` registra uma limitação ou anomalia da fonte sem inventar correção.

# COMMAND ----------

validacoes = []
dt_validacao = datetime.now(timezone.utc).replace(tzinfo=None)


def registrar(categoria, validacao, observado, esperado, status, detalhe):
    validacoes.append((
        categoria,
        validacao,
        str(observado),
        str(esperado),
        status,
        detalhe,
        dt_validacao,
    ))


def status_zero(valor, severidade="ERRO"):
    return "OK" if valor == 0 else severidade


anos_manifesto = {r[0] for r in manifesto.select("ano_arquivo").distinct().collect()}
registrar(
    "COLETA",
    "Quatro anos completos no manifesto",
    sorted(anos_manifesto),
    list(ANOS_FONTE),
    "OK" if anos_manifesto == set(ANOS_FONTE) else "ERRO",
    "O recorte analítico usa somente arquivos anuais completos de 2022 a 2025.",
)

qtd_manifesto = manifesto.count()
arquivos_manifesto = manifesto.select("arquivo").distinct().count()
registrar(
    "COLETA",
    "Exatamente quatro arquivos distintos no manifesto",
    f"linhas={qtd_manifesto}; arquivos={arquivos_manifesto}",
    "linhas=4; arquivos=4",
    "OK" if qtd_manifesto == 4 and arquivos_manifesto == 4 else "ERRO",
    "Cada ano completo deve corresponder a um único XLSX original.",
)

manifesto_invalido = manifesto.filter(
    F.col("sha256").isNull()
    | ~F.col("sha256").rlike("^[0-9a-fA-F]{64}$")
    | F.col("tamanho_bytes").isNull()
    | (F.col("tamanho_bytes") <= 0)
    | (F.col("status") != "OK")
).count()
registrar(
    "COLETA",
    "Checksum e tamanho dos arquivos",
    manifesto_invalido,
    0,
    status_zero(manifesto_invalido),
    "Cada arquivo deve ter SHA-256 válido e tamanho maior que zero.",
)

cod_bronze = encontrar_coluna(bronze, ["CD_IBGE", "COD_IBGE", "COD IBGE"])
if cod_bronze:
    cod_bronze_sql = (
        "try_cast(try_cast("
        f"`{cod_bronze.replace('`', '``')}` AS DECIMAL(20,3)) AS BIGINT)"
    )
    fora_escopo_bronze = bronze.filter(
        F.expr(cod_bronze_sql).isNull()
        | (F.expr(cod_bronze_sql) != COD_IBGE_SOROCABA)
    ).count()
else:
    fora_escopo_bronze = bronze.count()
registrar(
    "ESCOPO",
    "Bronze contém somente o código IBGE de Sorocaba",
    fora_escopo_bronze,
    0,
    status_zero(fora_escopo_bronze),
    "O filtro ocorre durante a leitura em lotes, antes da escrita da tabela Bronze.",
)

fora_escopo_silver = silver.filter(
    F.col("cod_ibge").isNull() | (F.col("cod_ibge") != COD_IBGE_SOROCABA)
).count()
registrar(
    "ESCOPO",
    "Silver contém somente o código IBGE de Sorocaba",
    fora_escopo_silver,
    0,
    status_zero(fora_escopo_silver),
    "A Silver confirma novamente o filtro municipal.",
)

hash_bronze_duplicado = bronze.groupBy("id_registro_fonte").count().filter("count > 1").count()
hash_bronze_formato = bronze.filter(
    F.col("id_registro_fonte").isNull()
    | ~F.col("id_registro_fonte").rlike("^[0-9a-fA-F]{64}$")
).count()
hash_silver_duplicado = silver.groupBy("id_registro_fonte").count().filter("count > 1").count()
hash_silver_formato = silver.filter(
    F.col("id_registro_fonte").isNull()
    | ~F.col("id_registro_fonte").rlike("^[0-9a-fA-F]{64}$")
).count()
hash_silver_sem_bronze = silver.select("id_registro_fonte").join(
    bronze.select("id_registro_fonte").distinct(),
    "id_registro_fonte",
    "left_anti",
).count()

registrar(
    "UNICIDADE",
    "Duplicatas idênticas observadas na Bronze",
    hash_bronze_duplicado,
    "informativo",
    "INFORMATIVO",
    "Grupos repetidos são removidos exclusivamente pelo hash da linha original.",
)
registrar(
    "LINHAGEM",
    "Hash Bronze presente e no formato SHA-256",
    hash_bronze_formato,
    0,
    status_zero(hash_bronze_formato),
    "Toda linha Bronze deve possuir identificador de 64 algarismos hexadecimais.",
)
registrar(
    "UNICIDADE",
    "Hash único após deduplicação Silver",
    hash_silver_duplicado,
    0,
    status_zero(hash_silver_duplicado),
    "Nenhum hash pode aparecer mais de uma vez na Silver.",
)
registrar(
    "LINHAGEM",
    "Formato SHA-256 do id_registro_fonte",
    hash_silver_formato,
    0,
    status_zero(hash_silver_formato),
    "O identificador deve conter 64 algarismos hexadecimais.",
)
registrar(
    "LINHAGEM",
    "Todo hash Silver existe na Bronze",
    hash_silver_sem_bronze,
    0,
    status_zero(hash_silver_sem_bronze),
    "Garante rastreabilidade de cada registro tratado até a captura bruta.",
)

bronze_distintos = bronze.select("id_registro_fonte").distinct().count()
total_silver = silver.count()
registrar(
    "CONSERVAÇÃO",
    "Silver corresponde aos hashes distintos da Bronze",
    total_silver,
    bronze_distintos,
    "OK" if total_silver == bronze_distintos else "ERRO",
    "A única remoção de linhas permitida entre Bronze e Silver é duplicata idêntica.",
)

contagens_silver = contagens_regras_silver(silver)
for atributo in [
    "hora_ocorrencia_bo",
    "periodo_dia",
    "origem_periodo",
    "mes_estatistica",
    "ano_estatistica",
    "_ano_arquivo",
    "ano_bo",
]:
    valor = contagens_silver[atributo]
    registrar(
        "DOMÍNIO",
        f"Domínio válido de {atributo}",
        valor,
        0,
        status_zero(valor),
        "Domínio definido pelo contrato da Silver e pelas regras de transformação.",
    )

datas_impossiveis = contagens_silver["dt_ocorrencia_bo"]
registrar(
    "TEMPORAL",
    "Datas impossíveis de ocorrência",
    datas_impossiveis,
    0,
    status_zero(datas_impossiveis, severidade="ATENÇÃO"),
    "Datas anteriores a 1900 ou posteriores à ingestão são sinalizadas; datas antigas "
    "plausíveis não são descartadas automaticamente.",
)

linhas_transformacao = transformacoes.count()
transformacoes_sem_contagem = transformacoes.filter(
    F.col("linhas_antes").isNull()
    | F.col("linhas_depois").isNull()
    | F.col("linhas_afetadas").isNull()
    | (F.col("linhas_antes") < 0)
    | (F.col("linhas_depois") < 0)
    | (F.col("linhas_afetadas") < 0)
).count()
registrar(
    "TRANSFORMAÇÃO",
    "Matriz de transformações preenchida",
    linhas_transformacao,
    "> 0",
    "OK" if linhas_transformacao > 0 else "ERRO",
    "Cada regra deve registrar motivo, campos e impacto quantitativo.",
)
registrar(
    "TRANSFORMAÇÃO",
    "Contagens válidas na matriz de transformações",
    transformacoes_sem_contagem,
    0,
    status_zero(transformacoes_sem_contagem),
    "Nenhuma transformação pode omitir as contagens antes/depois/afetadas.",
)

dimensoes = [
    (TABELA_DIM_TEMPO, dim_tempo, "sk_tempo"),
    (TABELA_DIM_PERIODO, dim_periodo, "sk_periodo_dia"),
    (TABELA_DIM_NATUREZA, dim_natureza, "sk_natureza"),
]
for nome_tabela, dimensao, chave in dimensoes:
    qtd_sentinela = dimensao.filter(F.col(chave) == -1).count()
    chaves_repetidas = dimensao.groupBy(chave).count().filter("count > 1").count()
    registrar(
        "DIMENSÃO",
        f"Uma sentinela em {nome_tabela}",
        qtd_sentinela,
        1,
        "OK" if qtd_sentinela == 1 else "ERRO",
        "Cada dimensão possui exatamente um membro desconhecido com chave -1.",
    )
    registrar(
        "DIMENSÃO",
        f"Chave única em {nome_tabela}",
        chaves_repetidas,
        0,
        status_zero(chaves_repetidas),
        f"A chave {chave} não pode se repetir.",
    )

fks = [
    ("sk_tempo", dim_tempo),
    ("sk_periodo_dia", dim_periodo),
    ("sk_natureza", dim_natureza),
]
for chave, dimensao in fks:
    fks_nulas = fato.filter(F.col(chave).isNull()).count()
    orfas = fato.select(chave).distinct().join(
        dimensao.select(chave).distinct(), chave, "left_anti"
    ).count()
    registrar(
        "INTEGRIDADE",
        f"FK {chave} não nula",
        fks_nulas,
        0,
        status_zero(fks_nulas),
        "Ausências devem apontar para a sentinela -1, nunca para NULL.",
    )
    registrar(
        "INTEGRIDADE",
        f"FK {chave} sem órfãos",
        orfas,
        0,
        status_zero(orfas),
        "Toda FK da fato deve existir na dimensão correspondente.",
    )

total_fato = fato.count()
soma_medida = fato.agg(F.sum("qtd_ocorrencia").alias("total")).first()["total"] or 0
medida_invalida = fato.filter(F.col("qtd_ocorrencia") != 1).count()
hash_fato_duplicado = fato.groupBy("id_registro_fonte").count().filter("count > 1").count()
hash_fato_sem_silver = fato.select("id_registro_fonte").join(
    silver.select("id_registro_fonte"), "id_registro_fonte", "left_anti"
).count()

registrar(
    "CONSERVAÇÃO",
    "Uma linha fato por linha Silver",
    total_fato,
    total_silver,
    "OK" if total_fato == total_silver else "ERRO",
    "O join dimensional não pode multiplicar nem eliminar ocorrências.",
)
registrar(
    "CONSERVAÇÃO",
    "Soma de qtd_ocorrencia",
    soma_medida,
    total_silver,
    "OK" if soma_medida == total_silver else "ERRO",
    "A medida aditiva deve conservar o total Silver deduplicado.",
)
registrar(
    "CONSERVAÇÃO",
    "qtd_ocorrencia unitária em toda a fato",
    medida_invalida,
    0,
    status_zero(medida_invalida),
    "Toda linha do grão representa exatamente uma ocorrência registrada.",
)
registrar(
    "GRÃO",
    "Uma linha fato por id_registro_fonte",
    hash_fato_duplicado,
    0,
    status_zero(hash_fato_duplicado),
    "O grão é uma natureza por registro original deduplicado.",
)
registrar(
    "LINHAGEM",
    "Todo hash da fato existe na Silver",
    hash_fato_sem_silver,
    0,
    status_zero(hash_fato_sem_silver),
    "Mantém a rastreabilidade da Gold até a Silver.",
)

qtd_perfil_bronze = perfil_bronze.select("atributo").distinct().count()
qtd_perfil_silver = perfil_silver.select("atributo").distinct().count()
registrar(
    "PERFIL",
    "Todas as colunas Bronze perfiladas",
    qtd_perfil_bronze,
    len(bronze.columns),
    "OK" if qtd_perfil_bronze == len(bronze.columns) else "ERRO",
    "Deve existir exatamente uma linha de perfil para cada atributo Bronze.",
)
registrar(
    "PERFIL",
    "Todas as colunas Silver perfiladas",
    qtd_perfil_silver,
    len(silver.columns),
    "OK" if qtd_perfil_silver == len(silver.columns) else "ERRO",
    "Deve existir exatamente uma linha de perfil para cada atributo Silver.",
)

schema_validacoes = T.StructType([
    T.StructField("categoria", T.StringType(), False),
    T.StructField("validacao", T.StringType(), False),
    T.StructField("resultado_observado", T.StringType(), False),
    T.StructField("resultado_esperado", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("detalhe", T.StringType(), False),
    T.StructField("dt_validacao", T.TimestampType(), False),
])
df_validacoes = spark.createDataFrame(validacoes, schema_validacoes)
df_validacoes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABELA_VALIDACOES)

display(df_validacoes.orderBy("categoria", "validacao"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Registro no catálogo

# COMMAND ----------


def sql_texto(valor: str) -> str:
    return valor.replace("'", "''")


def comentar_tabela(nome_tabela: str, descricao: str, colunas: dict):
    qualificada = f"`{CATALOG}`.`{SCHEMA}`.`{nome_tabela}`"
    spark.sql(f"COMMENT ON TABLE {qualificada} IS '{sql_texto(descricao)}'")
    for nome_coluna, comentario in colunas.items():
        spark.sql(
            f"ALTER TABLE {qualificada} ALTER COLUMN `{nome_coluna}` "
            f"COMMENT '{sql_texto(comentario)}'"
        )


comentarios_perfil = {
    "camada": "Camada de origem do perfil: BRONZE ou SILVER.",
    "atributo": "Nome exato do atributo perfilado.",
    "tipo_dado": "Tipo Spark/Delta observado na execução.",
    "total_linhas": "Quantidade de linhas da tabela no momento do perfil.",
    "qtd_nulos": "Quantidade de nulos; para texto, inclui string vazia.",
    "pct_nulos": "Percentual de valores ausentes em relação ao total.",
    "qtd_distintos": "Quantidade exata de valores não nulos distintos.",
    "valor_min": "Menor valor observado convertido para texto.",
    "valor_max": "Maior valor observado convertido para texto.",
    "completude_status": "Resultado da dimensão de completude.",
    "completude_justificativa": "Interpretação da completude do atributo.",
    "consistencia_status": "Resultado de domínio, formato ou coerência interna.",
    "qtd_inconsistentes": "Quantidade de valores que violam a regra aplicável.",
    "consistencia_justificativa": "Regra de consistência usada ou razão para N/A.",
    "unicidade_status": "Resultado da dimensão de unicidade ou N/A.",
    "qtd_duplicados": "Repetições não nulas além da primeira ocorrência do valor.",
    "unicidade_justificativa": "Expectativa de unicidade conforme o papel do atributo.",
    "acuracia_contextual_status": "Comparação contextual disponível ou N/A.",
    "qtd_sem_confirmacao": "Valores divergentes quando existe referência contextual.",
    "acuracia_contextual_justificativa": "Referência usada ou razão para N/A.",
    "outliers_status": "Resultado de outliers ou N/A.",
    "qtd_outliers": "Quantidade sinalizada quando a regra é aplicável.",
    "outliers_justificativa": "Regra aplicada ou razão semântica para N/A.",
    "impacto_analitico": "Efeito potencial do atributo nas perguntas ou na auditoria.",
    "dt_perfil": "Instante UTC de geração do perfil.",
}
comentar_tabela(
    TABELA_PERFIL_BRONZE,
    "Perfil atributo a atributo da captura Bronze preservada.",
    comentarios_perfil,
)
comentar_tabela(
    TABELA_PERFIL_SILVER,
    "Perfil atributo a atributo da Silver após tipagem e padronização.",
    comentarios_perfil,
)
comentar_tabela(
    TABELA_VALIDACOES,
    "Validações executáveis de coleta, escopo, domínios, linhagem e integridade.",
    {
        "categoria": "Grupo temático da validação.",
        "validacao": "Nome da regra avaliada.",
        "resultado_observado": "Valor calculado na execução.",
        "resultado_esperado": "Contrato esperado para aprovação.",
        "status": "OK, ATENÇÃO, INFORMATIVO ou ERRO.",
        "detalhe": "Interpretação e fundamento da regra.",
        "dt_validacao": "Instante UTC da execução.",
    },
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Síntese automática

# COMMAND ----------

resumo_status = {
    r["status"]: r["total"]
    for r in df_validacoes.groupBy("status").count()
    .withColumnRenamed("count", "total")
    .collect()
}
atributos_bronze_atencao = perfil_bronze.filter(
    (F.col("completude_status") == "ATENÇÃO")
    | (F.col("consistencia_status") == "ATENÇÃO")
).count()
atributos_silver_atencao = perfil_silver.filter(
    (F.col("completude_status") == "ATENÇÃO")
    | (F.col("consistencia_status") == "ATENÇÃO")
).count()
falhas_pipeline = int(resumo_status.get("ERRO", 0))

displayHTML(f"""
<h3>Síntese da execução</h3>
<ul>
  <li>A Bronze teve <strong>{len(bronze.columns)}</strong> atributos perfilados;
      {atributos_bronze_atencao} possuem ausência ou inconsistência bruta a observar.</li>
  <li>A Silver teve <strong>{len(silver.columns)}</strong> atributos perfilados;
      {atributos_silver_atencao} permanecem com cobertura parcial ou alerta.</li>
  <li>Foram executadas <strong>{df_validacoes.count()}</strong> validações:
      {resumo_status.get('OK', 0)} OK, {resumo_status.get('ATENÇÃO', 0)} em atenção,
      {resumo_status.get('INFORMATIVO', 0)} informativa(s) e {falhas_pipeline} erro(s).</li>
  <li>Atenções descrevem limitações do dado e devem ser consideradas nas análises;
      erros indicam quebra reproduzível do contrato do pipeline.</li>
</ul>
""")

if falhas_pipeline:
    display(df_validacoes.filter(F.col("status") == "ERRO"))
    raise AssertionError(
        f"Qualidade reprovada: {falhas_pipeline} invariante(s) do pipeline falharam."
    )

print("Qualidade aprovada: nenhum invariante técnico falhou.")
dbutils.notebook.exit("ok")
