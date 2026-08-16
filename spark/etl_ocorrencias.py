"""
ETL das ocorrências criminais de Sorocaba — job PySpark para o Dataproc Serverless.

Etapa 4 do trabalho (Carga). Implementa o componente de ETL da arquitetura de
ambiente de BI descrita na Aula 1: "extração, transformação e carga é o
componente da arquitetura do ambiente de BI que operacionaliza o processo de
alimentação do DW a partir de fontes de dados operacionais. Diversas questões
de integração (padronização, heterogeneidade semântica) e tratamento da
qualidade dos dados (tratamento de dados faltantes, resolução de
inconsistências) são tratadas durante a execução desse processo."

    Extração ......: lê a zona preparada do data lake (Parquet, 5 anos)
    Transformação .: concilia os esquemas divergentes entre anos, converte
                     sentinelas em nulo, tipa os campos, deriva atributos
                     ausentes e filtra o município de Sorocaba
    Carga .........: grava a tabela conformada no BigQuery (área de staging),
                     de onde a modelagem dimensional é feita em SQL

Por que processamento distribuído: o filtro por município só pode ser aplicado
depois de ler o estado inteiro de São Paulo — são cerca de 5,5 milhões de
registros nos cinco arquivos, dos quais aproximadamente 1,5% são de Sorocaba.

Execução (a partir do notebook 02):

    gcloud dataproc batches submit pyspark spark/etl_ocorrencias.py \
        --region=southamerica-east1 \
        -- --projeto=PROJETO --bucket=BUCKET
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# De-para dos nomes de coluna
#
# A fonte trocou a grafia de vários campos ao longo dos anos. O levantamento
# está no notebook 01 (seção "Descoberta do esquema") e resultou neste mapa:
# cada campo canônico do data warehouse aceita todas as grafias já publicadas.
# A comparação é feita sobre o nome NORMALIZADO (maiúsculo, sem acento, com
# underscore no lugar de espaço), de modo que variações de acentuação ou de
# caixa em anos futuros não quebrem o pipeline.
#
#   campo canônico            -> grafias observadas (2022 a 2026)
# --------------------------------------------------------------------------
DE_PARA = {
    # identificação do boletim
    "num_bo":                     ["NUM_BO"],
    "ano_bo":                     ["ANO_BO"],
    # datas e hora
    "data_registro":              ["DATA_REGISTRO", "DATA_COMUNICACAO_BO"],
    "data_ocorrencia":            ["DATA_OCORRENCIA_BO"],
    "hora_ocorrencia":            ["HORA_OCORRENCIA_BO"],
    "periodo_ocorrencia":         ["DESC_PERIODO", "DESCR_PERIODO"],
    "mes_estatistica":            ["MES_ESTATISTICA"],
    "ano_estatistica":            ["ANO_ESTATISTICA"],
    # local do fato (circunscrição)
    "cod_ibge_municipio":         ["COD_IBGE", "CD_IBGE"],
    "municipio_circunscricao":    ["NOME_MUNICIPIO_CIRCUNSCRICAO", "NOME_MUNICIPIO_CIRCUNCRICAO"],
    "delegacia_circunscricao":    ["NOME_DELEGACIA_CIRCUNSCRICAO", "NOME_DELEGACIA_CIRCUNCRICAO"],
    "seccional_circunscricao":    ["NOME_SECCIONAL_CIRCUNSCRICAO", "NOME_SECCIONAL_CIRCUNCRICAO"],
    "departamento_circunscricao": ["NOME_DEPARTAMENTO_CIRCUNSCRICAO", "NOME_DEPARTAMENTO_CIRCUNCRICAO"],
    # local do registro (para conferência: pode divergir da circunscrição)
    "municipio_registro":         ["NOME_MUNICIPIO", "CIDADE"],
    "delegacia_registro":         ["NOME_DELEGACIA"],
    "seccional_registro":         ["NOME_SECCIONAL"],
    "departamento_registro":      ["NOME_DEPARTAMENTO"],
    # caracterização do local
    "tipo_local":                 ["DESCR_TIPOLOCAL"],      # ausente em 2022, 2023 e 2024
    "subtipo_local":              ["DESCR_SUBTIPOLOCAL"],
    "bairro":                     ["BAIRRO"],
    "latitude":                   ["LATITUDE"],
    "longitude":                  ["LONGITUDE"],
    # natureza criminal
    "rubrica":                    ["RUBRICA"],
    "conduta":                    ["DESCR_CONDUTA"],
    "natureza_apurada":           ["NATUREZA_APURADA"],
    # área da Polícia Militar
    "comando_pm":                 ["CMD"],
    "batalhao_pm":                ["BTL"],
    "companhia_pm":               ["CIA"],
    # auditoria acrescentada na ingestão
    "arquivo_origem":             ["_ARQUIVO_ORIGEM"],
    "guia_origem":                ["_GUIA_ORIGEM"],
    "dt_ingestao":                ["_DT_INGESTAO"],
    "ano_arquivo":                ["ANO_ARQUIVO"],
}

# LOGRADOURO e NUMERO_LOGRADOURO existem na origem e são deliberadamente
# deixados de fora da carga: identificam o endereço exato do fato e não são
# necessários a nenhuma pergunta de negócio (decisão de LGPD registrada em
# docs/09-linhagem.md).

# Valores que a fonte usa para representar ausência de dado. Chegam intactos
# da zona preparada, de propósito, para que a análise de qualidade possa
# evidenciá-los; é aqui que viram nulo de verdade.
SENTINELAS = ["NULL", "(Vazio)", "(VAZIO)", "-", ""]

# Proporção mínima que o tipo de local predominante precisa ter, dentro de um
# subtipo, para que a derivação seja considerada confiável. Ver derivar_tipo_local.
LIMIAR_CONFIANCA_TIPO_LOCAL = 0.95

# Faixas horárias correspondentes às categorias de período usadas pela fonte.
# Servem para derivar o período quando a fonte não o informou, mas a hora existe.
FAIXAS_PERIODO = [
    (0, 5, "DE MADRUGADA"),
    (6, 11, "PELA MANHA"),
    (12, 17, "A TARDE"),
    (18, 23, "A NOITE"),
]


def normalizar_nome(nome: str) -> str:
    """Nome de coluna canônico: maiúsculo, sem acento, espaço vira underscore."""
    import unicodedata
    decomposto = unicodedata.normalize("NFKD", str(nome))
    sem_acento = "".join(c for c in decomposto if not unicodedata.combining(c))
    return sem_acento.strip().upper().replace(" ", "_")


def conciliar_esquema(df):
    """Reduz as várias grafias de cada ano a um único conjunto de colunas.

    Esta é a etapa de reconciliação exigida pelo processo de ETL: os arquivos
    de 2022 a 2026 descrevem os mesmos conceitos com nomes diferentes
    (CIDADE x NOME_MUNICIPIO, DATA_COMUNICACAO_BO x DATA_REGISTRO,
    CIRCUNCRIÇÃO x CIRCUNSCRICAO, CD_IBGE x COD IBGE). Quando um campo não
    existe em determinado ano, ele é criado como nulo.
    """
    disponiveis = {normalizar_nome(c): c for c in df.columns}
    selecao = []
    ausentes = {}

    for canonico, grafias in DE_PARA.items():
        origens = [disponiveis[g] for g in grafias if g in disponiveis]
        if not origens:
            ausentes[canonico] = grafias
            selecao.append(F.lit(None).cast("string").alias(canonico))
        elif len(origens) == 1:
            selecao.append(F.col("`%s`" % origens[0]).cast("string").alias(canonico))
        else:
            # Mais de uma grafia presente no mesmo conjunto de arquivos: o
            # coalesce resolve linha a linha, já que cada ano preenche a sua.
            selecao.append(
                F.coalesce(*[F.col("`%s`" % o).cast("string") for o in origens]).alias(canonico)
            )

    if ausentes:
        print(f"[esquema] campos ausentes em todos os arquivos lidos: {sorted(ausentes)}")
    return df.select(*selecao)


def limpar_sentinelas(df, colunas):
    """Converte as sentinelas textuais da fonte em nulo de verdade."""
    for coluna in colunas:
        limpa = F.trim(F.col(coluna))
        df = df.withColumn(
            coluna,
            F.when(limpa.isin(SENTINELAS), F.lit(None)).otherwise(limpa),
        )
    return df


def derivar_tipo_local(df_municipio, df_estado):
    """Preenche o tipo de local nos anos em que a fonte não publicou o campo.

    DESCR_TIPOLOCAL só existe nos arquivos de 2025 e 2026; DESCR_SUBTIPOLOCAL
    existe em todos. Como o subtipo é, por definição da própria fonte, um
    "subgrupo de tipos de locais, vinculado ao tipo de local", a correspondência
    subtipo -> tipo é extraída dos anos que publicam os dois campos e aplicada
    aos anos anteriores.

    A correspondência é calculada sobre o estado INTEIRO, e não apenas sobre o
    município: quanto mais registros, melhor a cobertura de subtipos raros e
    mais confiável a escolha do tipo predominante.

    Duas colunas registram o que foi feito, porque a derivação não é exata:

        origem_tipo_local  -> se o valor veio publicado ou foi derivado;
        tipo_local_ambiguo -> se o subtipo aparece associado a mais de um tipo
                              na fonte (ex.: 'Lojas' aparece como 'Condomínio
                              Comercial' e como 'Shopping Center'), caso em que
                              prevalece o mais frequente e a marcação alerta
                              que aquele valor pode estar errado.

    Sem essas duas colunas, um dado derivado seria indistinguível de um dado
    publicado — exatamente o tipo de opacidade que o registro de linhagem
    existe para evitar.
    """
    pares = (
        df_estado
        .filter(F.col("tipo_local").isNotNull() & F.col("subtipo_local").isNotNull())
        .groupBy("subtipo_local", "tipo_local").count()
    )
    por_subtipo = Window.partitionBy("subtipo_local")
    ordem = por_subtipo.orderBy(F.col("count").desc(), F.col("tipo_local"))
    correspondencia = (
        pares
        .withColumn("posicao", F.row_number().over(ordem))
        .withColumn("total_subtipo", F.sum("count").over(por_subtipo))
        .filter(F.col("posicao") == 1)
        .select(
            "subtipo_local",
            F.col("tipo_local").alias("tipo_local_derivado"),
            (F.col("count") / F.col("total_subtipo")).alias("confianca"),
        )
        # Um subtipo é considerado ambíguo quando o tipo predominante responde
        # por menos de 95% das suas ocorrências. O limiar existe porque a
        # contagem bruta de tipos distintos é enganosa: 'Via Pública' aparece
        # 1.151.726 vezes como 'Via Pública' e 4 vezes com outros tipos — são
        # erros de digitação da fonte, não ambiguidade real.
        .withColumn("subtipo_ambiguo", F.col("confianca") < LIMIAR_CONFIANCA_TIPO_LOCAL)
    )
    total = correspondencia.count()
    ambiguos = correspondencia.filter("subtipo_ambiguo").count()
    print(f"[tipo_local] correspondências subtipo -> tipo: {total} "
          f"({ambiguos} com confiança abaixo de {LIMIAR_CONFIANCA_TIPO_LOCAL:.0%})")

    return (
        df_municipio.join(F.broadcast(correspondencia), on="subtipo_local", how="left")
        .withColumn(
            "origem_tipo_local",
            F.when(F.col("tipo_local").isNotNull(), F.lit("publicado pela fonte"))
             .when(F.col("tipo_local_derivado").isNotNull(), F.lit("derivado do subtipo"))
             .otherwise(F.lit("não informado")),
        )
        .withColumn(
            "tipo_local_ambiguo",
            F.col("tipo_local").isNull() & F.coalesce(F.col("subtipo_ambiguo"), F.lit(False)),
        )
        .withColumn("tipo_local", F.coalesce("tipo_local", "tipo_local_derivado"))
        .drop("tipo_local_derivado", "subtipo_ambiguo", "confianca")
    )


def derivar_periodo(df):
    """Completa o período do dia a partir da hora, quando a fonte não o informou.

    A regra usa as mesmas quatro categorias da fonte e é registrada na coluna
    origem_periodo.
    """
    faixa = F.lit(None).cast("string")
    for inicio, fim, rotulo in FAIXAS_PERIODO:
        faixa = F.when(F.col("hora").between(inicio, fim), F.lit(rotulo)).otherwise(faixa)

    periodo_fonte = F.upper(
        F.translate(F.col("periodo_ocorrencia"), "ÁÀÂÃÉÊÍÓÔÕÚÇáàâãéêíóôõúç", "AAAAEEIOOOUCAAAAEEIOOOUC")
    )
    return (
        df.withColumn("periodo_normalizado", periodo_fonte)
          .withColumn("periodo_derivado", faixa)
          .withColumn(
              "origem_periodo",
              F.when(F.col("periodo_normalizado").isNotNull(), F.lit("publicado pela fonte"))
               .when(F.col("periodo_derivado").isNotNull(), F.lit("derivado da hora"))
               .otherwise(F.lit("não informado")),
          )
          .withColumn("periodo_ocorrencia",
                      F.coalesce("periodo_normalizado", "periodo_derivado", F.lit("NAO INFORMADO")))
          .drop("periodo_normalizado", "periodo_derivado")
    )


def main():
    argumentos = argparse.ArgumentParser(description=__doc__)
    argumentos.add_argument("--projeto", required=True, help="ID do projeto GCP")
    argumentos.add_argument("--bucket", required=True, help="bucket do data lake, sem gs://")
    argumentos.add_argument("--dataset", default="stg", help="dataset de staging no BigQuery")
    argumentos.add_argument("--tabela", default="ocorrencias_sorocaba")
    argumentos.add_argument("--cod-ibge", default="3552205", help="código IBGE do município")
    args = argumentos.parse_args()

    origem = f"gs://{args.bucket}/preparada/ocorrencias/"
    destino_parquet = f"gs://{args.bucket}/preparada/{args.tabela}/"
    destino_bq = f"{args.projeto}.{args.dataset}.{args.tabela}"

    spark = (
        SparkSession.builder
        .appName("etl-ocorrencias-sorocaba")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )

    # ---------------------------------------------------------------- EXTRAÇÃO
    print(f"[extração] lendo {origem}")
    bruto = spark.read.option("mergeSchema", "true").parquet(origem)
    total_estado = bruto.count()
    print(f"[extração] {total_estado:,} registros lidos (estado de São Paulo, 2022-2026)")
    print(f"[extração] colunas encontradas: {len(bruto.columns)}")

    # ----------------------------------------------------------- TRANSFORMAÇÃO
    conformado = conciliar_esquema(bruto)

    textuais = [c for c in conformado.columns]
    conformado = limpar_sentinelas(conformado, textuais)

    # Filtro do município pelo código IBGE, e não pelo nome.
    # O levantamento do notebook 01 mostrou que o código publicado acompanha a
    # CIRCUNSCRIÇÃO, ou seja, o local onde o fato ocorreu — e não o município
    # onde o boletim foi registrado, que pode ser outro (registros feitos pela
    # Delegacia Eletrônica, por exemplo). Como o objetivo é medir a
    # criminalidade EM Sorocaba, a circunscrição é o critério correto.
    # Usar o código dispensa depender da grafia do nome.
    sorocaba = conformado.filter(F.col("cod_ibge_municipio") == args.cod_ibge)
    total_sorocaba = sorocaba.count()
    print(f"[transformação] {total_sorocaba:,} registros de Sorocaba "
          f"({total_sorocaba / total_estado:.2%} do estado)")

    # Tipagem: a zona preparada é toda textual por decisão de fidelidade à
    # origem; é aqui que cada campo assume o seu tipo.
    tipado = (
        sorocaba
        .withColumn("data_ocorrencia", F.to_date("data_ocorrencia", "yyyy-MM-dd"))
        .withColumn("data_registro", F.to_date("data_registro", "yyyy-MM-dd"))
        # A fonte não é consistente no formato da hora: em alguns arquivos ela
        # vem como '18:46:00', em outros com hora de um dígito ('9:30:00').
        # Extrair pela posição do caractere quebraria o segundo caso; separar
        # pelo ':' funciona nos dois. A hora cheia é gravada em coluna própria
        # para que a carga do DW não precise repetir esta regra em SQL — regra
        # repetida em dois lugares é regra que vai divergir.
        .withColumn("hora",
                    F.when(F.col("hora_ocorrencia").rlike(r"^\d{1,2}:\d{2}"),
                           F.split("hora_ocorrencia", ":").getItem(0).cast("int")))
        .withColumn("ano_bo", F.col("ano_bo").cast("int"))
        .withColumn("ano_estatistica", F.col("ano_estatistica").cast("int"))
        .withColumn("mes_estatistica", F.col("mes_estatistica").cast("int"))
        .withColumn("ano_arquivo", F.col("ano_arquivo").cast("int"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
    )

    # Coordenada zero é ausência de informação disfarçada de número: a fonte
    # usa 0 para registros sem geolocalização. Vira nulo, com marcação.
    tipado = (
        tipado
        .withColumn("latitude", F.when(F.col("latitude") == 0, None).otherwise(F.col("latitude")))
        .withColumn("longitude", F.when(F.col("longitude") == 0, None).otherwise(F.col("longitude")))
        .withColumn("tem_geolocalizacao",
                    F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    )

    tipado = derivar_tipo_local(tipado, conformado)
    tipado = derivar_periodo(tipado)

    # -------------------------------------------------------------------------
    # Padronização de descritores
    #
    # A fonte alterna caixa, acentuação e tipo de traço para designar a MESMA
    # coisa. Dois casos foram confirmados no levantamento e precisam de
    # tratamento, sob pena de a análise contar o mesmo fenômeno duas vezes:
    #
    #   natureza_apurada: 'TRÁFICO DE ENTORPECENTES' e 'TRAFICO DE
    #     ENTORPECENTES' são a mesma natureza; o mesmo vale para
    #     'LESÃO CORPORAL CULPOSA - OUTRAS' (hífen) e
    #     'LESÃO CORPORAL CULPOSA – OUTRAS' (travessão), entre outros. Sem
    #     padronizar, 28 valores distintos representariam apenas 23 naturezas.
    #
    #   bairro: 'VILA ZULMIRA' e 'Vila Zulmira' são o mesmo bairro.
    #
    # Em ambos os casos o valor original é preservado em coluna própria, para
    # que a análise de qualidade possa medir o tamanho do problema e para que
    # a transformação seja auditável.
    # -------------------------------------------------------------------------
    def padronizar(coluna):
        """Maiúsculas, sem acento, traços unificados e espaços colapsados."""
        sem_acento = F.translate(
            F.trim(coluna),
            "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ",
            "AAAAAEEEEIIIIOOOOOUUUUCNAAAAAEEEEIIIIOOOOOUUUUCN",
        )
        traco_unico = F.regexp_replace(sem_acento, r"[–—]", "-")
        return F.regexp_replace(F.upper(traco_unico), r"\s+", " ")

    def expandir_abreviacoes(coluna):
        """Escreve por extenso as abreviações de logradouro usadas no bairro.

        O campo bairro é de preenchimento livre, e o mesmo bairro aparece como
        'JARDIM SAO CARLOS', 'JD SAO CARLOS', 'JD. SAO CARLOS' e 'JD.SAO CARLOS'.
        Sem expandir, cada grafia vira um bairro diferente na dimensão e a
        contagem por bairro fica fragmentada.
        """
        texto = F.regexp_replace(coluna, r"[.,]", " ")
        for abreviacao, extenso in [
            (r"\bJD\b", "JARDIM"), (r"\bJDM\b", "JARDIM"), (r"\bJRD\b", "JARDIM"),
            (r"\bVL\b", "VILA"), (r"\bPQ\b", "PARQUE"), (r"\bPRQ\b", "PARQUE"),
            (r"\bCJ\b", "CONJUNTO"), (r"\bCH\b", "CHACARA"),
            (r"\bRES\b", "RESIDENCIAL"), (r"\bCOND\b", "CONDOMINIO"),
        ]:
            texto = F.regexp_replace(texto, abreviacao, extenso)
        return F.trim(F.regexp_replace(texto, r"\s+", " "))

    tipado = (
        tipado
        .withColumn("natureza_apurada_origem", F.col("natureza_apurada"))
        .withColumn(
            "natureza_apurada",
            F.when(F.col("natureza_apurada").isNull(), F.lit(None))
             .otherwise(padronizar(F.col("natureza_apurada"))),
        )
        .withColumn(
            "bairro_padronizado",
            F.when(F.col("bairro").isNull(), F.lit("NAO INFORMADO"))
             .otherwise(expandir_abreviacoes(padronizar(F.col("bairro")))),
        )
    )

    # Divergência entre o município do registro e o da circunscrição: mantida
    # como indicador, porque explica registros feitos fora de Sorocaba.
    tipado = tipado.withColumn(
        "registrado_em_outro_municipio",
        F.col("municipio_registro").isNotNull()
        & (F.upper(F.col("municipio_registro")) != F.lit("SOROCABA")),
    )

    # Duplicidade exata: mesma linha repetida em guias ou arquivos diferentes
    # (a fonte republica os arquivos periodicamente).
    antes = tipado.count()
    limpo = tipado.dropDuplicates([
        "num_bo", "ano_bo", "data_ocorrencia", "hora_ocorrencia",
        "rubrica", "natureza_apurada", "conduta", "bairro_padronizado",
    ])
    depois = limpo.count()
    print(f"[transformação] duplicidades removidas: {antes - depois:,} "
          f"({(antes - depois) / antes:.3%} dos registros de Sorocaba)")

    final = limpo.withColumn("dt_processamento", F.current_timestamp())

    print(f"[transformação] {final.count():,} registros conformados")
    final.printSchema()

    # -------------------------------------------------------------------- CARGA
    print(f"[carga] gravando cópia em {destino_parquet}")
    final.write.mode("overwrite").parquet(destino_parquet)

    print(f"[carga] gravando {destino_bq}")
    (
        final.write.format("bigquery")
        .option("table", destino_bq)
        .option("temporaryGcsBucket", f"{args.bucket}/temp-dataproc")
        .option("writeMethod", "indirect")
        .mode("overwrite")
        .save()
    )

    print("[carga] concluída")
    spark.stop()


if __name__ == "__main__":
    main()
