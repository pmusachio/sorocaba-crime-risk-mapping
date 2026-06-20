# Databricks notebook source
# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Notebook 00 — Coleta Incremental + Bronze
#
# Responsabilidades:
#   1. Verificar se os arquivos da SSP-SP foram atualizados (HTTP Content-Length)
#   2. Baixar xlsx para /tmp do driver (anos novos ou com arquivo alterado)
#   3. Converter xlsx → pandas batches → Spark DataFrame (sem arquivo intermediário)
#   4. Escrever/atualizar a camada Bronze em Delta Lake (incremental por _ano_arquivo)
#
# Ponto de entrada do pipeline semanal. O Job gerencia a sequência 00→01→02→03.
# Sem dbutils.notebook.run() — evita dupla execução do notebook 01.
# =============================================================================

# COMMAND ----------
# MAGIC %pip install openpyxl

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuração

# COMMAND ----------

import datetime as dt
import os
import urllib.request

import openpyxl
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA  = "sorocaba_seguranca"

# xlsx vai para /tmp (disponível no driver serverless, os.makedirs funciona).
# Sem Parquet intermediário: xlsx → pandas → spark.createDataFrame() → Delta staging.
# O Spark em serverless não acessa /tmp — todo I/O Spark usa Delta no catálogo.
LOCAL_XLSX = "/tmp/sorocaba_xlsx"

URL_TEMPLATE = (
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/"
    "spDados/SPDadosCriminais_{ano}.xlsx"
)

# Guias de dados por ano (aba de dicionário é ignorada)
CONFIG_ANOS = {
    2022: ("SPDadosCriminais_2022.xlsx", ["JAN-JUN_2022", "JUL-DEZ_2022"]),
    2023: ("SPDadosCriminais_2023.xlsx", ["JAN-JUN_2023", "JUL-DEZ_2023"]),
    2024: ("SPDadosCriminais_2024.xlsx", ["JAN-JUN_2024", "JUL-DEZ_2024"]),
    2025: ("SPDadosCriminais_2025.xlsx", ["JAN-JUN_2025", "JUL-DEZ_2025"]),
    2026: ("SPDadosCriminais_2026.xlsx", ["JAN-ABR_2026"]),
}

ANO_CORRENTE = max(CONFIG_ANOS.keys())
LOTE         = 50_000   # linhas por pandas batch (mantém pico de RAM baixo)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

os.makedirs(LOCAL_XLSX, exist_ok=True)

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Xlsx tmp: {LOCAL_XLSX}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Tabela de controle
# MAGIC
# MAGIC Registra o Content-Length de cada download bem-sucedido.
# MAGIC Na próxima execução, se o tamanho não mudou, o ano é pulado.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS _controle_coleta (
        ano              INT,
        arquivo          STRING,
        url              STRING,
        content_length   BIGINT,
        dt_download      TIMESTAMP,
        n_linhas_bronze  BIGINT,
        status           STRING
    )
    USING DELTA
""")


def controle_anterior(ano: int):
    rows = spark.sql(f"""
        SELECT content_length FROM _controle_coleta
        WHERE ano = {ano} AND status = 'ok'
        ORDER BY dt_download DESC LIMIT 1
    """).collect()
    return rows[0]["content_length"] if rows else None


def registrar_controle(ano, arquivo, url, content_length, n_linhas, status):
    spark.sql(f"""
        INSERT INTO _controle_coleta VALUES (
            {ano}, '{arquivo}', '{url}', {content_length},
            current_timestamp(), {n_linhas}, '{status}'
        )
    """)


spark.sql("SELECT * FROM _controle_coleta ORDER BY ano DESC, dt_download DESC").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Verificação e download incremental

# COMMAND ----------


def content_length_remoto(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "TCC-SSP/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return int(r.headers.get("Content-Length", 0))
    except Exception as e:
        print(f"  HEAD falhou ({e}) — forcando download")
        return 0


def render_valor(v):
    """Converte celula Excel para string deterministica (datas: yyyy-MM-dd)."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.time):
        return v.strftime("%H:%M:%S")
    return str(v)


def processar_guia_staging(caminho_xlsx: str, guia: str, ano: int, staging: str) -> int:
    """
    Le uma guia xlsx em lotes via openpyxl streaming, converte para pandas e
    escreve em Delta staging. Sem acesso ao filesystem pelo Spark.
    Retorna total de linhas processadas.
    """
    ts       = dt.datetime.now().isoformat(timespec="seconds")
    nome_arq = os.path.basename(caminho_xlsx)

    wb = openpyxl.load_workbook(caminho_xlsx, read_only=True, data_only=True)
    ws = wb[guia]
    rows_iter = ws.iter_rows(values_only=True)

    header  = [str(h).strip() if h is not None else f"_col{i}"
               for i, h in enumerate(next(rows_iter))]
    audit   = ["_arquivo_origem", "_guia_origem", "_ano_arquivo", "_dt_ingestao"]
    colunas = header + audit

    buf   = []
    total = 0

    def flush():
        nonlocal total
        if not buf:
            return
        dados = [
            [render_valor(v) for v in row] + [nome_arq, guia, str(ano), ts]
            for row in buf
        ]
        pdf = pd.DataFrame(dados, columns=colunas)
        sdf = spark.createDataFrame(pdf)
        (sdf.write.format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(staging))
        total += len(buf)
        buf.clear()
        print(f"      ... {total:,} linhas -> {staging}")

    for row in rows_iter:
        buf.append(row)
        if len(buf) >= LOTE:
            flush()
    flush()
    wb.close()
    print(f"    [{guia}] {total:,} linhas processadas")
    return total


# --- Loop principal ---
anos_processados = []

for ano, (arquivo, guias) in CONFIG_ANOS.items():
    url         = URL_TEMPLATE.format(ano=ano)
    cl_novo     = content_length_remoto(url)
    cl_anterior = controle_anterior(ano)

    precisa = (cl_novo != cl_anterior) or (ano == ANO_CORRENTE)
    if not precisa:
        print(f"[{ano}] sem alteracao ({cl_novo/1e6:.1f} MB) — pulado")
        continue

    if cl_anterior is None:
        motivo = "novo"
    elif cl_novo == cl_anterior:
        motivo = "ano corrente"
    else:
        motivo = f"tamanho: {cl_anterior/1e6:.1f}->{cl_novo/1e6:.1f} MB"

    print(f"\n[{ano}] Processando ({motivo}) ...")

    # Download xlsx para /tmp
    caminho_xlsx = os.path.join(LOCAL_XLSX, arquivo)
    print(f"  Baixando {url} ...")
    urllib.request.urlretrieve(url, caminho_xlsx)
    print(f"  Download OK ({os.path.getsize(caminho_xlsx)/1e6:.1f} MB)")

    # Staging Delta temporaria para este ano
    staging = f"_bronze_staging_{ano}"
    spark.sql(f"DROP TABLE IF EXISTS {staging}")

    total_linhas = 0
    print(f"  Convertendo {len(guias)} guia(s) para {staging} ...")
    for guia in guias:
        total_linhas += processar_guia_staging(caminho_xlsx, guia, ano, staging)

    # Grava/atualiza particao _ano_arquivo na Bronze
    df_staging = spark.table(staging)
    ano_na_bronze = (
        spark.catalog.tableExists("bronze")
        and spark.sql(f"SELECT 1 FROM bronze WHERE _ano_arquivo='{ano}' LIMIT 1").count() > 0
    )

    if ano_na_bronze:
        print(f"  Atualizando _ano_arquivo='{ano}' na Bronze (replaceWhere)...")
        (df_staging.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"_ano_arquivo = '{ano}'")
         .option("mergeSchema", "true")
         .saveAsTable("bronze"))
    elif spark.catalog.tableExists("bronze"):
        print(f"  Inserindo ano {ano} na Bronze (append)...")
        (df_staging.write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .saveAsTable("bronze"))
    else:
        print(f"  Criando tabela Bronze (ano {ano})...")
        (df_staging.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("bronze"))

    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    registrar_controle(ano, arquivo, url, cl_novo, total_linhas, "ok")
    anos_processados.append(ano)
    print(f"  [{ano}] Bronze atualizado: {total_linhas:,} linhas")

print("\nAnos processados:", anos_processados or "nenhum (sem alteracoes)")
print(f"Bronze total    : {spark.table('bronze').count():,} linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Sumario de controle

# COMMAND ----------

# O Job gerencia a sequencia 00 -> 01 -> 02 -> 03.
print("Historico de downloads:")
spark.sql("""
    SELECT ano, arquivo, round(content_length/1e6,1) AS mb,
           dt_download, n_linhas_bronze, status
    FROM _controle_coleta
    ORDER BY ano DESC, dt_download DESC
""").show(truncate=False)

dbutils.notebook.exit(f"ok: {anos_processados}")
