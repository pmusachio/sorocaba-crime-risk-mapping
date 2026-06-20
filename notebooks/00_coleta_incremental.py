# Databricks notebook source
# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Notebook 00 — Coleta Incremental + Bronze
#
# Responsabilidades:
#   1. Verificar se os arquivos da SSP-SP foram atualizados (HTTP Content-Length)
#   2. Baixar para DBFS apenas os anos novos ou com arquivo alterado
#      (ano corrente é sempre reverificado — pode ter novos meses)
#   3. Converter xlsx → Parquet em DBFS via openpyxl streaming (sem OOM)
#   4. Escrever/atualizar a camada Bronze em Delta Lake (incremental por _ano_arquivo)
#   5. Encadear o notebook 01 (Silver + Gold) quando houver dados novos
#
# Ponto de entrada do pipeline semanal orquestrado pelo GitHub Actions.
# Pode também ser rodado manualmente para inicialização ou reprocessamento.
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
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

SCHEMA = "sorocaba_seguranca"

# Caminhos no DBFS (dbfs:/... para Spark; /dbfs/... para I/O Python no driver)
DBFS_BASE        = "dbfs:/FileStore/sorocaba_seguranca"
DBFS_XLSX        = f"{DBFS_BASE}/xlsx"
DBFS_PARQUET     = f"{DBFS_BASE}/parquet"
LOCAL_XLSX       = "/dbfs/FileStore/sorocaba_seguranca/xlsx"
LOCAL_PARQUET    = "/dbfs/FileStore/sorocaba_seguranca/parquet"

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

ANO_CORRENTE = max(CONFIG_ANOS.keys())  # sempre reverifica: pode ter meses novos
LOTE         = 100_000                  # linhas por bloco de escrita (memória limitada)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")

dbutils.fs.mkdirs(DBFS_XLSX)
dbutils.fs.mkdirs(DBFS_PARQUET)
os.makedirs(LOCAL_XLSX,   exist_ok=True)
os.makedirs(LOCAL_PARQUET, exist_ok=True)

print(f"Schema  : {SCHEMA}")
print(f"DBFS xlsx   : {DBFS_XLSX}")
print(f"DBFS parquet: {DBFS_PARQUET}")

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
        print(f"  HEAD falhou ({e}) — forçando download")
        return 0


def render_valor(v):
    """Converte célula Excel → string determinística (datas: yyyy-MM-dd, horas: HH:MM:SS)."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.time):
        return v.strftime("%H:%M:%S")
    return str(v)


def converter_guia_parquet(caminho_xlsx: str, guia: str, ano: int, dir_saida: str) -> int:
    """Converte uma guia de xlsx para Parquet no DBFS; retorna total de linhas."""
    wb = openpyxl.load_workbook(caminho_xlsx, read_only=True, data_only=True)
    ws = wb[guia]
    rows_iter = ws.iter_rows(values_only=True)

    header = [str(h).strip() for h in next(rows_iter)]
    audit  = ["_arquivo_origem", "_guia_origem", "_ano_arquivo", "_dt_ingestao"]
    todas  = header + audit
    schema = pa.schema([(c, pa.string()) for c in todas])

    nome_arq = os.path.basename(caminho_xlsx)
    ts       = dt.datetime.now().isoformat(timespec="seconds")
    saida    = os.path.join(dir_saida, f"{os.path.splitext(nome_arq)[0]}__{guia}.parquet")

    writer = pq.ParquetWriter(saida, schema, compression="snappy")
    buf, total = [], 0

    def flush():
        nonlocal buf, total
        if not buf:
            return
        n = len(buf)
        cols = [[buf[i][j] for i in range(n)] for j in range(len(header))]
        cols += [[nome_arq]*n, [guia]*n, [str(ano)]*n, [ts]*n]
        tbl = pa.table({c: pa.array(cols[k], type=pa.string()) for k, c in enumerate(todas)})
        writer.write_table(tbl)
        total += n
        buf.clear()

    for r in rows_iter:
        buf.append([render_valor(v) for v in r])
        if len(buf) >= LOTE:
            flush()
            print(f"      ... {total:,} linhas gravadas")
    flush()
    writer.close()
    wb.close()
    print(f"    [{guia}] {total:,} linhas → {os.path.basename(saida)}")
    return total


# --- Loop de processamento ---
anos_processados = []

for ano, (arquivo, guias) in CONFIG_ANOS.items():
    url = URL_TEMPLATE.format(ano=ano)
    cl_novo     = content_length_remoto(url)
    cl_anterior = controle_anterior(ano)

    # Força reprocessamento do ano corrente (pode ter novos meses mesmo sem mudança de tamanho)
    precisa = (cl_novo != cl_anterior) or (ano == ANO_CORRENTE)

    if not precisa:
        print(f"[{ano}] sem alteração ({cl_novo/1e6:.1f} MB) — pulado")
        continue

    motivo = "novo" if cl_anterior is None else ("ano corrente" if cl_novo == cl_anterior else f"tamanho: {cl_anterior/1e6:.1f}→{cl_novo/1e6:.1f} MB")
    print(f"
[{ano}] Processando ({motivo}) ...")

    # Download
    caminho_xlsx = os.path.join(LOCAL_XLSX, arquivo)
    print(f"  Baixando {url} ...")
    urllib.request.urlretrieve(url, caminho_xlsx)
    print(f"  Download OK ({os.path.getsize(caminho_xlsx)/1e6:.1f} MB)")

    # Conversão xlsx → Parquet por guia
    dir_parquet = os.path.join(LOCAL_PARQUET, str(ano))
    os.makedirs(dir_parquet, exist_ok=True)
    print(f"  Convertendo {len(guias)} guia(s)...")
    for guia in guias:
        converter_guia_parquet(caminho_xlsx, guia, ano, dir_parquet)

    # Lê Parquet e grava/atualiza partição _ano_arquivo na Bronze
    df_new = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(f"{DBFS_PARQUET}/{ano}")
    )

    bronze_existe = (
        spark.catalog.tableExists("bronze")
        and spark.sql(f"SELECT 1 FROM bronze WHERE _ano_arquivo='{ano}' LIMIT 1").count() > 0
    )

    if bronze_existe:
        print(f"  Atualizando partição _ano_arquivo='{ano}' na Bronze (replaceWhere)...")
        (df_new.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"_ano_arquivo = '{ano}'")
         .option("mergeSchema", "true")
         .saveAsTable("bronze"))
    elif spark.catalog.tableExists("bronze"):
        print(f"  Inserindo ano {ano} novo na Bronze (append)...")
        (df_new.write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .saveAsTable("bronze"))
    else:
        print(f"  Criando tabela Bronze com ano {ano}...")
        (df_new.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable("bronze"))

    n_linhas = df_new.count()
    registrar_controle(ano, arquivo, url, cl_novo, n_linhas, "ok")
    anos_processados.append(ano)
    print(f"  [{ano}] Bronze atualizado: {n_linhas:,} linhas")

print(f"
Anos processados: {anos_processados or 'nenhum (sem alterações)'}")
print(f"Bronze total    : {spark.table('bronze').count():,} linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Encadeamento — Silver + Gold

# COMMAND ----------

if anos_processados:
    print("Encadeando notebook 01 (Silver + Gold)...")
    result = dbutils.notebook.run(
        "01_pipeline_bronze_silver_gold",   # relativo à pasta do notebook
        timeout_seconds=7200,
        arguments={"anos_atualizados": ",".join(str(a) for a in anos_processados)},
    )
    print(f"Notebook 01 concluído: {result}")
else:
    print("Sem dados novos — Silver/Gold não reprocessados.")
    print("Execute o notebook 01 manualmente se quiser forçar a atualização.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Sumário de controle

# COMMAND ----------

print("Histórico de downloads:")
spark.sql("""
    SELECT ano, arquivo, round(content_length/1e6,1) AS mb,
           dt_download, n_linhas_bronze, status
    FROM _controle_coleta
    ORDER BY ano DESC, dt_download DESC
""").show(truncate=False)
