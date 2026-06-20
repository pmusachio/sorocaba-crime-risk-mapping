"""
Coleta (parte 2) — conversão dos .xlsx brutos para Parquet.

Por que esta etapa existe:
  - O Spark NÃO lê .xlsx nativamente (precisaria da lib externa spark-excel).
  - Ler um .xlsx de ~190 MB com pandas no driver do Databricks Community Edition
    é arriscado (estoura a memória do driver / OOM).
  - Parquet é colunar, comprimido e lido nativa e distribuidamente pelo Spark.

Esta etapa roda UMA vez, localmente (na máquina que tem os .xlsx), e produz a
zona de aterrissagem (landing) em Parquet. É esse diretório `data/parquet/` que
deve ser enviado ao Volume/DBFS do Databricks — não os .xlsx.

Decisões de fidelidade (camada de aterrissagem = fiel à origem):
  - Tudo é gravado como STRING (a tipagem acontece na Silver, no Spark).
  - Sentinelas da fonte ('NULL', '(Vazio)', '-', '0' em lat/long) são PRESERVADAS
    aqui de propósito, para que a análise de qualidade de dados (notebook 02)
    consiga evidenciá-las e a Silver as trate de forma documentada.
  - Datas (datetime do Excel) -> 'yyyy-MM-dd'; horas -> 'HH:MM:SS'. Foi confirmado
    na inspeção do dado real que as datas vêm como datetime nativo do Excel, e
    NÃO como texto 'M/D/YY' — daí a necessidade de renderização determinística.
  - Os nomes ORIGINAIS de coluna de cada ano são mantidos (ex.: CIDADE em 2022 vs
    NOME_MUNICIPIO em 2023+). A reconciliação para nomes canônicos é feita na
    Silver, via coalesce — não aqui.

Cada guia vira um arquivo Parquet em `data/parquet/`. No Spark, basta ler o
diretório com mergeSchema=true para unir todos os anos (colunas ausentes em
anos antigos, como DESCR_TIPOLOCAL, viram nulo automaticamente).

Uso:
    python3 scripts/converter_para_parquet.py

Dependências: openpyxl, pyarrow.
"""
from __future__ import annotations

import datetime as dt
import os

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq

RAIZ_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(RAIZ_REPO, "data")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")

LOTE = 100_000  # linhas por bloco gravado (mantém memória limitada)

# Guias de dados por ano (a aba "CAMPOS_DA_TABELA_SPDADOS" é dicionário, ignorada)
CONFIG_ANOS = {
    2022: ("SPDadosCriminais_2022.xlsx", ["JAN-JUN_2022", "JUL-DEZ_2022"]),
    2023: ("SPDadosCriminais_2023.xlsx", ["JAN-JUN_2023", "JUL-DEZ_2023"]),
    2024: ("SPDadosCriminais_2024.xlsx", ["JAN-JUN_2024", "JUL-DEZ_2024"]),
    2025: ("SPDadosCriminais_2025.xlsx", ["JAN-JUN_2025", "JUL-DEZ_2025"]),
    2026: ("SPDadosCriminais_2026.xlsx", ["JAN-ABR_2026"]),
}


def render_valor(v) -> str | None:
    """Converte um valor de célula em string determinística, preservando a origem.
    None vira nulo real; datas/horas recebem formato canônico; o resto vira str().
    """
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d")          # DATA_* vêm como datetime à meia-noite
    if isinstance(v, dt.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.time):
        return v.strftime("%H:%M:%S")          # HORA_OCORRENCIA_BO
    return str(v)                              # int/float/str: faithful (não faz strip)


def converter_guia(arquivo: str, guia: str, ano: int) -> str:
    caminho = os.path.join(DATA_DIR, arquivo)
    wb = openpyxl.load_workbook(caminho, read_only=True, data_only=True)
    ws = wb[guia]
    rows = ws.iter_rows(values_only=True)

    header = [str(h).strip() for h in next(rows)]
    audit_cols = ["_arquivo_origem", "_guia_origem", "_ano_arquivo", "_dt_ingestao"]
    todas_cols = header + audit_cols
    schema = pa.schema([(c, pa.string()) for c in todas_cols])

    os.makedirs(PARQUET_DIR, exist_ok=True)
    saida = os.path.join(PARQUET_DIR, f"{os.path.splitext(arquivo)[0]}__{guia}.parquet")
    writer = pq.ParquetWriter(saida, schema, compression="snappy")
    ts_ingestao = dt.datetime.now().isoformat(timespec="seconds")

    buffer: list[list] = []
    total = 0

    def flush():
        nonlocal buffer, total
        if not buffer:
            return
        n = len(buffer)
        colunas = []
        for j in range(len(header)):
            colunas.append([buffer[i][j] for i in range(n)])
        colunas.append([arquivo] * n)
        colunas.append([guia] * n)
        colunas.append([str(ano)] * n)
        colunas.append([ts_ingestao] * n)
        tabela = pa.table({c: pa.array(colunas[k], type=pa.string())
                           for k, c in enumerate(todas_cols)})
        writer.write_table(tabela)
        total += n
        buffer = []

    for r in rows:
        buffer.append([render_valor(v) for v in r])
        if len(buffer) >= LOTE:
            flush()
            print(f"    ... {total:,} linhas")
    flush()
    writer.close()
    wb.close()
    print(f"  [{guia}] {total:,} linhas -> {os.path.basename(saida)}")
    return saida


def main() -> None:
    os.makedirs(PARQUET_DIR, exist_ok=True)
    print(f"Origem .xlsx : {DATA_DIR}")
    print(f"Destino Parquet: {PARQUET_DIR}\n")
    for ano, (arquivo, guias) in CONFIG_ANOS.items():
        caminho = os.path.join(DATA_DIR, arquivo)
        if not os.path.exists(caminho):
            print(f"[{ano}] {arquivo} não encontrado — pulando (rode coletar_dados.py antes)")
            continue
        print(f"[{ano}] {arquivo}")
        for guia in guias:
            converter_guia(arquivo, guia, ano)
    print("\nConversão concluída. Envie o diretório data/parquet/ para o Volume/DBFS do Databricks.")


if __name__ == "__main__":
    main()
