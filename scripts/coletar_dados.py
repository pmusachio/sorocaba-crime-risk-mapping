"""Baixa os quatro XLSX originais da SSP-SP e calcula seus checksums.

Este utilitário é executado localmente. Depois da coleta, faça upload manual dos
arquivos em ``data/raw`` para o diretório informado pelo notebook
``00_coleta_bronze.py``. Os arquivos e o manifesto local não devem ser
versionados no Git.

Uso:
    python3 scripts/coletar_dados.py
    python3 scripts/coletar_dados.py 2022 2024
    python3 scripts/coletar_dados.py --force

O script usa apenas GET; não consulta HEAD, Content-Length nem mantém estado de
carga incremental. Consulte e registre no README os termos de uso publicados
pela SSP-SP na data da coleta.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import os
import sys
import urllib.request
import zipfile
from pathlib import Path

URL_TEMPLATE = (
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/"
    "spDados/SPDadosCriminais_{ano}.xlsx"
)
ARQUIVO_TEMPLATE = "SPDadosCriminais_{ano}.xlsx"
ANOS_PERMITIDOS = (2022, 2023, 2024, 2025)
TAMANHO_BLOCO = 1024 * 1024
ASSINATURA_ZIP = b"PK\x03\x04"

RAIZ_REPOSITORIO = Path(__file__).resolve().parent.parent
DIRETORIO_DESTINO = RAIZ_REPOSITORIO / "data" / "raw"
CAMINHO_MANIFESTO = DIRETORIO_DESTINO / "manifesto_sha256.csv"


def sha256_arquivo(caminho: Path) -> str:
    digest = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(TAMANHO_BLOCO), b""):
            digest.update(bloco)
    return digest.hexdigest()


def validar_xlsx(caminho: Path) -> None:
    """Rejeita respostas HTML/arquivos vazios antes do cálculo do manifesto."""
    if not caminho.is_file() or caminho.stat().st_size == 0:
        raise ValueError(f"arquivo ausente ou vazio: {caminho}")
    with caminho.open("rb") as arquivo:
        assinatura = arquivo.read(len(ASSINATURA_ZIP))
    if assinatura != ASSINATURA_ZIP:
        raise ValueError(f"conteúdo recebido não é um XLSX válido: {caminho}")
    try:
        with zipfile.ZipFile(caminho) as pacote:
            nomes = set(pacote.namelist())
    except zipfile.BadZipFile as exc:
        raise ValueError(f"XLSX incompleto ou corrompido: {caminho}") from exc
    obrigatorios = {"[Content_Types].xml", "xl/workbook.xml"}
    if not obrigatorios.issubset(nomes):
        raise ValueError(f"estrutura XLSX incompleta: {caminho}")


def baixar_arquivo(ano: int, sobrescrever: bool) -> dict[str, object]:
    nome = ARQUIVO_TEMPLATE.format(ano=ano)
    url = URL_TEMPLATE.format(ano=ano)
    destino = DIRETORIO_DESTINO / nome

    if destino.exists() and not sobrescrever:
        status = "EXISTENTE_VALIDADO"
        print(f"[{ano}] arquivo existente; calculando SHA-256: {destino}")
    else:
        temporario = destino.with_suffix(destino.suffix + ".part")
        print(f"[{ano}] baixando {url}")
        requisicao = urllib.request.Request(
            url,
            headers={"User-Agent": "mvp-engenharia-dados-sorocaba/1.0"},
        )
        try:
            with urllib.request.urlopen(requisicao, timeout=180) as resposta:
                with temporario.open("wb") as saida:
                    while True:
                        bloco = resposta.read(TAMANHO_BLOCO)
                        if not bloco:
                            break
                        saida.write(bloco)
            os.replace(temporario, destino)
        finally:
            if temporario.exists():
                temporario.unlink()
        status = "BAIXADO"

    validar_xlsx(destino)
    tamanho = destino.stat().st_size
    checksum = sha256_arquivo(destino)
    print(f"[{ano}] {status}: {tamanho:,} bytes; sha256={checksum}")
    return {
        "ano": ano,
        "arquivo": nome,
        "url": url,
        "caminho_local": str(destino),
        "tamanho_bytes": tamanho,
        "sha256": checksum,
        "dt_coleta_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "status": status,
    }


def gravar_manifesto(registros: list[dict[str, object]]) -> None:
    campos = [
        "ano",
        "arquivo",
        "url",
        "caminho_local",
        "tamanho_bytes",
        "sha256",
        "dt_coleta_utc",
        "status",
    ]
    with CAMINHO_MANIFESTO.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(registros)


def ler_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baixa os XLSX da SSP-SP usados no MVP (2022–2025)."
    )
    parser.add_argument(
        "anos",
        nargs="*",
        default=None,
        type=int,
        metavar="ANO",
        help="Anos desejados (2022–2025); o padrão baixa os quatro anos.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Baixa novamente arquivos locais já existentes.",
    )
    argumentos = parser.parse_args()
    invalidos = sorted(set(argumentos.anos or ()) - set(ANOS_PERMITIDOS))
    if invalidos:
        parser.error(
            "ano(s) fora do escopo: " + ", ".join(str(ano) for ano in invalidos)
        )
    return argumentos


def main() -> int:
    argumentos = ler_argumentos()
    anos = argumentos.anos or list(ANOS_PERMITIDOS)
    DIRETORIO_DESTINO.mkdir(parents=True, exist_ok=True)

    print(f"Destino local: {DIRETORIO_DESTINO}")
    print(f"Anos: {', '.join(str(ano) for ano in anos)}")

    registros = []
    falhas = []
    for ano in anos:
        try:
            registros.append(baixar_arquivo(ano, argumentos.force))
        except Exception as exc:  # mantém o diagnóstico dos demais anos
            falhas.append((ano, str(exc)))
            print(f"[{ano}] FALHA: {exc}", file=sys.stderr)

    if registros:
        gravar_manifesto(registros)
        print(f"Manifesto local: {CAMINHO_MANIFESTO}")
        print(
            "Próximo passo: envie os XLSX para "
            "/Volumes/workspace/sorocaba_seguranca/dados/xlsx/ e execute "
            "notebooks/00_coleta_bronze.py no Databricks."
        )

    return 1 if falhas else 0


if __name__ == "__main__":
    raise SystemExit(main())
