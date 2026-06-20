"""
Coleta — download dos arquivos brutos de ocorrências criminais da SSP-SP.

Etapa 1 do pipeline (Coleta). Baixa os arquivos anuais .xlsx diretamente da
fonte oficial e os grava localmente, prontos para a conversão para Parquet
(ver `converter_para_parquet.py`).

Fonte ......: SSP-SP / Portal de Dados Abertos do Estado de São Paulo
              dataset "Dados Criminais" (Números Sem Mistério)
Licença ....: Creative Commons Attribution 4.0 (CC-BY 4.0)
Ética ......: arquivos públicos, disponibilizados oficialmente para download
              direto pela própria Secretaria; nenhum scraping de página, apenas
              GET nos arquivos publicados. User-Agent identificado.

Uso:
    python3 scripts/coletar_dados.py            # baixa 2022..2026 para ./data
    python3 scripts/coletar_dados.py 2025 2026  # baixa apenas anos informados

Sem dependências externas (usa apenas a biblioteca padrão).
"""
import os
import sys
import urllib.request

URL_BASE = "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados"
ARQUIVO_FMT = "SPDadosCriminais_{ano}.xlsx"
ANOS_PADRAO = [2022, 2023, 2024, 2025, 2026]

# Resolve ./data relativo à raiz do repositório, independente de onde o script é chamado
RAIZ_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESTINO_DIR = os.path.join(RAIZ_REPO, "data")


def baixar(ano: int) -> None:
    arquivo = ARQUIVO_FMT.format(ano=ano)
    url = f"{URL_BASE}/{arquivo}"
    destino = os.path.join(DESTINO_DIR, arquivo)

    if os.path.exists(destino):
        tamanho_mb = os.path.getsize(destino) / (1024 * 1024)
        print(f"[{ano}] já existe ({tamanho_mb:.0f} MB) — pulando: {destino}")
        return

    print(f"[{ano}] baixando {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "sorocaba-crime-risk/1.0 (TCC eng. dados)"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(destino, "wb") as out:
        baixados = 0
        while True:
            bloco = resp.read(1024 * 1024)
            if not bloco:
                break
            out.write(bloco)
            baixados += len(bloco)
    print(f"[{ano}] OK — {baixados / (1024 * 1024):.0f} MB gravados em {destino}")


def main() -> None:
    os.makedirs(DESTINO_DIR, exist_ok=True)
    anos = [int(a) for a in sys.argv[1:]] or ANOS_PADRAO
    print(f"Destino: {DESTINO_DIR}")
    print(f"Anos: {anos}\n")
    for ano in anos:
        try:
            baixar(ano)
        except Exception as e:  # noqa: BLE001 - log e segue para os demais anos
            print(f"[{ano}] FALHA: {e}")
    print("\nColeta concluída. Próximo passo: python3 scripts/converter_para_parquet.py")


if __name__ == "__main__":
    main()
