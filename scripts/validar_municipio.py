# Script de validação — grafias do campo "município" por ano
# Objetivo: confirmar como "Sorocaba" aparece em cada arquivo antes de aplicar o filtro
# Roda rápido: lê só a coluna de município de cada guia, não o arquivo inteiro

import pandas as pd
import unicodedata

# Ajuste o caminho conforme onde você subiu os arquivos no Colab
caminho = ""  # ex: "/content/drive/MyDrive/sua_pasta/"

anos_arquivos = {
    2022: ("SPDadosCriminais_2022.xlsx", "CIDADE", ["JAN-JUN_2022", "JUL-DEZ_2022"]),
    2023: ("SPDadosCriminais_2023.xlsx", "NOME_MUNICIPIO", ["JAN-JUN_2023", "JUL-DEZ_2023"]),
    2024: ("SPDadosCriminais_2024.xlsx", "NOME_MUNICIPIO", ["JAN-JUN_2024", "JUL-DEZ_2024"]),
    2025: ("SPDadosCriminais_2025.xlsx", "NOME_MUNICIPIO", ["JAN-JUN_2025", "JUL-DEZ_2025"]),
    2026: ("SPDadosCriminais_2026.xlsx", "NOME_MUNICIPIO", ["JAN-ABR_2026"]),
}

def normaliza(texto):
    """Remove acentos e converte para maiúsculo, para comparação robusta."""
    if pd.isna(texto):
        return texto
    nfkd = unicodedata.normalize("NFKD", str(texto))
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sem_acento.strip().upper()

for ano, (arquivo, col_municipio, guias) in anos_arquivos.items():
    print(f"\n=== {ano} (coluna: {col_municipio}) ===")
    for guia in guias:
        df = pd.read_excel(caminho + arquivo, sheet_name=guia, usecols=[col_municipio])
        valores_unicos = df[col_municipio].dropna().unique()

        # Filtra só valores que, normalizados, contêm "SOROCABA"
        candidatos = [v for v in valores_unicos if "SOROCABA" in normaliza(v)]

        print(f"  [{guia}] {len(valores_unicos)} valores únicos de município no total")
        if candidatos:
            print(f"    Grafias de Sorocaba encontradas: {candidatos}")
        else:
            print(f"    Nenhuma grafia de Sorocaba encontrada nesta guia")
