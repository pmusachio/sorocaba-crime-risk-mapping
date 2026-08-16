# 2. Coleta

Execução: [`notebooks/01_coleta_ingestao.ipynb`](../notebooks/01_coleta_ingestao.ipynb)

## Fontes escolhidas

| Fonte | O que fornece | Formato | Endereço |
|---|---|---|---|
| **SSP-SP — SPDados** | ocorrências criminais registradas no estado de São Paulo, um arquivo por ano | `.xlsx`, ~200 MB por ano | `ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ano}.xlsx` |
| **IBGE — API de Localidades** | código, nome e hierarquia regional do município | JSON | `servicodados.ibge.gov.br/api/v1/localidades/municipios/3552205` |
| **IBGE — SIDRA, tabela 6579** | estimativas anuais de população | JSON | `apisidra.ibge.gov.br/values/t/6579/n6/3552205/p/all` |
| **IBGE — SIDRA, tabela 4709** | população do Censo 2022 | JSON | `apisidra.ibge.gov.br/values/t/4709/n6/3552205/v/93/p/all` |

**Data da coleta:** agosto de 2026. A SSP-SP republica os arquivos periodicamente (os de 2022 a 2025 foram atualizados pela última vez em 30/06/2026, e o de 2026 em 30/07/2026), de modo que uma nova coleta pode devolver números ligeiramente diferentes dos apresentados neste trabalho. Por isso a data de coleta é registrada aqui e em cada linha carregada, na coluna de auditoria `dt_ingestao`.

### Por que duas fontes

Os dados da SSP-SP respondem *o que* aconteceu, mas não permitem comparar anos: o número absoluto de ocorrências cresce junto com a população. Os dados do IBGE entram como **dados de referência**, no sentido da Aula 2 de Governança:

> "Dados de referência são utilizados para classificar ou categorizar outros dados... A utilização de referências externas é interessante e deve ser adotada quando for possível, pois permite a comparação e a utilização de dados de várias fontes."

Eles cumprem dois papéis: o código IBGE valida o município (nenhum código fora da lista oficial passa despercebido) e a população converte contagens em **taxa por 100 mil habitantes**, que é o que torna 2022 e 2025 comparáveis.

## Licença e ética

Os dados da SSP-SP são **dados abertos**, publicados oficialmente pela Secretaria para download direto no portal de transparência do Governo do Estado de São Paulo. Os dados do IBGE também são abertos e de uso livre com citação da fonte.

Sobre a forma de coleta, duas decisões:

- **Não há web scraping.** A coleta faz requisições HTTP diretas aos arquivos que a própria Secretaria publica para download, com *User-Agent* identificando o trabalho acadêmico. Nenhuma página é raspada, nenhum contorno de proteção é feito, e a frequência é de uma requisição por arquivo.
- **Os arquivos são grandes.** Cada requisição baixa cerca de 200 MB. A coleta é executada uma única vez e o resultado fica persistido no data lake; reexecuções pulam o que já foi baixado.

## Onde os dados foram armazenados

O destino é um bucket no Cloud Storage, que implementa o **data lake** — na definição da Aula 3, "um repositório distribuído que reúne dados em qualquer escala" no qual "os dados são armazenados em seu formato nativo, ou seja, exatamente como foram gerados".

```
gs://<projeto>-datalake/
├── bruta/                          formato nativo, exatamente como publicado
│   ├── ssp-sp/
│   │   ├── SPDadosCriminais_2022.xlsx
│   │   ├── ...
│   │   └── SPDadosCriminais_2026.xlsx
│   └── ibge/
│       ├── municipio_3552205.json
│       ├── populacao_estimativas_t6579.json
│       └── populacao_censo2022_t4709.json
└── preparada/                      mesmo conteúdo, formato legível pelo Spark
    └── ocorrencias/
        ├── ano_arquivo=2022/JAN-JUN_2022.parquet
        ├── ano_arquivo=2022/JUL-DEZ_2022.parquet
        └── ...
```

### Por que duas zonas

O Spark **não lê `.xlsx` nativamente**, e abrir uma planilha de 200 MB dentro do driver seria um gargalo. A zona preparada resolve isso convertendo para Parquet — formato colunar, comprimido e lido de forma distribuída.

A conversão é apenas de **formato**, nunca de conteúdo, e quatro decisões garantem isso:

1. **Tudo é gravado como texto.** A tipagem acontece no ETL, onde pode ser documentada e testada. Converter tipos na ingestão esconderia problemas.
2. **As sentinelas da fonte são preservadas.** `NULL`, `(Vazio)`, `-` e o `0` das coordenadas chegam intactos à análise de qualidade. Limpá-los aqui apagaria a evidência do problema antes que ele pudesse ser medido.
3. **Os nomes originais de cada ano são mantidos.** A conciliação entre `CIDADE` e `NOME_MUNICIPIO` é trabalho do ETL, não da ingestão.
4. **Colunas de auditoria são acrescentadas** — `_arquivo_origem`, `_guia_origem` e `_dt_ingestao` — que sustentam o registro de linhagem descrito em [`09-linhagem.md`](09-linhagem.md).

Datas e horas são a única exceção à regra do texto puro: o Excel as entrega como números seriais, e elas são renderizadas em formato canônico (`AAAA-MM-DD` e `HH:MM:SS`) para que a leitura não dependa da configuração regional de quem executa o notebook.

## O que a descoberta de esquema revelou

Antes de modelar qualquer coisa, o notebook lê o dicionário de campos que a própria SSP-SP embute como aba dentro de cada arquivo, e compara os cabeçalhos dos cinco anos. Três achados mudaram o desenho do pipeline:

**1. Os nomes das colunas mudam entre anos.** Não é uma variação cosmética — nove dos trinta campos têm grafias diferentes ao longo da série:

| Campo | 2022 | 2023 e 2024 | 2025 | 2026 |
|---|---|---|---|---|
| município do registro | `CIDADE` | `NOME_MUNICIPIO` | `NOME_MUNICIPIO` | `NOME_MUNICIPIO` |
| data do registro | `DATA_COMUNICACAO_BO` | `DATA_REGISTRO` | `DATA_REGISTRO` | `DATA_REGISTRO` |
| período do dia | `DESCR_PERIODO` | `DESC_PERIODO` | `DESC_PERIODO` | `DESC_PERIODO` |
| tipo de local | *ausente* | *ausente* | `DESCR_TIPOLOCAL` | `DESCR_TIPOLOCAL` |
| código do município | `CD_IBGE` | `CD_IBGE` | `CD_IBGE` | `COD IBGE` |
| delegacia da área | `NOME_DELEGACIA_CIRCUNCRIÇÃO` | idem | idem | `NOME_DELEGACIA_CIRCUNSCRICAO` |

Note que a fonte grafa "CIRCUNCRIÇÃO" (com erro e com acento) até 2025 e "CIRCUNSCRICAO" em 2026. É precisamente a "heterogeneidade semântica" que a Aula 1 aponta como razão de existir do processo de ETL. O de-para resultante está versionado em [`spark/etl_ocorrencias.py`](../spark/etl_ocorrencias.py) e é aplicado sobre os nomes normalizados, de modo que uma nova variação de acento não quebre o pipeline.

**2. O código IBGE acompanha a circunscrição, não o registro.** Os arquivos trazem *dois* municípios por linha: o de registro do boletim e o da delegacia de circunscrição, que é onde o fato ocorreu. A verificação sobre o arquivo de 2026 mostrou que, das 8.069 linhas com circunscrição em Sorocaba, **todas** têm `COD IBGE = 3552205`; já entre as linhas com *registro* em Sorocaba, o código varia (aparecem 3557006, 3523909 e outros). Isso define o critério de filtro do ETL — e é a diferença entre medir "crimes que aconteceram em Sorocaba" e "boletins que foram digitados em Sorocaba".

**3. A base não contém dados pessoais identificáveis.** Não há nome, documento, idade ou sexo de vítimas ou autores. Os campos com algum potencial de identificação descrevem o *local do fato*, e a própria Secretaria já suprime o endereço nos registros mais sensíveis: 2.097 das 8.069 linhas de Sorocaba em 2026 trazem, no lugar do logradouro, o texto `VEDAÇÃO DA DIVULGAÇÃO DOS DADOS RELATIVOS`. A decisão tomada a partir daí — não carregar o endereço exato no DW — está registrada em [`09-linhagem.md`](09-linhagem.md).

## Volume coletado

| Ano | Linhas no arquivo (estado de SP) | Linhas de Sorocaba | Guias |
|---|---|---|---|
| 2022 | 1.198.114 | 15.212 | JAN-JUN, JUL-DEZ |
| 2023 | 1.241.912 | 16.944 | JAN-JUN, JUL-DEZ |
| 2024 | 1.191.814 | 16.363 | JAN-JUN, JUL-DEZ |
| 2025 | 1.161.134 | 16.807 | JAN-JUN, JUL-DEZ |
| 2026 | 555.404 | 8.069 | JAN-JUN (ano incompleto) |
| **Total** | **5.348.378** | **73.395** | |

Sorocaba responde por **1,37%** dos registros do estado. Essa proporção é a justificativa técnica para o processamento distribuído na etapa seguinte: o filtro por município só pode ser aplicado *depois* de ler os 5,3 milhões de registros.

---

**Anterior:** [1. Objetivo](01-objetivo.md) · **Próximo:** [3. Modelagem](03-modelagem.md)
