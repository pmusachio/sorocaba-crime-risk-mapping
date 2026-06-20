# Objetivo do MVP

## Problema

Sorocaba não possui uma ferramenta pública, atualizada e granular que permita a
cidadãos, pesquisadores ou gestores de segurança visualizar a distribuição
espaço-temporal de ocorrências criminais na cidade. Os dados existem e são
publicados pela Secretaria de Segurança Pública do Estado de São Paulo (SSP-SP),
mas estão disponibilizados em arquivos brutos anuais, sem consolidação em formato
analítico nem recorte municipal.

Este MVP constrói o pipeline de dados que estrutura essas ocorrências em um
modelo dimensional consultável (Esquema Estrela), servindo como base analítica
para este trabalho e como base preditiva para um MVP futuro de Machine Learning,
voltado à predição de ocorrências criminais na cidade.

## Perguntas de negócio

1. Quais bairros de Sorocaba concentram o maior volume de ocorrências
   registradas entre 2022 e 2026, e essa concentração se mantém estável ou
   migra ao longo do período?

2. Existe sazonalidade nas ocorrências por dia da semana e por mês? *(a
   análise por horário do dia é reportada com a ressalva de cobertura parcial
   do campo `hora_ocorrencia_bo` — ver Catálogo de Dados, Seção 7)*

3. Quais tipos de ocorrência (`rubrica`/`natureza_apurada`) predominam em
   cada bairro/região da cidade?

4. Há tendência de crescimento, queda ou estabilidade no volume de
   ocorrências ao longo dos anos? *(o ano de 2026 é analisado separadamente
   como período parcial — apenas Jan-Abr disponível no momento da coleta —
   e não é comparado diretamente a anos fechados sem normalização pela
   fração do ano coberta)*

5. Existe relação entre o tipo de local da ocorrência (`descr_tipolocal` —
   via pública, comércio, residência, transporte) e o tipo de ocorrência
   predominante? *(disponível apenas para registros de 2025 em diante, já
   que este campo não existe no schema da fonte em anos anteriores)*

6. Existe correlação espacial entre diferentes tipos de ocorrência — por
   exemplo, bairros com mais roubo de veículo também concentram mais furto?
   *(pergunta exploratória; pode não ser totalmente respondida com os dados
   e recursos disponíveis neste MVP)*

## Fonte dos dados

- **Origem:** SSP-SP / Portal de Dados Abertos do Estado de São Paulo,
  dataset "Números Sem Mistério".
- **Licença:** Creative Commons Attribution 4.0 (CC-BY 4.0).
- **URL de download:** `https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ANO}.xlsx`
- **Período coletado:** 2022 a 2026 (ano de 2026 parcial, dados até abril).

## Plataforma

Databricks Community Edition, com persistência em Delta Lake (arquitetura
medallion: Bronze → Silver → Gold).

## Observação sobre escopo

Conforme orientação do enunciado, nem todas as perguntas listadas acima
têm garantia de resposta completa — em particular a pergunta 6, de natureza
exploratória. A documentação deste objetivo permanece intacta ao longo do
trabalho; a discussão sobre o que foi ou não respondido está na
[Autoavaliação](./autoavaliacao.md).
