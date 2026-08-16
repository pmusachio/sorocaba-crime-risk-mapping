-- =============================================================================
-- Etapa 4 (Carga) — carga do fato de população
--
-- A população entra no modelo para permitir a taxa por 100 mil habitantes.
-- Sem ela, comparar o número absoluto de ocorrências entre 2022 e 2025 seria
-- enganoso: a população de Sorocaba cresceu cerca de 5% no período, de modo
-- que uma parte de qualquer aumento seria apenas reflexo de haver mais gente.
--
-- Cobertura da fonte oficial (IBGE):
--   2022 -> Censo Demográfico 2022 (tabela 4709)
--   2024 -> estimativa populacional (tabela 6579)
--   2025 -> estimativa populacional (tabela 6579)
--   2023 -> SEM NÚMERO OFICIAL PUBLICADO
--   2026 -> SEM NÚMERO OFICIAL PUBLICADO
--
-- Tratamento das lacunas, e por que ele é declarado aqui:
--
--   2023 recebe interpolação linear entre 2022 e 2024, marcada na coluna
--   origem_populacao com o valor 'interpolado'. A apostila de Qualidade de
--   Dados trata o preenchimento de ausentes como uma transformação legítima,
--   desde que registrada: "dados que estavam ausentes podem ter sido
--   preenchidos também segundo algum critério" — e é esse critério que a
--   coluna torna explícito, para que nenhuma análise confunda estimativa
--   oficial com número calculado por nós.
--
--   2026 fica de fora: além de não haver população publicada, o ano está
--   incompleto na base de ocorrências, e uma taxa anual calculada sobre meio
--   ano seria simplesmente errada. As perguntas que usam taxa se restringem
--   a 2022-2025, o que está declarado na documentação da análise.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 02 antes da execução
-- =============================================================================

TRUNCATE TABLE `@projeto.dw.fato_populacao_anual`;

INSERT INTO `@projeto.dw.fato_populacao_anual`
(sk_municipio, ano, populacao, origem_populacao)
WITH oficial AS (
  SELECT ano, populacao, origem_populacao
  FROM `@projeto.stg.populacao_ibge`
),
interpolado AS (
  SELECT
    2023 AS ano,
    CAST(ROUND((
      (SELECT populacao FROM oficial WHERE ano = 2022) +
      (SELECT populacao FROM oficial WHERE ano = 2024)
    ) / 2) AS INT64) AS populacao,
    'interpolado' AS origem_populacao
),
consolidado AS (
  SELECT * FROM oficial
  UNION ALL
  SELECT * FROM interpolado
)
SELECT
  municipio.sk_municipio,
  consolidado.ano,
  consolidado.populacao,
  consolidado.origem_populacao
FROM consolidado
CROSS JOIN (
  SELECT sk_municipio FROM `@projeto.dw.dim_municipio` WHERE cod_ibge = '3552205'
) AS municipio
ORDER BY consolidado.ano;
