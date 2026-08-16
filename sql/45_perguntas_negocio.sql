-- =============================================================================
-- Etapa 5b (Análise) — consultas que respondem às perguntas de negócio
--
-- Cada consulta abaixo responde a uma das perguntas declaradas no objetivo do
-- trabalho (docs/01-objetivo.md), antes de qualquer coleta. A discussão de cada
-- resultado está no notebook 04, junto dos gráficos; aqui fica o SQL, versionado
-- e executável isoladamente.
--
-- As consultas exercitam os operadores analíticos apresentados na Aula 1:
--   slice and dice -> filtros por natureza, por ano e por período
--   roll-up        -> subir de natureza para categoria, de dia para mês e ano
--   drill-down     -> descer de período para hora, de tipo para subtipo de local
--   pivoting       -> trocar a perspectiva de agrupamento da mesma medida
--
-- Parâmetro: @projeto  -- substituído pelo notebook 04 antes da execução
-- =============================================================================

-- -----------------------------------------------------------------------------
-- P1. Como evoluiu o volume total de ocorrências em Sorocaba, em números
--     absolutos e em taxa por 100 mil habitantes?
--
-- A taxa é o que permite comparar anos: a população do município cresceu no
-- período, de modo que parte de qualquer variação absoluta é apenas reflexo
-- disso. Restrita a 2022-2025, os anos completos e com população publicada.
-- -----------------------------------------------------------------------------
SELECT
  ano,
  SUM(ocorrencias)                                        AS ocorrencias,
  MAX(populacao)                                          AS populacao,
  MAX(origem_populacao)                                   AS origem_populacao,
  ROUND(100000 * SUM(ocorrencias) / MAX(populacao), 1)    AS taxa_por_100_mil,
  ROUND(100 * SAFE_DIVIDE(
    SUM(ocorrencias) - LAG(SUM(ocorrencias)) OVER (ORDER BY ano),
    LAG(SUM(ocorrencias)) OVER (ORDER BY ano)), 1)        AS variacao_percentual_absoluta
FROM `@projeto.dw.vw_taxa_anual`
GROUP BY ano
ORDER BY ano;

-- -----------------------------------------------------------------------------
-- P2. Quais naturezas criminais concentram o maior volume, e quais mais
--     cresceram ou caíram no período?
--
-- Roll-up: a mesma medida é apresentada por categoria e por natureza. A
-- cláusula ROLLUP acrescenta as linhas de subtotal por categoria e o total
-- geral, que é exatamente a navegação entre níveis de agregação de uma
-- hierarquia de dimensão.
-- -----------------------------------------------------------------------------
SELECT
  IFNULL(categoria, 'TOTAL GERAL')            AS categoria,
  IFNULL(natureza_apurada, 'subtotal')        AS natureza_apurada,
  SUM(qtd_ocorrencia)                         AS ocorrencias,
  ROUND(100 * SUM(qtd_ocorrencia)
        / SUM(SUM(qtd_ocorrencia)) OVER (), 2) AS percentual_do_total
FROM `@projeto.dw.vw_ocorrencias`
WHERE ano_estatistica BETWEEN 2022 AND 2025
GROUP BY ROLLUP (categoria, natureza_apurada)
ORDER BY categoria, ocorrencias DESC;

-- Variação de cada natureza entre o primeiro e o último ano completo
SELECT
  natureza_apurada,
  categoria,
  SUM(IF(ano = 2022, ocorrencias, 0))                     AS ocorrencias_2022,
  SUM(IF(ano = 2025, ocorrencias, 0))                     AS ocorrencias_2025,
  ROUND(100 * SAFE_DIVIDE(
    SUM(IF(ano = 2025, ocorrencias, 0)) - SUM(IF(ano = 2022, ocorrencias, 0)),
    SUM(IF(ano = 2022, ocorrencias, 0))), 1)              AS variacao_percentual,
  ROUND(SUM(IF(ano = 2025, taxa_por_100_mil, 0))
      - SUM(IF(ano = 2022, taxa_por_100_mil, 0)), 1)      AS variacao_da_taxa
FROM `@projeto.dw.vw_taxa_anual`
WHERE ano IN (2022, 2025)
GROUP BY natureza_apurada, categoria
HAVING ocorrencias_2022 + ocorrencias_2025 >= 100   -- ignora naturezas de volume irrelevante
ORDER BY variacao_percentual DESC;

-- -----------------------------------------------------------------------------
-- P3. Há sazonalidade mensal? O padrão se repete entre os anos?
--
-- Usa a data da ocorrência, e não a da estatística: sazonalidade é uma
-- propriedade de quando o fato acontece.
-- -----------------------------------------------------------------------------
SELECT
  mes_ocorrencia,
  nome_mes_ocorrencia,
  SUM(IF(ano_ocorrencia = 2022, qtd_ocorrencia, 0)) AS ano_2022,
  SUM(IF(ano_ocorrencia = 2023, qtd_ocorrencia, 0)) AS ano_2023,
  SUM(IF(ano_ocorrencia = 2024, qtd_ocorrencia, 0)) AS ano_2024,
  SUM(IF(ano_ocorrencia = 2025, qtd_ocorrencia, 0)) AS ano_2025,
  ROUND(AVG(qtd_ocorrencia) * COUNT(*) / 4, 0)      AS media_mensal_do_periodo
FROM `@projeto.dw.vw_ocorrencias`
WHERE ano_ocorrencia BETWEEN 2022 AND 2025
GROUP BY mes_ocorrencia, nome_mes_ocorrencia
ORDER BY mes_ocorrencia;

-- -----------------------------------------------------------------------------
-- P4. Como as ocorrências se distribuem por período do dia e dia da semana, e
--     esse padrão muda conforme a natureza criminal?
--
-- Restrita aos registros com hora informada: incluir os sem hora distorceria a
-- distribuição. Quantos ficam de fora está medido na análise de qualidade.
-- -----------------------------------------------------------------------------
SELECT
  periodo,
  nome_dia_semana,
  SUM(qtd_ocorrencia)                                    AS ocorrencias,
  SUM(IF(natureza_apurada = 'FURTO - OUTROS', qtd_ocorrencia, 0))  AS furto,
  SUM(IF(natureza_apurada = 'ROUBO - OUTROS', qtd_ocorrencia, 0))  AS roubo,
  ROUND(100 * SUM(IF(crime_violento, qtd_ocorrencia, 0))
        / SUM(qtd_ocorrencia), 1)                        AS percentual_violento
FROM `@projeto.dw.vw_ocorrencias`
WHERE hora_informada
  AND ano_estatistica BETWEEN 2022 AND 2025
GROUP BY periodo, nome_dia_semana
ORDER BY ocorrencias DESC;

-- Drill-down do período para a hora cheia, comparando furto e roubo
SELECT
  hora,
  SUM(IF(natureza_apurada = 'FURTO - OUTROS', qtd_ocorrencia, 0)) AS furto,
  SUM(IF(natureza_apurada = 'ROUBO - OUTROS', qtd_ocorrencia, 0)) AS roubo
FROM `@projeto.dw.vw_ocorrencias`
WHERE hora_informada AND ano_estatistica BETWEEN 2022 AND 2025
GROUP BY hora
ORDER BY hora;

-- -----------------------------------------------------------------------------
-- P5. Que tipos de local concentram cada natureza criminal?
--
-- Restrita a 2025 e 2026, os anos em que o tipo de local é publicado pela
-- fonte: nos anos anteriores o tipo é derivado do subtipo e parte da derivação
-- é ambígua, o que tornaria a comparação por tipo pouco confiável.
-- -----------------------------------------------------------------------------
SELECT
  tipo_local,
  SUM(qtd_ocorrencia)                                             AS ocorrencias,
  ROUND(100 * SUM(qtd_ocorrencia) / SUM(SUM(qtd_ocorrencia)) OVER (), 1) AS percentual,
  SUM(IF(categoria = 'PATRIMONIO', qtd_ocorrencia, 0))            AS patrimonio,
  SUM(IF(categoria = 'PESSOA', qtd_ocorrencia, 0))                AS pessoa,
  SUM(IF(crime_violento, qtd_ocorrencia, 0))                      AS violentos
FROM `@projeto.dw.vw_ocorrencias`
WHERE origem_tipo_local = 'publicado pela fonte'
GROUP BY tipo_local
ORDER BY ocorrencias DESC;

-- -----------------------------------------------------------------------------
-- P6. Como as ocorrências se distribuem no território, por delegacia de
--     circunscrição e por bairro?
-- -----------------------------------------------------------------------------
SELECT
  delegacia,
  SUM(qtd_ocorrencia)                                        AS ocorrencias,
  SUM(IF(crime_violento, qtd_ocorrencia, 0))                 AS violentos,
  ROUND(100 * SUM(IF(crime_violento, qtd_ocorrencia, 0))
        / SUM(qtd_ocorrencia), 1)                            AS percentual_violento
FROM `@projeto.dw.vw_ocorrencias`
WHERE ano_estatistica BETWEEN 2022 AND 2025
GROUP BY delegacia
ORDER BY ocorrencias DESC;

-- Drill-down para bairro, entre os bairros com volume relevante
SELECT
  nome_bairro,
  delegacia,
  SUM(qtd_ocorrencia)                        AS ocorrencias,
  SUM(IF(crime_violento, qtd_ocorrencia, 0)) AS violentos
FROM `@projeto.dw.vw_ocorrencias`
WHERE bairro_informado
  AND ano_estatistica BETWEEN 2022 AND 2025
GROUP BY nome_bairro, delegacia
HAVING ocorrencias >= 200
ORDER BY ocorrencias DESC
LIMIT 30;

-- -----------------------------------------------------------------------------
-- P7. Furto e roubo de veículo: evolução e participação no total.
-- -----------------------------------------------------------------------------
SELECT
  ano,
  SUM(IF(natureza_apurada = 'FURTO DE VEICULO', ocorrencias, 0))      AS furto_de_veiculo,
  SUM(IF(natureza_apurada = 'ROUBO DE VEICULO', ocorrencias, 0))      AS roubo_de_veiculo,
  SUM(IF(natureza_apurada = 'FURTO DE VEICULO', taxa_por_100_mil, 0)) AS taxa_furto_veiculo,
  SUM(IF(natureza_apurada = 'ROUBO DE VEICULO', taxa_por_100_mil, 0)) AS taxa_roubo_veiculo,
  ROUND(100 * SUM(IF(natureza_apurada IN ('FURTO DE VEICULO', 'ROUBO DE VEICULO'),
                     ocorrencias, 0)) / SUM(ocorrencias), 1)          AS percentual_do_total
FROM `@projeto.dw.vw_taxa_anual`
GROUP BY ano
ORDER BY ano;

-- -----------------------------------------------------------------------------
-- P8. A geolocalização permite identificar concentração espacial?
--
-- Primeiro a viabilidade: qual a cobertura de coordenadas válidas.
-- -----------------------------------------------------------------------------
SELECT
  ano_estatistica,
  COUNT(*)                                                   AS ocorrencias,
  COUNTIF(tem_geolocalizacao)                                AS com_coordenada,
  ROUND(100 * COUNTIF(tem_geolocalizacao) / COUNT(*), 1)     AS percentual_com_coordenada
FROM `@projeto.dw.vw_ocorrencias`
GROUP BY ano_estatistica
ORDER BY ano_estatistica;

-- Concentração espacial: as coordenadas são arredondadas a três casas decimais,
-- o que corresponde a células de aproximadamente 110 x 100 metros. Agregar em
-- células evita tratar cada ponto como único e revela onde há repetição.
SELECT
  ROUND(latitude, 3)                         AS latitude_celula,
  ROUND(longitude, 3)                        AS longitude_celula,
  ANY_VALUE(nome_bairro)                     AS bairro_predominante,
  SUM(qtd_ocorrencia)                        AS ocorrencias,
  SUM(IF(crime_violento, qtd_ocorrencia, 0)) AS violentos
FROM `@projeto.dw.vw_ocorrencias`
WHERE tem_geolocalizacao
  AND latitude  BETWEEN -23.60 AND -23.35
  AND longitude BETWEEN -47.60 AND -47.35
  AND ano_estatistica BETWEEN 2022 AND 2025
GROUP BY latitude_celula, longitude_celula
ORDER BY ocorrencias DESC
LIMIT 25;
