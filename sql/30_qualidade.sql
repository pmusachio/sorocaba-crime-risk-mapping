-- =============================================================================
-- Etapa 5a (Análise) — qualidade de dados
--
-- O trabalho pede uma análise de qualidade "para cada atributo do conjunto de
-- dados". Este script produz duas tabelas persistidas, para que a evidência
-- fique gravada na plataforma e não apenas na tela de um notebook:
--
--   qualidade.perfil_atributos -> um retrato de cada atributo: quantos valores
--                                 estão preenchidos, quantos são distintos,
--                                 qual o menor e o maior, e quantos ainda
--                                 carregam sentinelas da fonte;
--   qualidade.verificacoes ..... -> um conjunto de verificações com resultado
--                                 esperado explícito e situação (OK / ATENÇÃO).
--
-- O critério de qualidade adotado é o da Aula 2 de Governança de Dados: avaliar
-- "coluna a coluna" quais valores são esperados — domínio, faixa, formato,
-- obrigatoriedade — e confrontar com o que existe. Sem metadados não há
-- referência de qualidade: por isso cada verificação abaixo declara, em texto,
-- o que se esperava encontrar.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 03 antes da execução
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Perfil de cada atributo da tabela conformada
--
-- Todos os atributos são convertidos para texto e transpostos em pares
-- (atributo, valor), o que permite perfilar as trinta e poucas colunas com uma
-- única consulta. Nulos recebem o marcador '<<NULO>>' antes da transposição
-- porque UNPIVOT descarta valores nulos — e é justamente eles que precisamos
-- contar.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.qualidade.perfil_atributos` AS
WITH textual AS (
  SELECT
    IFNULL(num_bo,                                    '<<NULO>>') AS num_bo,
    IFNULL(CAST(ano_bo AS STRING),                    '<<NULO>>') AS ano_bo,
    IFNULL(CAST(data_ocorrencia AS STRING),           '<<NULO>>') AS data_ocorrencia,
    IFNULL(CAST(data_registro AS STRING),             '<<NULO>>') AS data_registro,
    IFNULL(hora_ocorrencia,                           '<<NULO>>') AS hora_ocorrencia,
    -- Numéricos de faixa curta recebem zero à esquerda: sem isso, o mínimo e o
    -- máximo do perfil seriam calculados em ordem alfabética ('9' > '23').
    IFNULL(LPAD(CAST(hora AS STRING), 2, '0'),        '<<NULO>>') AS hora,
    IFNULL(periodo_ocorrencia,                        '<<NULO>>') AS periodo_ocorrencia,
    IFNULL(origem_periodo,                            '<<NULO>>') AS origem_periodo,
    IFNULL(LPAD(CAST(mes_estatistica AS STRING), 2, '0'), '<<NULO>>') AS mes_estatistica,
    IFNULL(CAST(ano_estatistica AS STRING),           '<<NULO>>') AS ano_estatistica,
    IFNULL(cod_ibge_municipio,                        '<<NULO>>') AS cod_ibge_municipio,
    IFNULL(municipio_circunscricao,                   '<<NULO>>') AS municipio_circunscricao,
    IFNULL(municipio_registro,                        '<<NULO>>') AS municipio_registro,
    IFNULL(delegacia_circunscricao,                   '<<NULO>>') AS delegacia_circunscricao,
    IFNULL(seccional_circunscricao,                   '<<NULO>>') AS seccional_circunscricao,
    IFNULL(departamento_circunscricao,                '<<NULO>>') AS departamento_circunscricao,
    IFNULL(tipo_local,                                '<<NULO>>') AS tipo_local,
    IFNULL(subtipo_local,                             '<<NULO>>') AS subtipo_local,
    IFNULL(origem_tipo_local,                         '<<NULO>>') AS origem_tipo_local,
    IFNULL(bairro,                                    '<<NULO>>') AS bairro,
    IFNULL(bairro_padronizado,                        '<<NULO>>') AS bairro_padronizado,
    IFNULL(CAST(latitude AS STRING),                  '<<NULO>>') AS latitude,
    IFNULL(CAST(longitude AS STRING),                 '<<NULO>>') AS longitude,
    IFNULL(rubrica,                                   '<<NULO>>') AS rubrica,
    IFNULL(conduta,                                   '<<NULO>>') AS conduta,
    IFNULL(natureza_apurada,                          '<<NULO>>') AS natureza_apurada,
    IFNULL(natureza_apurada_origem,                   '<<NULO>>') AS natureza_apurada_origem,
    IFNULL(comando_pm,                                '<<NULO>>') AS comando_pm,
    IFNULL(batalhao_pm,                               '<<NULO>>') AS batalhao_pm,
    IFNULL(companhia_pm,                              '<<NULO>>') AS companhia_pm,
    IFNULL(CAST(ano_arquivo AS STRING),               '<<NULO>>') AS ano_arquivo
  FROM `@projeto.stg.ocorrencias_sorocaba`
),
longo AS (
  SELECT atributo, valor
  FROM textual
  UNPIVOT (valor FOR atributo IN (
    num_bo, ano_bo, data_ocorrencia, data_registro, hora_ocorrencia, hora,
    periodo_ocorrencia, origem_periodo, mes_estatistica, ano_estatistica,
    cod_ibge_municipio, municipio_circunscricao, municipio_registro,
    delegacia_circunscricao, seccional_circunscricao, departamento_circunscricao,
    tipo_local, subtipo_local, origem_tipo_local, bairro, bairro_padronizado,
    latitude, longitude, rubrica, conduta, natureza_apurada,
    natureza_apurada_origem, comando_pm, batalhao_pm, companhia_pm, ano_arquivo
  ))
)
SELECT
  atributo,
  COUNT(*)                                                        AS total_registros,
  COUNTIF(valor = '<<NULO>>')                                     AS nulos,
  ROUND(100 * COUNTIF(valor = '<<NULO>>') / COUNT(*), 2)          AS percentual_nulos,
  COUNT(DISTINCT IF(valor = '<<NULO>>', NULL, valor))             AS valores_distintos,
  -- Quantos valores distintos deixariam de ser distintos se a acentuação e a
  -- caixa fossem ignoradas. Diferente de zero indica a mesma coisa escrita de
  -- formas diferentes — o problema que a padronização do ETL trata.
  COUNT(DISTINCT IF(valor = '<<NULO>>', NULL, valor))
    - COUNT(DISTINCT IF(valor = '<<NULO>>', NULL,
        UPPER(NORMALIZE_AND_CASEFOLD(valor, NFKC))))              AS distintos_por_grafia,
  MIN(IF(valor = '<<NULO>>', NULL, valor))                        AS valor_minimo,
  MAX(IF(valor = '<<NULO>>', NULL, valor))                        AS valor_maximo
FROM longo
GROUP BY atributo
ORDER BY percentual_nulos DESC, atributo;

-- -----------------------------------------------------------------------------
-- 2. Verificações com resultado esperado declarado
--
-- Cada linha responde a uma pergunta objetiva sobre a integridade dos dados.
-- A coluna situacao permite ordenar pelo que precisa de atenção.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.qualidade.verificacoes` AS

-- --- Integridade da carga ----------------------------------------------------
WITH v01 AS (
  SELECT
    'Carga: fato x staging' AS verificacao,
    'A soma da medida no fato deve ser igual ao número de linhas conformadas pelo ETL.' AS esperado,
    CAST((SELECT SUM(qtd_ocorrencia) FROM `@projeto.dw.fato_ocorrencia`) AS STRING)
      || ' no fato / '
      || CAST((SELECT COUNT(*) FROM `@projeto.stg.ocorrencias_sorocaba`) AS STRING)
      || ' na staging' AS resultado,
    IF((SELECT SUM(qtd_ocorrencia) FROM `@projeto.dw.fato_ocorrencia`)
       = (SELECT COUNT(*) FROM `@projeto.stg.ocorrencias_sorocaba`), 'OK', 'ATENÇÃO') AS situacao
),

-- --- Integridade referencial -------------------------------------------------
-- As restrições de chave estrangeira são declaradas NOT ENFORCED no BigQuery,
-- ou seja, o banco não as verifica. A verificação é feita aqui, explicitamente.
v02 AS (
  SELECT
    'Integridade referencial: fatos órfãos' AS verificacao,
    'Nenhuma chave estrangeira do fato deve apontar para uma linha inexistente na dimensão.' AS esperado,
    CAST(COUNT(*) AS STRING) || ' fatos órfãos' AS resultado,
    IF(COUNT(*) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.dw.fato_ocorrencia` AS f
  WHERE NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_tempo`        d WHERE d.sk_tempo       = f.sk_tempo_ocorrencia)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_tempo`        d WHERE d.sk_tempo       = f.sk_tempo_estatistica)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_periodo_dia`  d WHERE d.sk_periodo_dia = f.sk_periodo_dia)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_natureza`     d WHERE d.sk_natureza    = f.sk_natureza)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_local`        d WHERE d.sk_local       = f.sk_local)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_bairro`       d WHERE d.sk_bairro      = f.sk_bairro)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_delegacia`    d WHERE d.sk_delegacia   = f.sk_delegacia)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_area_pm`      d WHERE d.sk_area_pm     = f.sk_area_pm)
     OR NOT EXISTS (SELECT 1 FROM `@projeto.dw.dim_municipio`    d WHERE d.sk_municipio   = f.sk_municipio)
),

-- --- Granularidade -----------------------------------------------------------
-- "Não deve existir mais de um registro na tabela fato com valores idênticos
-- das chaves de todas as dimensões" (Aula 1).
v03 AS (
  SELECT
    'Granularidade: unicidade do grão' AS verificacao,
    'Não deve haver duas linhas do fato com a mesma combinação de todas as chaves estrangeiras e do boletim.' AS esperado,
    CAST(COUNT(*) AS STRING) || ' combinações repetidas' AS resultado,
    IF(COUNT(*) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM (
    SELECT 1
    FROM `@projeto.dw.fato_ocorrencia`
    GROUP BY num_bo, ano_bo, sk_tempo_ocorrencia, sk_periodo_dia, sk_natureza,
             sk_local, sk_bairro, sk_delegacia, sk_area_pm
    HAVING COUNT(*) > 1
  )
),

-- --- Domínio: município ------------------------------------------------------
-- Conferência contra o dado de referência do IBGE: "a cidade informada precisa
-- ser uma dos cerca de 5.570 municípios existentes no Brasil" (Governança, Aula 2).
v04 AS (
  SELECT
    'Domínio: município válido no IBGE' AS verificacao,
    'Todo código de município do fato deve existir na dimensão carregada da API do IBGE.' AS esperado,
    CAST(COUNTIF(m.cod_ibge IS NULL) AS STRING) || ' fatos com município fora da referência' AS resultado,
    IF(COUNTIF(m.cod_ibge IS NULL) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.dw.fato_ocorrencia` AS f
  LEFT JOIN `@projeto.dw.dim_municipio` AS m ON m.sk_municipio = f.sk_municipio
  WHERE f.sk_municipio <> -1
),

-- --- Faixa esperada: hora ----------------------------------------------------
v05 AS (
  SELECT
    'Faixa: hora da ocorrência' AS verificacao,
    'A hora deve estar entre 0 e 23, ou ser nula quando a fonte não informou.' AS esperado,
    CAST(COUNTIF(hora IS NOT NULL AND hora NOT BETWEEN 0 AND 23) AS STRING) || ' horas fora da faixa' AS resultado,
    IF(COUNTIF(hora IS NOT NULL AND hora NOT BETWEEN 0 AND 23) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.dw.dim_periodo_dia`
),

-- --- Faixa esperada: coordenadas ---------------------------------------------
-- Sorocaba está contida aproximadamente entre as latitudes -23,60 e -23,35 e as
-- longitudes -47,60 e -47,35. Coordenadas fora dessa moldura descrevem um ponto
-- que não fica no município e, portanto, não podem ser usadas na análise espacial.
v06 AS (
  SELECT
    'Faixa: coordenadas dentro de Sorocaba' AS verificacao,
    'Latitude entre -23,60 e -23,35 e longitude entre -47,60 e -47,35 para os fatos com geolocalização.' AS esperado,
    CAST(COUNTIF(NOT (latitude BETWEEN -23.60 AND -23.35
                  AND longitude BETWEEN -47.60 AND -47.35)) AS STRING)
      || ' de ' || CAST(COUNT(*) AS STRING) || ' fatos geolocalizados caem fora da moldura' AS resultado,
    IF(COUNTIF(NOT (latitude BETWEEN -23.60 AND -23.35
                AND longitude BETWEEN -47.60 AND -47.35)) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.dw.fato_ocorrencia`
  WHERE tem_geolocalizacao
),

-- --- Consistência temporal ---------------------------------------------------
v07 AS (
  SELECT
    'Consistência: ocorrência antes do registro' AS verificacao,
    'A data da ocorrência nunca deve ser posterior à data do registro do boletim.' AS esperado,
    CAST(COUNTIF(data_ocorrencia > data_registro) AS STRING) || ' registros com data de ocorrência no futuro do registro' AS resultado,
    IF(COUNTIF(data_ocorrencia > data_registro) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.stg.ocorrencias_sorocaba`
  WHERE data_ocorrencia IS NOT NULL AND data_registro IS NOT NULL
),

-- --- Cobertura da categorização ----------------------------------------------
v08 AS (
  SELECT
    'Cobertura: naturezas sem categoria' AS verificacao,
    "Toda natureza apurada deve ter correspondência na tabela de-para; as sem correspondência caem em 'OUTROS'." AS esperado,
    IFNULL(STRING_AGG(DISTINCT natureza_apurada, '; '), 'nenhuma') AS resultado,
    IF(COUNT(*) = 0, 'OK', 'ATENÇÃO') AS situacao
  FROM `@projeto.dw.dim_natureza`
  WHERE categoria = 'OUTROS' AND natureza_apurada <> 'NAO INFORMADO'
),

-- --- Efeito da padronização --------------------------------------------------
-- Mede o problema que o ETL corrigiu: quantas grafias distintas a fonte usa
-- para a mesma natureza criminal.
v09 AS (
  SELECT
    'Padronização: grafias da natureza apurada' AS verificacao,
    'A fonte publica a mesma natureza com acentuação e traços diferentes; após a padronização o número de valores distintos deve cair.' AS esperado,
    CAST(COUNT(DISTINCT natureza_apurada_origem) AS STRING) || ' grafias na origem -> '
      || CAST(COUNT(DISTINCT natureza_apurada) AS STRING) || ' naturezas após padronizar' AS resultado,
    IF(COUNT(DISTINCT natureza_apurada_origem) > COUNT(DISTINCT natureza_apurada), 'TRATADO', 'OK') AS situacao
  FROM `@projeto.stg.ocorrencias_sorocaba`
),

-- --- Efeito da derivação do período ------------------------------------------
v10 AS (
  SELECT
    'Completude: período do dia' AS verificacao,
    'O período é derivado da hora quando a fonte não o informa; o percentual não informado deve cair após a derivação.' AS esperado,
    FORMAT('%.1f%% publicado pela fonte, %.1f%% derivado da hora, %.1f%% sem informação',
           100 * COUNTIF(origem_periodo = 'publicado pela fonte') / COUNT(*),
           100 * COUNTIF(origem_periodo = 'derivado da hora') / COUNT(*),
           100 * COUNTIF(origem_periodo = 'não informado') / COUNT(*)) AS resultado,
    'INFORMATIVO' AS situacao
  FROM `@projeto.stg.ocorrencias_sorocaba`
),

-- --- Efeito da derivação do tipo de local ------------------------------------
v11 AS (
  SELECT
    'Completude: tipo de local' AS verificacao,
    'O tipo de local só é publicado a partir de 2025; nos anos anteriores é derivado do subtipo, e a derivação pode ser ambígua.' AS esperado,
    FORMAT('%.1f%% publicado, %.1f%% derivado (dos quais %.1f%% com subtipo ambíguo)',
           100 * COUNTIF(origem_tipo_local = 'publicado pela fonte') / COUNT(*),
           100 * COUNTIF(origem_tipo_local = 'derivado do subtipo') / COUNT(*),
           SAFE_DIVIDE(100 * COUNTIF(tipo_local_ambiguo),
                       COUNTIF(origem_tipo_local = 'derivado do subtipo'))) AS resultado,
    'INFORMATIVO' AS situacao
  FROM `@projeto.stg.ocorrencias_sorocaba`
),

-- --- Divergência entre local do fato e local do registro ---------------------
v12 AS (
  SELECT
    'Consistência: registro fora de Sorocaba' AS verificacao,
    'O fato é atribuído ao município da circunscrição; parte dos boletins é registrada em outro município.' AS esperado,
    FORMAT('%d de %d fatos (%.2f%%) foram registrados fora de Sorocaba',
           COUNTIF(registrado_em_outro_municipio), COUNT(*),
           100 * COUNTIF(registrado_em_outro_municipio) / COUNT(*)) AS resultado,
    'INFORMATIVO' AS situacao
  FROM `@projeto.dw.fato_ocorrencia`
),

-- --- Cobertura temporal ------------------------------------------------------
-- O arquivo de cada ano é organizado pelo ano de ENTRADA NA ESTATÍSTICA, não
-- pelo ano do fato: um boletim registrado em 2026 pode se referir a um fato de
-- 2019. Isso é característica da fonte, não erro, mas muda a leitura das séries.
v13 AS (
  SELECT
    'Cobertura: ano do fato x ano da estatística' AS verificacao,
    'O arquivo de cada ano contém fatos ocorridos em anos anteriores; séries anuais devem declarar qual das duas datas usam.' AS esperado,
    FORMAT('%.2f%% dos fatos ocorreram em ano diferente do ano da estatística (mais antigo: %d)',
           100 * COUNTIF(ano_ocorrencia <> ano_estatistica) / COUNT(*),
           MIN(ano_ocorrencia)) AS resultado,
    'INFORMATIVO' AS situacao
  FROM (
    SELECT EXTRACT(YEAR FROM data_ocorrencia) AS ano_ocorrencia, ano_estatistica
    FROM `@projeto.stg.ocorrencias_sorocaba`
    WHERE data_ocorrencia IS NOT NULL
  )
),

-- --- Duplicidade de boletim --------------------------------------------------
v14 AS (
  SELECT
    'Duplicidade: boletins com mais de uma linha' AS verificacao,
    'Um mesmo boletim pode gerar mais de uma linha quando apura mais de uma natureza — é o grão do fato, não duplicidade.' AS esperado,
    FORMAT('%d boletins distintos para %d linhas de fato; %d boletins com mais de uma natureza',
           COUNT(*), SUM(linhas), COUNTIF(linhas > 1)) AS resultado,
    'INFORMATIVO' AS situacao
  FROM (
    SELECT num_bo, ano_bo, COUNT(*) AS linhas
    FROM `@projeto.dw.fato_ocorrencia`
    GROUP BY num_bo, ano_bo
  )
)

SELECT * FROM v01
UNION ALL SELECT * FROM v02
UNION ALL SELECT * FROM v03
UNION ALL SELECT * FROM v04
UNION ALL SELECT * FROM v05
UNION ALL SELECT * FROM v06
UNION ALL SELECT * FROM v07
UNION ALL SELECT * FROM v08
UNION ALL SELECT * FROM v09
UNION ALL SELECT * FROM v10
UNION ALL SELECT * FROM v11
UNION ALL SELECT * FROM v12
UNION ALL SELECT * FROM v13
UNION ALL SELECT * FROM v14;
