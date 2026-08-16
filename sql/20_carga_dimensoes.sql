-- =============================================================================
-- Etapa 4 (Carga) — carga das tabelas dimensão
--
-- "Usa-se o comando INSERT para a carga inicial de dados" (Banco de Dados,
-- aula de Carga e alteração de dados). Cada dimensão é esvaziada e recarregada
-- a partir da tabela conformada pelo job Spark, de modo que a execução seja
-- idempotente: rodar este script duas vezes produz exatamente o mesmo
-- resultado, sem duplicar linhas.
--
-- Três decisões valem para todas as dimensões:
--
--   1. A chave primária é uma surrogate gerada por ROW_NUMBER, "um código
--      sequencial e sem significado" (Aula 1).
--   2. Cada dimensão recebe uma linha de código -1 para "não informado". Sem
--      ela, um fato sem bairro ou sem hora ficaria órfão e desapareceria de
--      qualquer consulta com junção — o que falsearia as contagens.
--   3. Os valores nulos vindos da origem são convertidos em texto descritivo
--      ('NAO INFORMADO') dentro da própria dimensão, para que apareçam nos
--      relatórios em vez de sumirem.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 02 antes da execução
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_tempo
--
-- Gerada por calendário, e não a partir dos dados: uma dimensão tempo deve
-- conter todos os dias do intervalo, inclusive aqueles sem nenhuma ocorrência,
-- para que séries temporais não fiquem com buracos silenciosos.
--
-- O intervalo começa em 1970 por uma razão empírica: o arquivo de cada ano é
-- fechado pelo ano de ENTRADA NA ESTATÍSTICA, não pelo ano do fato, e traz
-- ocorrências antigas registradas recentemente. Nos dados de Sorocaba, 609
-- fatos ocorreram antes de 2022 e o mais antigo é de 1976. Um calendário que
-- começasse em 2022 deixaria esses fatos órfãos da dimensão tempo.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_tempo`;

INSERT INTO `@projeto.dw.dim_tempo`
(sk_tempo, data, ano, semestre, trimestre, mes, nome_mes, ano_mes, dia,
 dia_semana, nome_dia_semana, fim_de_semana)
WITH calendario AS (
  SELECT dia AS data
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '1970-01-01', DATE '2026-12-31')) AS dia
),
nomeado AS (
  SELECT
    data,
    EXTRACT(YEAR    FROM data) AS ano,
    IF(EXTRACT(MONTH FROM data) <= 6, 1, 2) AS semestre,
    EXTRACT(QUARTER FROM data) AS trimestre,
    EXTRACT(MONTH   FROM data) AS mes,
    EXTRACT(DAY     FROM data) AS dia,
    EXTRACT(DAYOFWEEK FROM data) AS dia_semana,
    FORMAT_DATE('%Y-%m', data) AS ano_mes
  FROM calendario
)
SELECT
  ROW_NUMBER() OVER (ORDER BY data) AS sk_tempo,
  data, ano, semestre, trimestre, mes,
  ['Janeiro','Fevereiro','Março','Abril','Maio','Junho',
   'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'][ORDINAL(mes)] AS nome_mes,
  ano_mes, dia, dia_semana,
  ['Domingo','Segunda-feira','Terça-feira','Quarta-feira',
   'Quinta-feira','Sexta-feira','Sábado'][ORDINAL(dia_semana)] AS nome_dia_semana,
  dia_semana IN (1, 7) AS fim_de_semana
FROM nomeado;

-- Linha de "não informado"
INSERT INTO `@projeto.dw.dim_tempo`
(sk_tempo, data, ano, semestre, trimestre, mes, nome_mes, ano_mes, dia,
 dia_semana, nome_dia_semana, fim_de_semana)
VALUES (-1, NULL, NULL, NULL, NULL, NULL, 'Não informado', 'Não informado',
        NULL, NULL, 'Não informado', NULL);

-- -----------------------------------------------------------------------------
-- dim_periodo_dia
--
-- Construída a partir das combinações de hora e período efetivamente presentes
-- nos dados. Fazer assim (em vez de gerar as 24 horas por calendário) garante
-- que nenhum fato fique órfão: a fonte às vezes informa o período sem a hora,
-- e às vezes informa "Em hora incerta", casos que um calendário de horas não
-- comportaria.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_periodo_dia`;

INSERT INTO `@projeto.dw.dim_periodo_dia`
(sk_periodo_dia, hora, faixa_horaria, periodo, hora_informada)
WITH combinacoes AS (
  -- A hora cheia já vem calculada pelo ETL: a fonte alterna os formatos
  -- '18:46:00' e '9:30:00', e a regra que trata os dois casos fica em um
  -- lugar só, no job Spark.
  SELECT DISTINCT hora, periodo_ocorrencia AS periodo
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY periodo, hora) AS sk_periodo_dia,
  hora,
  CASE
    WHEN hora IS NULL THEN 'Não informada'
    ELSE FORMAT('%02dh-%02dh', DIV(hora, 3) * 3, DIV(hora, 3) * 3 + 2)
  END AS faixa_horaria,
  periodo,
  hora IS NOT NULL AS hora_informada
FROM combinacoes;

INSERT INTO `@projeto.dw.dim_periodo_dia`
(sk_periodo_dia, hora, faixa_horaria, periodo, hora_informada)
VALUES (-1, NULL, 'Não informada', 'NAO INFORMADO', FALSE);

-- -----------------------------------------------------------------------------
-- dim_natureza
--
-- A categoria e o indicador de crime violento vêm da tabela de-para declarada
-- em sql/15_de_para_natureza.sql, que reproduz o agrupamento usado pela
-- própria SSP-SP. Naturezas sem correspondência caem em 'OUTROS', o que é
-- verificado na análise de qualidade.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_natureza`;

INSERT INTO `@projeto.dw.dim_natureza`
(sk_natureza, categoria, natureza_apurada, rubrica, conduta, crime_violento)
WITH combinacoes AS (
  SELECT DISTINCT
    IFNULL(natureza_apurada, 'NAO INFORMADO') AS natureza_apurada,
    IFNULL(rubrica,          'NAO INFORMADO') AS rubrica,
    IFNULL(conduta,          'NAO INFORMADO') AS conduta
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY c.natureza_apurada, c.rubrica, c.conduta) AS sk_natureza,
  IFNULL(d.categoria, 'OUTROS')    AS categoria,
  c.natureza_apurada,
  c.rubrica,
  c.conduta,
  IFNULL(d.crime_violento, FALSE)  AS crime_violento
FROM combinacoes AS c
LEFT JOIN `@projeto.dw.de_para_natureza` AS d
       ON d.natureza_apurada = c.natureza_apurada;

INSERT INTO `@projeto.dw.dim_natureza`
(sk_natureza, categoria, natureza_apurada, rubrica, conduta, crime_violento)
VALUES (-1, 'OUTROS', 'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO', FALSE);

-- -----------------------------------------------------------------------------
-- dim_local
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_local`;

INSERT INTO `@projeto.dw.dim_local`
(sk_local, tipo_local, subtipo_local, origem_tipo_local, tipo_local_ambiguo)
WITH combinacoes AS (
  SELECT DISTINCT
    IFNULL(tipo_local,         'NAO INFORMADO') AS tipo_local,
    IFNULL(subtipo_local,      'NAO INFORMADO') AS subtipo_local,
    IFNULL(origem_tipo_local,  'não informado') AS origem_tipo_local,
    IFNULL(tipo_local_ambiguo, FALSE)           AS tipo_local_ambiguo
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY tipo_local, subtipo_local, origem_tipo_local) AS sk_local,
  tipo_local, subtipo_local, origem_tipo_local, tipo_local_ambiguo
FROM combinacoes;

INSERT INTO `@projeto.dw.dim_local`
(sk_local, tipo_local, subtipo_local, origem_tipo_local, tipo_local_ambiguo)
VALUES (-1, 'NAO INFORMADO', 'NAO INFORMADO', 'não informado', FALSE);

-- -----------------------------------------------------------------------------
-- dim_bairro
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_bairro`;

INSERT INTO `@projeto.dw.dim_bairro`
(sk_bairro, nome_bairro, bairro_informado)
WITH combinacoes AS (
  SELECT DISTINCT IFNULL(bairro_padronizado, 'NAO INFORMADO') AS nome_bairro
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY nome_bairro) AS sk_bairro,
  nome_bairro,
  nome_bairro <> 'NAO INFORMADO' AS bairro_informado
FROM combinacoes;

INSERT INTO `@projeto.dw.dim_bairro`
(sk_bairro, nome_bairro, bairro_informado)
VALUES (-1, 'NAO INFORMADO', FALSE);

-- -----------------------------------------------------------------------------
-- dim_delegacia
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_delegacia`;

INSERT INTO `@projeto.dw.dim_delegacia`
(sk_delegacia, delegacia, seccional, departamento)
WITH combinacoes AS (
  SELECT DISTINCT
    IFNULL(delegacia_circunscricao,    'NAO INFORMADO') AS delegacia,
    IFNULL(seccional_circunscricao,    'NAO INFORMADO') AS seccional,
    IFNULL(departamento_circunscricao, 'NAO INFORMADO') AS departamento
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY departamento, seccional, delegacia) AS sk_delegacia,
  delegacia, seccional, departamento
FROM combinacoes;

INSERT INTO `@projeto.dw.dim_delegacia`
(sk_delegacia, delegacia, seccional, departamento)
VALUES (-1, 'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO');

-- -----------------------------------------------------------------------------
-- dim_area_pm
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_area_pm`;

INSERT INTO `@projeto.dw.dim_area_pm`
(sk_area_pm, companhia, batalhao, comando)
WITH combinacoes AS (
  SELECT DISTINCT
    IFNULL(companhia_pm, 'NAO INFORMADO') AS companhia,
    IFNULL(batalhao_pm,  'NAO INFORMADO') AS batalhao,
    IFNULL(comando_pm,   'NAO INFORMADO') AS comando
  FROM `@projeto.stg.ocorrencias_sorocaba`
)
SELECT
  ROW_NUMBER() OVER (ORDER BY comando, batalhao, companhia) AS sk_area_pm,
  companhia, batalhao, comando
FROM combinacoes;

INSERT INTO `@projeto.dw.dim_area_pm`
(sk_area_pm, companhia, batalhao, comando)
VALUES (-1, 'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO');

-- -----------------------------------------------------------------------------
-- dim_municipio
--
-- Carregada do dado de referência do IBGE, não do nome informado no boletim.
-- É o que permite validar, na análise de qualidade, se o município registrado
-- na fonte corresponde a um município que de fato existe.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE `@projeto.dw.dim_municipio`;

INSERT INTO `@projeto.dw.dim_municipio`
(sk_municipio, cod_ibge, nome_municipio, uf, regiao_imediata,
 regiao_intermediaria, mesorregiao, microrregiao)
SELECT
  ROW_NUMBER() OVER (ORDER BY cod_ibge) AS sk_municipio,
  cod_ibge, nome_municipio, uf, regiao_imediata,
  regiao_intermediaria, mesorregiao, microrregiao
FROM `@projeto.stg.municipio_ibge`;

INSERT INTO `@projeto.dw.dim_municipio`
(sk_municipio, cod_ibge, nome_municipio, uf, regiao_imediata,
 regiao_intermediaria, mesorregiao, microrregiao)
VALUES (-1, 'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO',
        'NAO INFORMADO', 'NAO INFORMADO', 'NAO INFORMADO');
