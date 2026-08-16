-- =============================================================================
-- Etapa 4 (Carga) — carga da tabela fato
--
-- A tabela fato é carregada por junção com as dimensões já povoadas: cada
-- atributo descritivo da linha conformada é trocado pela chave surrogate da
-- dimensão correspondente. É o que dá ao esquema estrela os "caminhos curtos
-- de navegação no banco de dados (através das junções)" citados na Aula 1.
--
-- Todas as junções são LEFT JOIN com IFNULL(..., -1): se por qualquer motivo
-- um valor não encontrar par na dimensão, o fato continua existindo e aponta
-- para a linha "não informado", em vez de desaparecer da contagem. Quantos
-- fatos caíram nessa situação é medido em sql/30_qualidade.sql.
--
-- "Tabelas fato tipicamente têm alta cardinalidade e por isso não devem
-- armazenar informação redundante (como textos, valores nulos ou registros
-- com valores de medida zerados)" (Aula 1) — daí a tabela conter apenas
-- chaves, as duas dimensões degeneradas do boletim, as coordenadas e a medida.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 02 antes da execução
-- =============================================================================

TRUNCATE TABLE `@projeto.dw.fato_ocorrencia`;

INSERT INTO `@projeto.dw.fato_ocorrencia`
(sk_ocorrencia, sk_tempo_ocorrencia, sk_tempo_estatistica, sk_periodo_dia,
 sk_natureza, sk_local, sk_bairro, sk_delegacia, sk_area_pm, sk_municipio,
 num_bo, ano_bo, latitude, longitude, tem_geolocalizacao,
 registrado_em_outro_municipio, qtd_ocorrencia)
WITH preparado AS (
  -- Repete exatamente as mesmas expressões usadas na carga das dimensões.
  -- Manter as duas cargas simétricas é o que garante que toda linha do fato
  -- encontre a sua dimensão.
  SELECT
    o.data_ocorrencia,
    DATE(o.ano_estatistica, o.mes_estatistica, 1)        AS data_estatistica,
    o.hora,
    o.periodo_ocorrencia                                 AS periodo,
    IFNULL(o.natureza_apurada,           'NAO INFORMADO') AS natureza_apurada,
    IFNULL(o.rubrica,                    'NAO INFORMADO') AS rubrica,
    IFNULL(o.conduta,                    'NAO INFORMADO') AS conduta,
    IFNULL(o.tipo_local,                 'NAO INFORMADO') AS tipo_local,
    IFNULL(o.subtipo_local,              'NAO INFORMADO') AS subtipo_local,
    IFNULL(o.origem_tipo_local,          'não informado') AS origem_tipo_local,
    IFNULL(o.bairro_padronizado,         'NAO INFORMADO') AS nome_bairro,
    IFNULL(o.delegacia_circunscricao,    'NAO INFORMADO') AS delegacia,
    IFNULL(o.seccional_circunscricao,    'NAO INFORMADO') AS seccional,
    IFNULL(o.departamento_circunscricao, 'NAO INFORMADO') AS departamento,
    IFNULL(o.companhia_pm,               'NAO INFORMADO') AS companhia,
    IFNULL(o.batalhao_pm,                'NAO INFORMADO') AS batalhao,
    IFNULL(o.comando_pm,                 'NAO INFORMADO') AS comando,
    o.cod_ibge_municipio,
    o.num_bo,
    o.ano_bo,
    o.latitude,
    o.longitude,
    o.tem_geolocalizacao,
    o.registrado_em_outro_municipio
  FROM `@projeto.stg.ocorrencias_sorocaba` AS o
)
SELECT
  ROW_NUMBER() OVER (ORDER BY p.data_ocorrencia, p.num_bo, p.rubrica) AS sk_ocorrencia,
  IFNULL(t_ocorrencia.sk_tempo,   -1) AS sk_tempo_ocorrencia,
  IFNULL(t_estatistica.sk_tempo,  -1) AS sk_tempo_estatistica,
  IFNULL(periodo.sk_periodo_dia,  -1) AS sk_periodo_dia,
  IFNULL(natureza.sk_natureza,    -1) AS sk_natureza,
  IFNULL(local_fato.sk_local,    -1) AS sk_local,
  IFNULL(bairro.sk_bairro,        -1) AS sk_bairro,
  IFNULL(delegacia.sk_delegacia,  -1) AS sk_delegacia,
  IFNULL(area_pm.sk_area_pm,      -1) AS sk_area_pm,
  IFNULL(municipio.sk_municipio,  -1) AS sk_municipio,
  p.num_bo,
  p.ano_bo,
  p.latitude,
  p.longitude,
  p.tem_geolocalizacao,
  p.registrado_em_outro_municipio,
  1 AS qtd_ocorrencia
FROM preparado AS p
LEFT JOIN `@projeto.dw.dim_tempo` AS t_ocorrencia
       ON t_ocorrencia.data = p.data_ocorrencia
LEFT JOIN `@projeto.dw.dim_tempo` AS t_estatistica
       ON t_estatistica.data = p.data_estatistica
LEFT JOIN `@projeto.dw.dim_periodo_dia` AS periodo
       ON IFNULL(periodo.hora, -99) = IFNULL(p.hora, -99)
      AND periodo.periodo = p.periodo
LEFT JOIN `@projeto.dw.dim_natureza` AS natureza
       ON natureza.natureza_apurada = p.natureza_apurada
      AND natureza.rubrica          = p.rubrica
      AND natureza.conduta          = p.conduta
LEFT JOIN `@projeto.dw.dim_local` AS local_fato
       ON local_fato.tipo_local        = p.tipo_local
      AND local_fato.subtipo_local     = p.subtipo_local
      AND local_fato.origem_tipo_local = p.origem_tipo_local
LEFT JOIN `@projeto.dw.dim_bairro` AS bairro
       ON bairro.nome_bairro = p.nome_bairro
LEFT JOIN `@projeto.dw.dim_delegacia` AS delegacia
       ON delegacia.delegacia    = p.delegacia
      AND delegacia.seccional    = p.seccional
      AND delegacia.departamento = p.departamento
LEFT JOIN `@projeto.dw.dim_area_pm` AS area_pm
       ON area_pm.companhia = p.companhia
      AND area_pm.batalhao  = p.batalhao
      AND area_pm.comando   = p.comando
LEFT JOIN `@projeto.dw.dim_municipio` AS municipio
       ON municipio.cod_ibge = p.cod_ibge_municipio;
