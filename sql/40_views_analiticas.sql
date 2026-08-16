-- =============================================================================
-- Etapa 5b (Análise) — visões analíticas
--
-- "Ferramentas de análise que consultam os dados diretamente no repositório ou
-- através de visões multidimensionais, construídas a partir do DW" (Aula 1).
-- As visões abaixo são essas visões multidimensionais: elas resolvem de uma vez
-- as junções do esquema estrela e entregam os atributos já legíveis, de modo
-- que cada pergunta de negócio vire uma consulta curta de agregação.
--
-- "Além de criar a visão com o comando CREATE VIEW... a expressão da consulta
-- fica armazenada e pode ser referenciada como se fosse uma tabela"
-- (Banco de Dados, aula de SQL DML).
--
-- Parâmetro: @projeto  -- substituído pelo notebook 04 antes da execução
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Visão consolidada do esquema estrela
--
-- É o "cubo" sobre o qual os operadores analíticos são aplicados: cada linha é
-- uma ocorrência descrita por todas as suas perspectivas. Filtrar colunas ou
-- registros nesta visão é o slice and dice; trocar o atributo do GROUP BY é o
-- pivoting; subir ou descer na hierarquia de uma dimensão é o roll-up e o
-- drill-down.
--
-- Duas leituras de tempo convivem aqui, e a diferença entre elas importa:
--
--   ano_ocorrencia  -> quando o fato aconteceu. É a data correta para analisar
--                      sazonalidade e horário.
--   ano_estatistica -> quando a ocorrência entrou na estatística oficial da
--                      SSP-SP. É a data que reproduz os números publicados pela
--                      Secretaria, e a usada nas séries anuais, porque o arquivo
--                      de cada ano é fechado por ela.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `@projeto.dw.vw_ocorrencias` AS
SELECT
  -- medida
  f.qtd_ocorrencia,

  -- tempo do fato
  t_ocorrencia.data        AS data_ocorrencia,
  t_ocorrencia.ano         AS ano_ocorrencia,
  t_ocorrencia.mes         AS mes_ocorrencia,
  t_ocorrencia.nome_mes    AS nome_mes_ocorrencia,
  t_ocorrencia.ano_mes     AS ano_mes_ocorrencia,
  t_ocorrencia.trimestre   AS trimestre_ocorrencia,
  t_ocorrencia.nome_dia_semana,
  t_ocorrencia.fim_de_semana,

  -- tempo da estatística oficial
  t_estatistica.ano        AS ano_estatistica,
  t_estatistica.mes        AS mes_estatistica,
  t_estatistica.ano_mes    AS ano_mes_estatistica,

  -- período do dia
  p.hora,
  p.faixa_horaria,
  p.periodo,
  p.hora_informada,

  -- natureza criminal
  n.categoria,
  n.natureza_apurada,
  n.rubrica,
  n.conduta,
  n.crime_violento,

  -- local
  l.tipo_local,
  l.subtipo_local,
  l.origem_tipo_local,
  l.tipo_local_ambiguo,
  b.nome_bairro,
  b.bairro_informado,

  -- território policial
  d.delegacia,
  d.seccional,
  d.departamento,
  a.companhia,
  a.batalhao,
  a.comando,

  -- município
  m.cod_ibge,
  m.nome_municipio,

  -- atributos do próprio fato
  f.num_bo,
  f.ano_bo,
  f.latitude,
  f.longitude,
  f.tem_geolocalizacao,
  f.registrado_em_outro_municipio
FROM `@projeto.dw.fato_ocorrencia` AS f
INNER JOIN `@projeto.dw.dim_tempo`       AS t_ocorrencia  ON t_ocorrencia.sk_tempo  = f.sk_tempo_ocorrencia
INNER JOIN `@projeto.dw.dim_tempo`       AS t_estatistica ON t_estatistica.sk_tempo = f.sk_tempo_estatistica
INNER JOIN `@projeto.dw.dim_periodo_dia` AS p             ON p.sk_periodo_dia       = f.sk_periodo_dia
INNER JOIN `@projeto.dw.dim_natureza`    AS n             ON n.sk_natureza          = f.sk_natureza
INNER JOIN `@projeto.dw.dim_local`       AS l             ON l.sk_local             = f.sk_local
INNER JOIN `@projeto.dw.dim_bairro`      AS b             ON b.sk_bairro            = f.sk_bairro
INNER JOIN `@projeto.dw.dim_delegacia`   AS d             ON d.sk_delegacia         = f.sk_delegacia
INNER JOIN `@projeto.dw.dim_area_pm`     AS a             ON a.sk_area_pm           = f.sk_area_pm
INNER JOIN `@projeto.dw.dim_municipio`   AS m             ON m.sk_municipio         = f.sk_municipio;

-- -----------------------------------------------------------------------------
-- Visão da taxa anual por 100 mil habitantes
--
-- Cruza os dois fatos do modelo pela dimensão conformada município e pelo ano.
-- É a junção que justifica a existência do segundo fato: sem população, a série
-- de ocorrências mistura variação de criminalidade com variação de população.
--
-- Restrita a 2022-2025: 2026 está incompleto na base (apenas o primeiro
-- semestre) e não possui população oficial publicada.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `@projeto.dw.vw_taxa_anual` AS
WITH ocorrencias_por_ano AS (
  SELECT
    ano_estatistica AS ano,
    cod_ibge,
    categoria,
    natureza_apurada,
    SUM(qtd_ocorrencia) AS ocorrencias
  FROM `@projeto.dw.vw_ocorrencias`
  GROUP BY ano, cod_ibge, categoria, natureza_apurada
)
SELECT
  o.ano,
  o.categoria,
  o.natureza_apurada,
  o.ocorrencias,
  pop.populacao,
  pop.origem_populacao,
  ROUND(100000 * o.ocorrencias / pop.populacao, 1) AS taxa_por_100_mil
FROM ocorrencias_por_ano AS o
INNER JOIN `@projeto.dw.dim_municipio`        AS m   ON m.cod_ibge     = o.cod_ibge
INNER JOIN `@projeto.dw.fato_populacao_anual` AS pop ON pop.sk_municipio = m.sk_municipio
                                                    AND pop.ano          = o.ano;
