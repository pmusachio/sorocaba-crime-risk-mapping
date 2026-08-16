-- =============================================================================
-- Etapa 3 (Modelagem) — tabela de-para da categorização das naturezas criminais
--
-- Por que esta tabela existe
-- --------------------------
-- A dimensão natureza precisa de um nível de agregação acima da natureza
-- apurada, para que a análise consiga responder perguntas como "os crimes
-- contra o patrimônio cresceram?" sem enumerar dez naturezas a cada consulta.
-- É a hierarquia da dimensão prevista na Aula 1: "em cada dimensão, podem-se
-- definir diferentes níveis de agregação, representados por hierarquias".
--
-- Critério da classificação
-- -------------------------
-- A categoria NÃO foi inventada: ela reproduz a divisão por título do Código
-- Penal, que é a mesma usada pela SSP-SP para agrupar as naturezas em seus
-- relatórios mensais:
--
--   PATRIMONIO ......: Título II do Código Penal (furto, roubo, latrocínio,
--                      extorsão)
--   PESSOA ..........: Título I (homicídio e lesão corporal)
--   DIGNIDADE SEXUAL : Título VI (estupro e estupro de vulnerável)
--   TRANSITO ........: infrações do Código de Trânsito Brasileiro que a fonte
--                      classifica separadamente por serem culposas e ligadas à
--                      circulação de veículos
--   DROGAS E ARMAS ..: Lei 11.343/2006 e Lei 10.826/2003
--
-- O indicador crime_violento marca as naturezas que envolvem violência ou
-- grave ameaça à pessoa. Ele separa, por exemplo, o furto do roubo — dois
-- crimes contra o patrimônio cuja diferença jurídica é exatamente essa, e cuja
-- distinção importa para qualquer leitura sobre segurança pública.
--
-- Cobertura
-- ---------
-- A lista abaixo cobre as 23 naturezas efetivamente encontradas nos arquivos
-- de 2022 a 2026 para Sorocaba, depois da padronização de acentuação e traços
-- feita no ETL. Naturezas que porventura apareçam em cargas futuras e não
-- constem aqui caem em 'OUTROS' — e a consulta de qualidade
-- (sql/30_qualidade.sql) lista quais são, para que a tabela seja estendida.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 02 antes da execução
-- =============================================================================

CREATE OR REPLACE TABLE `@projeto.dw.de_para_natureza`
(
  natureza_apurada STRING NOT NULL OPTIONS(description="Natureza apurada padronizada (maiúsculas, sem acentuação, traço simples), como gravada pelo ETL."),
  categoria        STRING          OPTIONS(description="Categoria da natureza criminal. Domínio: PATRIMONIO, PESSOA, DIGNIDADE SEXUAL, TRANSITO, DROGAS E ARMAS, OUTROS."),
  crime_violento   BOOL            OPTIONS(description="Verdadeiro quando a natureza envolve violência ou grave ameaça à pessoa."),
  PRIMARY KEY (natureza_apurada) NOT ENFORCED
)
OPTIONS(description="Tabela de referência que classifica cada natureza criminal em categoria e indica se envolve violência. Critério: divisão por título do Código Penal, conforme o agrupamento usado pela SSP-SP.");

INSERT INTO `@projeto.dw.de_para_natureza` (natureza_apurada, categoria, crime_violento)
VALUES
  -- Crimes contra o patrimônio (Título II do Código Penal)
  ('FURTO - OUTROS',                                   'PATRIMONIO',       FALSE),
  ('FURTO DE VEICULO',                                 'PATRIMONIO',       FALSE),
  ('FURTO DE CARGA',                                   'PATRIMONIO',       FALSE),
  ('ROUBO - OUTROS',                                   'PATRIMONIO',       TRUE),
  ('ROUBO DE VEICULO',                                 'PATRIMONIO',       TRUE),
  ('ROUBO DE CARGA',                                   'PATRIMONIO',       TRUE),
  ('LATROCINIO',                                       'PATRIMONIO',       TRUE),
  ('EXTORSAO MEDIANTE SEQUESTRO',                      'PATRIMONIO',       TRUE),

  -- Crimes contra a pessoa (Título I)
  ('HOMICIDIO DOLOSO',                                 'PESSOA',           TRUE),
  ('TENTATIVA DE HOMICIDIO',                           'PESSOA',           TRUE),
  ('HOMICIDIO CULPOSO OUTROS',                         'PESSOA',           FALSE),
  ('LESAO CORPORAL DOLOSA',                            'PESSOA',           TRUE),
  ('LESAO CORPORAL SEGUIDA DE MORTE',                  'PESSOA',           TRUE),
  ('LESAO CORPORAL CULPOSA - OUTRAS',                  'PESSOA',           FALSE),

  -- Crimes contra a dignidade sexual (Título VI)
  ('ESTUPRO',                                          'DIGNIDADE SEXUAL', TRUE),
  ('ESTUPRO DE VULNERAVEL',                            'DIGNIDADE SEXUAL', TRUE),

  -- Ocorrências de trânsito
  ('LESAO CORPORAL CULPOSA POR ACIDENTE DE TRANSITO',  'TRANSITO',         FALSE),
  ('HOMICIDIO CULPOSO POR ACIDENTE DE TRANSITO',       'TRANSITO',         FALSE),
  ('HOMICIDIO DOLOSO POR ACIDENTE DE TRANSITO',        'TRANSITO',         TRUE),

  -- Drogas e armas (Lei 11.343/2006 e Lei 10.826/2003)
  ('TRAFICO DE ENTORPECENTES',                         'DROGAS E ARMAS',   FALSE),
  ('PORTE DE ENTORPECENTES',                           'DROGAS E ARMAS',   FALSE),
  ('APREENSAO DE ENTORPECENTES',                       'DROGAS E ARMAS',   FALSE),
  ('PORTE DE ARMA',                                    'DROGAS E ARMAS',   FALSE);
