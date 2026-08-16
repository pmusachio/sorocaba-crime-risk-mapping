-- =============================================================================
-- Etapa 3 (Modelagem) — criação do esquema do data warehouse
--
-- Abordagem ROLAP: "utilizando uma abordagem ROLAP, cada fato e dimensão do
-- esquema de um DW é implementado em uma tabela" (Aula 1). O esquema é
-- ESTRELA: para cada fato há uma única tabela, relacionada a exatamente uma
-- tabela por dimensão, e as dimensões são desnormalizadas — suas hierarquias
-- são representadas por atributos da própria tabela dimensão.
--
-- Convenções adotadas, conforme a aula:
--   - a chave primária de cada dimensão é uma surrogate (código sequencial
--     sem significado próprio);
--   - a chave primária do fato é uma surrogate, mas a combinação das chaves
--     estrangeiras é única — é ela que determina a granularidade;
--   - toda dimensão tem uma linha de código -1 para "não informado", de modo
--     que nenhum fato fique sem ligação e nenhuma contagem se perca em junção;
--   - os campos descritores são textuais e por extenso, "livres de ambiguidade
--     (evitando abreviações, códigos e jargão específico)", porque são eles que
--     aparecem nos filtros, agrupamentos e títulos dos relatórios.
--
-- As restrições PRIMARY KEY e FOREIGN KEY são declaradas como NOT ENFORCED:
-- no BigQuery elas documentam o modelo e informam o otimizador de consultas,
-- sem impor verificação linha a linha na carga. A integridade referencial é
-- verificada explicitamente pelas consultas de validação (sql/30_qualidade.sql).
--
-- A descrição de cada tabela e de cada coluna é gravada na própria plataforma
-- (cláusula OPTIONS(description=...)), fazendo do data warehouse parte do
-- "repositório de metadados" previsto na arquitetura de BI da Aula 1.
--
-- Parâmetro: @projeto  -- substituído pelo notebook 02 antes da execução
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSÃO TEMPO
--
-- "Vale ressaltar a importância da dimensão tempo na modelagem dimensional de
-- DWs, não apenas por ser uma dimensão obrigatória por definição, mas também
-- por haver uma série de possibilidades de adicionar características à
-- dimensão tempo, de forma a permitir diversas hierarquias e níveis de
-- agregação" (Aula 1).
--
-- Hierarquia: ano > semestre > trimestre > mês > dia.
-- Cobertura de 1970 a 2026 porque a base traz fatos antigos registrados
-- recentemente: entre os dados de Sorocaba, o fato mais antigo é de 1976.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_tempo`
(
  sk_tempo         INT64   NOT NULL OPTIONS(description="Chave surrogate da dimensão tempo. -1 representa data não informada."),
  data             DATE             OPTIONS(description="Data no calendário civil. Domínio: 1970-01-01 a 2026-12-31."),
  ano              INT64            OPTIONS(description="Ano com quatro dígitos. Domínio: 1970 a 2026."),
  semestre         INT64            OPTIONS(description="Semestre do ano. Domínio: 1 ou 2."),
  trimestre        INT64            OPTIONS(description="Trimestre do ano. Domínio: 1 a 4."),
  mes              INT64            OPTIONS(description="Mês do ano. Domínio: 1 a 12."),
  nome_mes         STRING           OPTIONS(description="Nome do mês por extenso, em português. Domínio: Janeiro a Dezembro."),
  ano_mes          STRING           OPTIONS(description="Competência no formato AAAA-MM, para ordenação de séries mensais."),
  dia              INT64            OPTIONS(description="Dia do mês. Domínio: 1 a 31."),
  dia_semana       INT64            OPTIONS(description="Dia da semana. Domínio: 1 (domingo) a 7 (sábado)."),
  nome_dia_semana  STRING           OPTIONS(description="Nome do dia da semana por extenso. Domínio: Domingo a Sábado."),
  fim_de_semana    BOOL             OPTIONS(description="Verdadeiro para sábado e domingo."),
  PRIMARY KEY (sk_tempo) NOT ENFORCED
)
OPTIONS(description="Dimensão tempo, na granularidade de dia. Dimensão obrigatória do DW; usada em dois papéis pelo fato: data da ocorrência e mês de entrada na estatística oficial.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO PERÍODO DO DIA
--
-- Hierarquia: período > faixa horária > hora.
-- A dimensão comporta os três estados encontrados na fonte: hora conhecida
-- (com período derivado dela), hora desconhecida mas período informado, e
-- nenhum dos dois. Sem isso, o terço dos registros sem hora ficaria de fora
-- de qualquer análise por período.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_periodo_dia`
(
  sk_periodo_dia   INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão período do dia. -1 representa hora e período não informados."),
  hora             INT64           OPTIONS(description="Hora cheia da ocorrência. Domínio: 0 a 23; nulo quando a fonte não informou a hora."),
  faixa_horaria    STRING          OPTIONS(description="Faixa de três horas à qual a hora pertence, para agrupamento intermediário. Domínio: '00h-02h' a '21h-23h'; 'Não informada' quando a hora é nula."),
  periodo          STRING          OPTIONS(description="Período do dia. Domínio: DE MADRUGADA (0h-5h), PELA MANHA (6h-11h), A TARDE (12h-17h), A NOITE (18h-23h), EM HORA INCERTA (declarado assim pela fonte), NAO INFORMADO."),
  hora_informada   BOOL            OPTIONS(description="Verdadeiro quando a fonte publicou a hora da ocorrência."),
  PRIMARY KEY (sk_periodo_dia) NOT ENFORCED
)
OPTIONS(description="Dimensão de período do dia, na granularidade de hora cheia. Permite navegar de hora para faixa horária e para período (roll-up), e o caminho inverso (drill-down).");

-- -----------------------------------------------------------------------------
-- DIMENSÃO NATUREZA CRIMINAL
--
-- Hierarquia: categoria > natureza apurada > rubrica > conduta.
-- A rubrica é a tipificação jurídica do fato; a natureza apurada é a
-- classificação validada pela SSP-SP após auditoria; a categoria é o
-- agrupamento usado pela própria Secretaria em seus relatórios mensais.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_natureza`
(
  sk_natureza      INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão natureza criminal. -1 representa natureza não informada."),
  categoria        STRING          OPTIONS(description="Agrupamento da natureza criminal por título do Código Penal. Domínio: PATRIMONIO, PESSOA, DIGNIDADE SEXUAL, TRANSITO, DROGAS E ARMAS, OUTROS."),
  natureza_apurada STRING          OPTIONS(description="Classificação da natureza criminal validada e auditada pela SSP-SP, padronizada pelo ETL em maiúsculas, sem acentuação e com traço simples, porque a fonte publica a mesma natureza com grafias diferentes. Domínio: 23 valores, de FURTO - OUTROS a EXTORSAO MEDIANTE SEQUESTRO."),
  rubrica          STRING          OPTIONS(description="Natureza jurídica da ocorrência, com o artigo do Código Penal. Ex.: 'Furto (art. 155)'."),
  conduta          STRING          OPTIONS(description="Parágrafo, inciso ou circunstância relacionada à rubrica. Ex.: Transeunte, Veículo, Fios e Cabos."),
  crime_violento   BOOL            OPTIONS(description="Verdadeiro para naturezas que envolvem violência ou grave ameaça à pessoa, conforme a classificação declarada em sql/15_de_para_natureza.sql."),
  PRIMARY KEY (sk_natureza) NOT ENFORCED
)
OPTIONS(description="Dimensão da natureza criminal da ocorrência, na granularidade da combinação categoria/natureza/rubrica/conduta.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO TIPO DE LOCAL
--
-- Hierarquia: tipo de local > subtipo de local.
-- O tipo de local só foi publicado pela fonte a partir de 2025; para os anos
-- anteriores ele é derivado do subtipo, e a coluna origem_tipo_local registra
-- essa procedência.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_local`
(
  sk_local          INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão local. -1 representa local não informado."),
  tipo_local        STRING          OPTIONS(description="Grupo de tipos de local onde se deu o fato. Ex.: Via Pública, Residência, Shopping Center, Outros."),
  subtipo_local     STRING          OPTIONS(description="Subgrupo de local, vinculado ao tipo. Ex.: Casa, Praça, Lojas, Via Pública."),
  origem_tipo_local STRING          OPTIONS(description="Procedência do valor de tipo_local. Domínio: 'publicado pela fonte' (arquivos de 2025 e 2026), 'derivado do subtipo' (2022 a 2024), 'não informado'."),
  tipo_local_ambiguo BOOL           OPTIONS(description="Verdadeiro quando o tipo foi derivado e o subtipo de origem aparece associado a mais de um tipo na fonte, caso em que prevaleceu o mais frequente e o valor pode estar incorreto."),
  PRIMARY KEY (sk_local) NOT ENFORCED
)
OPTIONS(description="Dimensão do tipo de local onde o fato ocorreu, na granularidade da combinação tipo/subtipo.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO BAIRRO
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_bairro`
(
  sk_bairro        INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão bairro. -1 representa bairro não informado."),
  nome_bairro      STRING          OPTIONS(description="Nome do bairro padronizado em maiúsculas e sem acentuação, porque a fonte alterna a grafia do mesmo bairro entre registros."),
  bairro_informado BOOL            OPTIONS(description="Verdadeiro quando a fonte informou o bairro."),
  PRIMARY KEY (sk_bairro) NOT ENFORCED
)
OPTIONS(description="Dimensão do bairro de Sorocaba onde o fato ocorreu, conforme informado no boletim de ocorrência.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO DELEGACIA DE CIRCUNSCRIÇÃO
--
-- Hierarquia: departamento > seccional > delegacia.
-- Refere-se sempre à circunscrição (a delegacia responsável pela área onde o
-- fato ocorreu), e não à delegacia onde o boletim foi registrado — que, no
-- caso de registros feitos pela Delegacia Eletrônica, não tem relação
-- territorial com o fato.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_delegacia`
(
  sk_delegacia   INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão delegacia. -1 representa delegacia não informada."),
  delegacia      STRING          OPTIONS(description="Delegacia de polícia responsável pela área onde o fato ocorreu. Ex.: '08º D.P. SOROCABA'."),
  seccional      STRING          OPTIONS(description="Delegacia seccional à qual a delegacia pertence. Em Sorocaba: 'DEL.SEC.SOROCABA'."),
  departamento   STRING          OPTIONS(description="Departamento de polícia ao qual a seccional pertence. Em Sorocaba: 'DEINTER 7 - SOROCABA'."),
  PRIMARY KEY (sk_delegacia) NOT ENFORCED
)
OPTIONS(description="Dimensão da delegacia de circunscrição do fato, com a hierarquia departamento > seccional > delegacia.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO ÁREA DA POLÍCIA MILITAR
--
-- Hierarquia: comando > batalhão > companhia.
-- É um segundo recorte territorial do local do fato, independente do recorte
-- da Polícia Civil, e permite cruzar as duas divisões na análise.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_area_pm`
(
  sk_area_pm   INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão área da Polícia Militar. -1 representa área não informada."),
  companhia    STRING          OPTIONS(description="Companhia da Polícia Militar responsável pela área do fato. Ex.: 1ªCIA, 2ªCIA."),
  batalhao     STRING          OPTIONS(description="Batalhão da Polícia Militar. Em Sorocaba: 7ºBPM/I e 55ºBPM/I."),
  comando      STRING          OPTIONS(description="Comando de policiamento da área. Em Sorocaba: CPI-7."),
  PRIMARY KEY (sk_area_pm) NOT ENFORCED
)
OPTIONS(description="Dimensão da área de responsabilidade da Polícia Militar onde o fato ocorreu, com a hierarquia comando > batalhão > companhia.");

-- -----------------------------------------------------------------------------
-- DIMENSÃO MUNICÍPIO
--
-- Construída a partir do dado de referência do IBGE, e não do nome informado
-- no boletim. "A utilização de referências externas é interessante e deve ser
-- adotada quando for possível, pois permite a comparação e a utilização de
-- dados de várias fontes" (Governança, Aula 2).
--
-- É uma dimensão conformada: é compartilhada pelo fato de ocorrências e pelo
-- fato de população.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.dim_municipio`
(
  sk_municipio         INT64  NOT NULL OPTIONS(description="Chave surrogate da dimensão município. -1 representa município não informado."),
  cod_ibge             STRING          OPTIONS(description="Código do município no IBGE, com sete dígitos. Sorocaba: 3552205."),
  nome_municipio       STRING          OPTIONS(description="Nome oficial do município segundo o IBGE."),
  uf                   STRING          OPTIONS(description="Sigla da unidade da federação. Domínio: SP."),
  regiao_imediata      STRING          OPTIONS(description="Região geográfica imediata do IBGE à qual o município pertence."),
  regiao_intermediaria STRING          OPTIONS(description="Região geográfica intermediária do IBGE."),
  mesorregiao          STRING          OPTIONS(description="Mesorregião do IBGE."),
  microrregiao         STRING          OPTIONS(description="Microrregião do IBGE."),
  PRIMARY KEY (sk_municipio) NOT ENFORCED
)
OPTIONS(description="Dimensão município, construída a partir do dado de referência da API de Localidades do IBGE. Dimensão conformada, compartilhada pelos dois fatos do modelo.");

-- -----------------------------------------------------------------------------
-- FATO OCORRÊNCIA
--
-- Granularidade: uma natureza criminal apurada em um boletim de ocorrência
-- registrado com circunscrição em Sorocaba. Um mesmo boletim pode originar
-- mais de uma linha quando apura mais de uma natureza (ex.: um boletim com
-- roubo e lesão corporal gera duas linhas).
--
-- A medida é obrigatoriamente numérica e aditiva, conforme a definição de fato
-- da Aula 1: qtd_ocorrencia vale sempre 1 e é somada nas agregações.
--
-- num_bo e ano_bo são dimensões degeneradas: identificam o boletim, não têm
-- atributos descritivos próprios e por isso ficam na própria tabela fato.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.fato_ocorrencia`
(
  sk_ocorrencia                 INT64 NOT NULL OPTIONS(description="Chave surrogate do fato, sequencial e sem significado."),
  sk_tempo_ocorrencia           INT64          OPTIONS(description="Chave estrangeira para dim_tempo: data em que o fato ocorreu."),
  sk_tempo_estatistica          INT64          OPTIONS(description="Chave estrangeira para dim_tempo: primeiro dia do mês em que a ocorrência entrou na estatística oficial da SSP-SP. A estatística oficial tem granularidade mensal; o dia 1 é uma convenção de ligação, não uma data real."),
  sk_periodo_dia                INT64          OPTIONS(description="Chave estrangeira para dim_periodo_dia."),
  sk_natureza                   INT64          OPTIONS(description="Chave estrangeira para dim_natureza."),
  sk_local                      INT64          OPTIONS(description="Chave estrangeira para dim_local."),
  sk_bairro                     INT64          OPTIONS(description="Chave estrangeira para dim_bairro."),
  sk_delegacia                  INT64          OPTIONS(description="Chave estrangeira para dim_delegacia."),
  sk_area_pm                    INT64          OPTIONS(description="Chave estrangeira para dim_area_pm."),
  sk_municipio                  INT64          OPTIONS(description="Chave estrangeira para dim_municipio."),
  num_bo                        STRING         OPTIONS(description="Número do boletim de ocorrência. Dimensão degenerada."),
  ano_bo                        INT64          OPTIONS(description="Ano do boletim de ocorrência. Dimensão degenerada. Domínio: 2006 a 2026."),
  latitude                      FLOAT64        OPTIONS(description="Latitude do local do fato, em graus decimais. Domínio esperado: -23,60 a -23,35 (área de Sorocaba). Nulo quando a fonte não informou ou informou zero."),
  longitude                     FLOAT64        OPTIONS(description="Longitude do local do fato, em graus decimais. Domínio esperado: -47,60 a -47,35. Nulo quando a fonte não informou ou informou zero."),
  tem_geolocalizacao            BOOL           OPTIONS(description="Verdadeiro quando latitude e longitude são válidas."),
  registrado_em_outro_municipio BOOL           OPTIONS(description="Verdadeiro quando o boletim foi registrado em município diferente de Sorocaba, embora o fato tenha ocorrido em Sorocaba."),
  qtd_ocorrencia                INT64          OPTIONS(description="Medida aditiva do fato. Vale sempre 1; a soma responde ao número de ocorrências em qualquer combinação de dimensões."),
  PRIMARY KEY (sk_ocorrencia) NOT ENFORCED,
  FOREIGN KEY (sk_tempo_ocorrencia)  REFERENCES `@projeto.dw.dim_tempo`(sk_tempo)              NOT ENFORCED,
  FOREIGN KEY (sk_tempo_estatistica) REFERENCES `@projeto.dw.dim_tempo`(sk_tempo)              NOT ENFORCED,
  FOREIGN KEY (sk_periodo_dia)       REFERENCES `@projeto.dw.dim_periodo_dia`(sk_periodo_dia)  NOT ENFORCED,
  FOREIGN KEY (sk_natureza)          REFERENCES `@projeto.dw.dim_natureza`(sk_natureza)        NOT ENFORCED,
  FOREIGN KEY (sk_local)             REFERENCES `@projeto.dw.dim_local`(sk_local)              NOT ENFORCED,
  FOREIGN KEY (sk_bairro)            REFERENCES `@projeto.dw.dim_bairro`(sk_bairro)            NOT ENFORCED,
  FOREIGN KEY (sk_delegacia)         REFERENCES `@projeto.dw.dim_delegacia`(sk_delegacia)      NOT ENFORCED,
  FOREIGN KEY (sk_area_pm)           REFERENCES `@projeto.dw.dim_area_pm`(sk_area_pm)          NOT ENFORCED,
  FOREIGN KEY (sk_municipio)         REFERENCES `@projeto.dw.dim_municipio`(sk_municipio)      NOT ENFORCED
)
OPTIONS(description="Tabela fato do data warehouse. Granularidade: uma natureza criminal apurada em um boletim de ocorrência com circunscrição em Sorocaba, de 2022 a 2026.");

-- -----------------------------------------------------------------------------
-- FATO POPULAÇÃO ANUAL
--
-- Segundo fato do modelo, compartilhando a dimensão município com o fato de
-- ocorrências. Dois fatos que compartilham dimensões caracterizam uma
-- constelação de fatos — um dos três tipos de esquema multidimensional
-- apresentados na Aula 1. Ele existe para permitir a taxa por 100 mil
-- habitantes, sem a qual comparar anos diferentes seria enganoso.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE `@projeto.dw.fato_populacao_anual`
(
  sk_municipio      INT64  OPTIONS(description="Chave estrangeira para dim_municipio."),
  ano               INT64  OPTIONS(description="Ano de referência da população. Domínio: 2022 a 2025."),
  populacao         INT64  OPTIONS(description="População residente do município no ano. Domínio observado: 723.682 a 762.172."),
  origem_populacao  STRING OPTIONS(description="Procedência do número. Domínio: 'censo' (IBGE, Censo 2022), 'estimativa' (IBGE, tabela 6579), 'interpolado' (média entre os dois anos oficiais vizinhos, aplicada apenas a 2023, que não possui número oficial publicado)."),
  FOREIGN KEY (sk_municipio) REFERENCES `@projeto.dw.dim_municipio`(sk_municipio) NOT ENFORCED
)
OPTIONS(description="Fato de população residente por município e ano, na granularidade município x ano. Fonte: IBGE (Censo 2022 e estimativas populacionais).");
