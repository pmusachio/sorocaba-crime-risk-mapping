# 1. Objetivo

> Esta é a primeira etapa do trabalho e foi escrita **antes** de qualquer coleta, conforme a orientação do descritivo: *"comece pelo objetivo do seu trabalho. Antes de iniciar sua busca pelos dados, pense e descreva claramente qual problema deseja resolver com este MVP. Enumere as perguntas que deseja responder. É de extrema importância que esta etapa seja feita antes de iniciar qualquer outra etapa."*

## O problema

Sorocaba é a quarta maior cidade do interior paulista, com mais de 760 mil habitantes. A Secretaria da Segurança Pública do Estado de São Paulo publica mensalmente os dados de ocorrências criminais de todo o estado, mas os divulga em planilhas anuais de cerca de 200 MB, com mais de um milhão de linhas cada, cobrindo os 645 municípios paulistas de uma vez.

Nesse formato, os dados existem mas não são utilizáveis: **não é possível abrir os arquivos em uma planilha comum, nem cruzar anos diferentes**, porque a fonte muda os nomes das colunas de um ano para o outro, publica a mesma natureza criminal com grafias diferentes e deixa de publicar campos inteiros em alguns anos. Quem quer entender a criminalidade de um município específico — um gestor público, um jornalista, um morador, um vereador — esbarra nessa barreira antes de conseguir formular qualquer pergunta.

O problema que este MVP se propõe a resolver é, portanto:

> **Transformar os dados criminais brutos e fragmentados do estado de São Paulo em uma base histórica confiável, integrada e consultável sobre o município de Sorocaba, capaz de sustentar respostas sobre quando, onde e de que tipo são as ocorrências criminais registradas na cidade.**

O produto do trabalho não é um relatório: é um **data warehouse** com o histórico de 2022 a 2026, sobre o qual qualquer uma das perguntas abaixo — e outras não previstas — pode ser respondida com uma consulta.

## As perguntas de negócio

| # | Pergunta | Por que importa |
|---|---|---|
| **P1** | Como evoluiu o volume de ocorrências criminais em Sorocaba, em números absolutos e em taxa por 100 mil habitantes? | É a pergunta de fundo: a cidade ficou mais ou menos segura? A taxa é indispensável, porque a população cresceu no período e parte de qualquer variação absoluta é apenas reflexo disso. |
| **P2** | Quais naturezas criminais concentram o maior volume, e quais mais cresceram ou caíram no período? | "Criminalidade" não é uma coisa só. Furto e homicídio têm causas, magnitudes e políticas de enfrentamento distintas; um total agregado esconde movimentos opostos. |
| **P3** | Existe sazonalidade mensal nas ocorrências? O padrão se repete entre os anos? | Sazonalidade estável permite antecipar demanda; um pico isolado em um único ano é outro tipo de fenômeno. A distinção só aparece comparando anos. |
| **P4** | Como as ocorrências se distribuem por período do dia e dia da semana, e esse padrão muda conforme a natureza? | É a informação mais diretamente acionável para alocação de policiamento — e a hipótese a testar é que furto e roubo têm perfis horários diferentes. |
| **P5** | Que tipos de local concentram cada natureza criminal? | Distingue o crime de rua do crime em residência e em comércio, que pedem respostas diferentes. |
| **P6** | Como as ocorrências se distribuem no território, por delegacia de circunscrição e por bairro? | Localiza o problema. Sem recorte territorial, a discussão sobre segurança na cidade fica no abstrato. |
| **P7** | Furto e roubo de veículo: qual a evolução e a participação no total? | É um indicador clássico e sensível de segurança pública, e um dos poucos com subnotificação baixa, já que o registro é exigido pelo seguro. |
| **P8** | A geolocalização disponível permite identificar concentração espacial das ocorrências? | Se a qualidade das coordenadas permitir, é o maior salto analítico possível sobre esta base — de bairro para ponto. **Esta é uma pergunta de risco:** depende inteiramente da qualidade de um campo que a fonte preenche de forma irregular. |

### Sobre as perguntas que podem não ser respondidas

O descritivo é explícito: *"não é necessário atingir todos os objetivos desenhados nesta seção. Assim, não remova perguntas as quais não se conseguiu responder."* A pergunta **P8** foi mantida deliberadamente como uma aposta de risco, e a discussão sobre ela — tenha dado certo ou não — está em [`08-autoavaliacao.md`](08-autoavaliacao.md).

## Delimitação do escopo

| Dimensão | Escopo |
|---|---|
| **Território** | município de Sorocaba (código IBGE 3552205), pelo critério de **circunscrição** — o local onde o fato ocorreu, e não onde o boletim foi registrado |
| **Período** | 2022 a 2026. Não por escolha: são os únicos anos que a SSP-SP mantém publicados no portal (2018 a 2021 retornam erro 404) |
| **Granularidade** | uma natureza criminal apurada em um boletim de ocorrência |
| **Fora do escopo** | identificação de pessoas (a base não a contém), endereço exato do fato (deliberadamente não carregado), e comparação com outros municípios |

### Duas ressalvas que atravessam todas as respostas

São limitações da fonte, não do trabalho, e estão declaradas aqui porque afetam a leitura de qualquer número apresentado adiante:

1. **A base registra ocorrências, não crimes.** Só existe na base o que foi comunicado à polícia e virou boletim. Variações podem refletir mudança na criminalidade ou mudança na propensão a registrar — e a facilidade do registro online, que cresceu no período, empurra essa propensão para cima.

2. **O ano do arquivo é o ano da estatística, não o do fato.** O arquivo de cada ano é fechado pelo mês em que a ocorrência entrou na estatística oficial; ele contém fatos ocorridos em anos anteriores, alguns bem antigos. Toda série anual apresentada declara qual das duas datas está usando.

---

**Próximo:** [2. Coleta](02-coleta.md)
