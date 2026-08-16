# 8. Autoavaliação

> "Ao finalizar o trabalho, é esperado que o aluno faça uma autoavaliação contendo uma discussão sobre se conseguiu atingir os objetivos delineados antes do início das outras etapas, suas dificuldades encontradas na execução do trabalho, bem como trabalhos futuros para enriquecer o problema e sua solução em seu portfólio."

## Os objetivos foram atingidos?

O objetivo declarado em [`01-objetivo.md`](01-objetivo.md) era transformar os dados criminais brutos do estado de São Paulo em uma base histórica confiável sobre Sorocaba, capaz de sustentar respostas sobre quando, onde e de que tipo são as ocorrências registradas na cidade.

**Sim, com ressalvas declaradas em cada resposta.**

| Pergunta | Respondida? | Ressalva |
|---|---|---|
| P1 — evolução do volume | sim | a base mede ocorrências comunicadas, não crimes ocorridos |
| P2 — naturezas que crescem e caem | sim | — |
| P3 — sazonalidade | sim, com resposta negativa | boa parte da amplitude é artefato de calendário |
| P4 — período do dia e dia da semana | sim | restrita aos 75,3% com hora informada, em hora cheia |
| P5 — tipo de local | sim | restrita a 2025 e 2026, anos com o campo publicado |
| P6 — território | sim | por delegacia é confiável; por bairro, a cardinalidade segue inflada |
| P7 — crimes contra veículos | sim | é o indicador mais confiável da base |
| P8 — concentração espacial *(risco)* | sim | restrita aos 61,5% com coordenada; os ausentes não são amostra aleatória |

**A pergunta de risco deu certo.** P8 foi declarada no objetivo como aposta, porque dependia da qualidade de um campo que a fonte preenche de forma irregular. A cobertura de 61,5% com 99,2% de coordenadas dentro do município foi suficiente para responder — mas apenas depois de descobrir que o `0` era sentinela de ausência, e não uma coordenada.

**Uma resposta negativa também é resultado.** P3 concluiu que **não existe** sazonalidade relevante em Sorocaba. Estabelecer isso com solidez evita que se tome decisão de alocação sazonal com base em um padrão inexistente.

## Dificuldades encontradas

### 1. A fonte muda de esquema todo ano

Foi a dificuldade central, e não estava prevista. Nove dos trinta campos mudam de grafia ao longo da série, e um deles simplesmente não existe nos três primeiros anos. Pior: a fonte grafa `CIRCUNCRIÇÃO` (com erro e com acento) até 2025 e `CIRCUNSCRICAO` em 2026.

**Como foi superada:** construindo o de-para a partir da **descoberta de esquema executada sobre os arquivos reais**, e não a partir de suposição — e aplicando-o sobre nomes normalizados, para que a próxima variação de acento não quebre o pipeline. Essa etapa passou a ser a primeira coisa que o notebook 01 faz.

**A lição:** a etapa de descoberta de esquema deveria ter sido planejada desde o início. Ela foi acrescentada depois de a primeira leitura conjunta dos cinco anos produzir colunas majoritariamente vazias.

### 2. Descobrir qual município a linha realmente descreve

Os arquivos trazem **dois** municípios por linha — o de registro do boletim e o da circunscrição — e não é óbvio qual deles o código IBGE acompanha. Filtrar pelo errado significaria medir "boletins digitados em Sorocaba" em vez de "crimes ocorridos em Sorocaba".

**Como foi superada:** com verificação empírica. Cruzando os dois campos contra o código IBGE em todo o arquivo de 2026, ficou demonstrado que o código acompanha a **circunscrição** — das 8.069 linhas com circunscrição em Sorocaba, todas têm código 3552205, enquanto entre as com registro em Sorocaba o código varia.

**A lição:** foi a decisão de maior impacto do trabalho, e teria passado despercebida se o filtro tivesse sido escrito pelo nome do município, como parecia natural.

### 3. Problemas de qualidade que não geram erro

Os dois piores problemas encontrados são silenciosos: a coordenada `0`, que passa por qualquer validação de tipo e desloca qualquer média, e as grafias divergentes da natureza criminal, que fragmentam categorias sem gerar exceção. Nenhum dos dois apareceria em uma verificação de tipos — só no perfil valor a valor.

**A lição:** validar tipo não é validar qualidade.

### 4. Decidir quanto derivar

Três campos incompletos admitiam derivação: período, tipo de local e população de 2023. A tentação de "completar tudo" é grande, porque produz tabelas bonitas.

**Como foi tratada:** derivando, mas **marcando toda derivação** em coluna de procedência, e restringindo as perguntas ao dado publicado quando a derivação não era confiável — foi o caso de P5. E medir a confiança exigiu cuidado: a contagem bruta de subtipos ambíguos dava 79,5% dos registros derivados, número inútil porque dominado por erros de digitação da fonte; com o limiar de confiança de 95%, o número real é 2,2%.

### 5. Traduzir a arquitetura ensinada para outra nuvem

O curso ensina a arquitetura sobre Hadoop/Spark e a plataforma Databricks. A escolha da GCP exigiu mapear cada componente sem inventar equivalências: Cloud Storage no lugar do HDFS (a própria Aula 3 autoriza: *"é possível utilizar o AWS S3 ou Azure Blob Storage em vez do HDFS"*), Dataproc Serverless como o cluster Spark, BigQuery como o SGBD relacional que hospeda o DW em abordagem ROLAP.

**O que não foi transposto:** o vocabulário. Termos específicos de uma plataforma não descrevem conceitos de arquitetura de dados, e o modelo, as zonas do data lake e o processo de ETL estão descritos com o vocabulário das apostilas.

## O que eu faria diferente

1. **Executar a descoberta de esquema antes de escrever qualquer linha de ETL.** Ela foi feita depois, e obrigou a reescrever o de-para.
2. **Perfilar os valores antes de definir o modelo.** A dimensão tempo foi inicialmente projetada para começar em 2022; só depois apareceu um fato de 1976, que teria ficado órfão.
3. **Medir a ambiguidade da derivação com limiar desde o início**, em vez de contar tipos distintos — a primeira métrica era enganosa por uma ordem de grandeza.

## Trabalhos futuros

**Sobre a qualidade dos dados**

- **Resolver a cardinalidade do bairro** cruzando com um cadastro oficial de bairros de Sorocaba, ou com geocodificação reversa a partir das coordenadas — que resolveria os dois problemas de uma vez, já que 61,5% dos registros já têm coordenada.
- **Investigar o viés dos registros sem geolocalização.** Se a ausência estiver concentrada em registros da Delegacia Eletrônica, é possível medir e declarar o viés de P8 em vez de apenas alertar para ele.

**Sobre o modelo**

- **Estender a outros municípios.** O modelo já está preparado: `dim_municipio` é conformada e carregada do IBGE, e o filtro do ETL é um parâmetro. Bastaria remover a restrição para comparar Sorocaba com municípios paulistas de porte semelhante — o que responderia "Sorocaba é mais ou menos violenta que cidades comparáveis?", pergunta que este MVP não responde.
- **Acrescentar dimensões socioeconômicas** (renda, densidade, IDH por setor censitário do IBGE), permitindo correlacionar concentração criminal com característica de território. A apostila de Governança cita exatamente esse tipo de enriquecimento com dados abertos.
- **Incorporar a série de vítimas**, que a SSP-SP publica em arquivo separado, transformando o modelo em constelação com um terceiro fato conformado.

**Sobre a engenharia**

- **Automatizar a ingestão mensal.** Hoje o pipeline é executado sob demanda a partir dos notebooks. Com Cloud Scheduler e Cloud Run, a coleta poderia acompanhar a republicação mensal da SSP-SP.
- **Implementar carga incremental.** A carga atual é integral, o que é aceitável em 73 mil linhas mas não escalaria para o estado inteiro.
- **Adicionar testes automatizados de qualidade como etapa bloqueante do pipeline**, de modo que uma carga que quebre a conservação da medida não chegue ao data warehouse.
- **Publicar um painel no Looker Studio** sobre as visões analíticas, transformando o DW em ferramenta de consulta para quem não escreve SQL — o componente de dashboards que a arquitetura de BI da Aula 1 prevê e que este MVP deixou de fora.

## Balanço

O trabalho entregou o que se propôs: um pipeline completo de coleta, modelagem, carga e análise em nuvem, com um data warehouse dimensional que responde às oito perguntas declaradas antes da coleta.

O que considero mais valioso não são as respostas, mas o que foi preciso descobrir para chegar a elas: que a fonte troca nomes de coluna todo ano, que o código do município acompanha um campo e não o outro, que o zero das coordenadas é ausência disfarçada, que 41% dos horários são arredondados. Nenhuma dessas coisas está documentada em lugar algum — todas foram encontradas perfilando os dados. **A parte difícil da engenharia de dados não foi mover 5,3 milhões de registros; foi descobrir o que eles significam.**

---

**Anterior:** [7. Análise e resultados](07-analise-e-resultados.md) · **Próximo:** [9. Linhagem](09-linhagem.md)
