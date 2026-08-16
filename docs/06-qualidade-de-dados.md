# 6. Análise da qualidade dos dados

Execução e evidências: [`notebooks/03_qualidade_dados.ipynb`](../notebooks/03_qualidade_dados.ipynb) · SQL: [`sql/30_qualidade.sql`](../sql/30_qualidade.sql)

O descritivo pede análise de qualidade "para cada atributo do conjunto de dados". O critério adotado é o da Aula 2 de Governança: avaliar coluna a coluna quais valores são esperados — domínio, faixa, formato, obrigatoriedade — e confrontar com o que existe. Como a apostila afirma, *"não é possível avaliar a qualidade de um conjunto de dados sem ter conhecimento dos seus metadados"*: por isso cada verificação declara o esperado antes de mostrar o encontrado, e os domínios esperados estão no [catálogo](04-catalogo-de-dados.md).

Os resultados são **persistidos** em duas tabelas do BigQuery — `qualidade.perfil_atributos` e `qualidade.verificacoes` — para que a evidência fique gravada na plataforma, e não apenas na saída de um notebook.

## Sumário dos problemas encontrados

| # | Problema | Evidência | Tratamento | Efeito residual |
|---|---|---|---|---|
| 1 | A mesma natureza criminal com grafias diferentes | 28 valores distintos na origem para 23 naturezas reais | padronização de acentos, traços e caixa (T4) | **resolvido** |
| 2 | Período do dia ausente | vazio em ~54% dos registros | derivação a partir da hora (T6) | ausência cai para **1,8%** |
| 3 | Tipo de local não publicado | ausente nos arquivos de 2022 a 2024 | derivação a partir do subtipo (T5) | 2,2% das derivações ambíguas; **P5 restrita aos anos publicados** |
| 4 | Bairro como campo livre | 1.251 grafias para algumas centenas de bairros reais | padronização e expansão de abreviações | cardinalidade cai ~12%; **ainda inflada** |
| 5 | Coordenadas ausentes ou zeradas | ~38% sem geolocalização utilizável; `0` usado como sentinela | zero convertido em nulo, com marcação (T2) | **P8 restrita a 61,5%** dos registros |
| 6 | Horários arredondados | 41% no minuto 00, 17% no minuto 30 | nenhum — é propriedade da fonte | **granularidade máxima: hora cheia** |
| 7 | Ano do arquivo ≠ ano do fato | 3% dos fatos em ano diferente; mais antigo de 1976 | duas datas no modelo, como papéis distintos da dimensão tempo | cada consulta declara qual usa |

E um caso que **não** é problema: cerca de 1.100 boletins geram mais de uma linha, porque apuram mais de uma natureza. É o grão do fato — deduplicar por número de boletim apagaria crimes.

## Os dois achados que mais mudaram o trabalho

**A coordenada zero.** A fonte usa `0` para representar ausência de geolocalização. Um `0` numérico passa por qualquer validação de tipo, entra em qualquer média e desloca qualquer centroide — sem gerar erro. Se não tivesse sido identificado, o mapa de P8 teria um ponto quente artificial no meio do Atlântico, e a média das coordenadas apontaria para um lugar onde nada aconteceu. Esse é o tipo de problema que só aparece quando se perfila **valor a valor**, e não apenas tipo a tipo.

**O arredondamento dos horários.** Não é um erro a corrigir, mas uma característica da fonte que muda o que se pode perguntar. Se 41% dos horários caem no minuto 00, a hora registrada é uma **estimativa da vítima**, não uma medição. Analisar por hora cheia é legítimo; comparar 14h10 com 14h40 seria ler ruído como sinal. Descobrir isso definiu a granularidade da dimensão de tempo do dia.

## Verificações de integridade do data warehouse

Como as restrições de chave no BigQuery são declaradas `NOT ENFORCED` — o banco as usa para otimizar consultas, mas não as verifica na carga —, a integridade é verificada explicitamente:

| Verificação | O que se espera |
|---|---|
| Conservação da medida | soma de `qtd_ocorrencia` no fato = número de linhas conformadas pelo ETL |
| Integridade referencial | nenhuma chave estrangeira apontando para linha inexistente |
| Unicidade do grão | nenhuma combinação repetida de boletim e chaves de dimensão |
| Domínio do município | todo código IBGE presente na referência oficial |
| Faixa da hora | entre 0 e 23, ou nulo |
| Faixa das coordenadas | dentro da moldura geográfica de Sorocaba |
| Consistência temporal | data da ocorrência nunca posterior à do registro |

Declarar uma restrição sem verificá-la seria pior do que não declará-la.

## Conclusão

**O conjunto de dados tem problemas?** Sim, sete — e nenhum era visível antes de perfilar atributo a atributo. Dois teriam produzido respostas erradas em silêncio (as grafias da natureza criminal e a coordenada zero) e dois teriam inviabilizado perguntas inteiras (o período ausente e o tipo de local não publicado).

**Eles afetam as respostas?** Depois do tratamento, de forma controlada e declarada:

| Pergunta | Situação |
|---|---|
| P1, P2, P7 | sem restrição — apoiam-se em campos completos |
| P3 | sem restrição, usando a data da ocorrência |
| P4 | restrita aos 75,3% com hora informada, na granularidade de hora cheia |
| P5 | restrita a 2025 e 2026 |
| P6 | delegacia sem restrição; bairro com cardinalidade ainda inflada |
| P8 | restrita aos 61,5% com coordenada válida |

Nenhuma pergunta foi respondida sem que sua limitação estivesse declarada junto da resposta.

---

**Anterior:** [5. Carga](05-carga-etl.md) · **Próximo:** [7. Análise e resultados](07-analise-e-resultados.md)
