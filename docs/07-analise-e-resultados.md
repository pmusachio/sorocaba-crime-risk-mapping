# 7. Análise e resultados

Execução, gráficos e discussão completa: [`notebooks/04_analise_resultados.ipynb`](../notebooks/04_analise_resultados.ipynb)
SQL: [`sql/40_views_analiticas.sql`](../sql/40_views_analiticas.sql) e [`sql/45_perguntas_negocio.sql`](../sql/45_perguntas_negocio.sql)

Esta página resume as respostas às oito perguntas declaradas em [`01-objetivo.md`](01-objetivo.md). A discussão detalhada de cada uma está no notebook, junto dos gráficos.

> Os números referem-se à coleta de **agosto de 2026**. A SSP-SP republica os arquivos periodicamente; uma nova execução do pipeline pode produzir números ligeiramente diferentes.

## Respostas

### P1 — Evolução do volume

| Ano | Ocorrências | População | Taxa / 100 mil |
|---|---|---|---|
| 2022 | 15.212 | 723.682 (censo) | 2.102 |
| 2023 | 16.944 | 740.571 (interpolado) | 2.288 |
| 2024 | 16.363 | 757.459 (estimativa) | 2.160 |
| 2025 | 16.806 | 762.172 (estimativa) | 2.205 |

**+10,5% em números absolutos, mas +4,9% em taxa.** Metade do crescimento aparente é crescimento populacional. O pico do período foi **2023**, não 2025 — a série oscila em torno de um patamar, não sobe de forma contínua.

### P2 — Composição e variação por natureza

**77,6%** de tudo é crime contra o patrimônio; uma única natureza (`FURTO - OUTROS`) responde por **57,3%**. Mas as naturezas se moveram em direções opostas entre 2022 e 2025:

| Em alta | | Em queda | |
|---|---|---|---|
| Tentativa de homicídio | +71,8% | Roubo — outros | −33,3% |
| Lesão corporal dolosa | +43,6% | Roubo de veículo | −23,9% |
| Furto de veículo | +36,7% | Tráfico de entorpecentes | −3,7% |
| Lesão corporal culposa (trânsito) | +30,1% | | |
| Estupro de vulnerável | +23,6% | | |

**Menos crime patrimonial violento, mais crime patrimonial sem confronto — e mais violência interpessoal.** São dois movimentos opostos que o número agregado de P1 esconde.

### P3 — Sazonalidade

Existe, mas é **fraca**: 22% de amplitude entre agosto (5.751) e fevereiro (4.715). Boa parte disso é artefato de calendário — fevereiro tem 28 dias contra 31 de agosto. Corrigido o número de dias, a sazonalidade real fica em torno de 10%. **Não há um "mês do crime"** que justifique alocação sazonal de recursos.

### P4 — Período do dia

No agregado, os quatro períodos são equilibrados (21,6% a 26,4%) — o que sugeriria ausência de padrão. O recorte por natureza desmente:

| | Furto | Roubo |
|---|---|---|
| Entre 18h e 23h | 19,2% | **36,2%** |

**O roubo concentra quase o dobro no período noturno.** É a informação diretamente acionável para policiamento ostensivo. O furto se distribui de forma plana pelas 24 horas.

### P5 — Tipo de local

| Tipo de local | Volume | % de crimes violentos |
|---|---|---|
| Via Pública | 55,4% | 20,1% |
| Residência | 18,1% | **41,9%** |
| Estabelecimento de Ensino | 1,0% | **51,2%** |
| Estacionamento/Garagem | 2,6% | 2,7% |

**O lugar mais perigoso da cidade é dentro de casa.** A rua tem o volume; a residência tem a gravidade. É a assinatura da violência doméstica, invisível em qualquer leitura por volume.

### P6 — Território

O 8º DP concentra **18,7%** das ocorrências e o 7º DP, **0,8%** — mais de vinte vezes de diferença. Mas o 7º DP, o menor em volume, tem a **maior proporção de crimes violentos (39,7%)**. Volume e gravidade apontam para lugares diferentes.

### P7 — Crimes contra veículos

| | 2022 | 2025 | Variação da taxa |
|---|---|---|---|
| Furto de veículo | 177,6 / 100 mil | 230,5 / 100 mil | **+30%** |
| Roubo de veículo | 46,3 / 100 mil | 33,5 / 100 mil | **−28%** |

A razão furto/roubo passou de 3,8 para 6,9. **É o indicador mais confiável da base**, porque a subnotificação de crimes contra veículos é mínima — o boletim é exigido pela seguradora. A queda do roubo é, portanto, muito provavelmente real.

### P8 — Concentração espacial *(pergunta de risco)*

**Respondida.** 61,5% das ocorrências têm coordenada válida, com cobertura estável entre anos (59,1% a 66,0%), e **99,2% das coordenadas presentes caem dentro da moldura de Sorocaba**.

Agregando em células de ~110 metros: cerca de 6.800 células ocupadas, a mais densa no Centro com 430 ocorrências, e as dez mais densas somando apenas **5% do total**. **A criminalidade de Sorocaba é dispersa, com pontos quentes moderados** — estratégias de *hot spot policing* teriam alcance limitado.

**Ressalva:** os 38,5% sem coordenada não são amostra aleatória. A conclusão sobre dispersão é sólida; a localização exata de cada ponto quente merece verificação antes de virar decisão operacional.

---

## Discussão geral

O problema declarado era transformar dados brutos e fragmentados do estado inteiro em base confiável e consultável sobre Sorocaba. Os 5,3 milhões de registros distribuídos em cinco arquivos de esquemas incompatíveis tornaram-se um data warehouse com 73.394 fatos, no qual cada pergunta é respondida por uma consulta de poucas linhas. **As oito perguntas foram respondidas**, inclusive a declarada como aposta de risco.

Sobre a criminalidade em Sorocaba, três conclusões:

1. **O volume cresceu menos do que parece** (4,9% em taxa, não 10,5%), com pico em 2023.
2. **A composição mudou mais do que o volume.** Menos crime patrimonial violento, mais furto — mas mais violência interpessoal (lesão corporal +44%, tentativa de homicídio +72%).
3. **Volume e gravidade nunca coincidem.** A rua tem volume, a casa tem gravidade. O distrito com menos ocorrências é o de maior proporção de crimes violentos. O furto não tem hora, o roubo tem.

**A conclusão que atravessa todas as respostas** é que *criminalidade* não é uma grandeza única. Cada vez que uma pergunta foi respondida no agregado e depois recortada por natureza, local ou horário, o recorte contradisse o agregado. É exatamente para isso que serve um modelo dimensional — e é o que justifica o custo de construir o warehouse em vez de responder cada pergunta com um script isolado sobre a planilha.

**O que a base não permite concluir.** Ela mede ocorrências **comunicadas**, não crimes ocorridos. Nenhuma variação pode ser atribuída com certeza a mudança na criminalidade em vez de mudança na propensão a registrar — exceto os crimes contra veículos. A limitação é da fonte, e está declarada desde o objetivo.

---

**Anterior:** [6. Qualidade de dados](06-qualidade-de-dados.md) · **Próximo:** [8. Autoavaliação](08-autoavaliacao.md)
