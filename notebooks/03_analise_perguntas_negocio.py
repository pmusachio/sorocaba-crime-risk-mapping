# Databricks notebook source
# =============================================================================
# MVP de Engenharia de Dados — Ocorrências registradas em Sorocaba
# Notebook 03 — Análise das três perguntas de negócio
#
# Todas as consultas analíticas usam apenas a Gold. Respostas, coberturas,
# limitações e conclusão são geradas com os valores observados na execução.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # Análise de Dados
# MAGIC
# MAGIC O termo **ocorrência** significa uma natureza registrada em uma linha da
# MAGIC fonte, depois da deduplicação por linha integralmente idêntica. As contagens
# MAGIC não medem diretamente criminalidade real, risco individual ou causalidade.

# COMMAND ----------

import html
import math

import matplotlib.pyplot as plt
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "sorocaba_seguranca"
ANO_INICIAL = 2022
ANO_FINAL = 2025
TOP_N_RANKING = 10
TOP_N_CRUZAMENTO = 5
NAO_INFORMADO = "NÃO INFORMADO"

TABELAS_NECESSARIAS = [
    "fato_ocorrencia",
    "dim_tempo",
    "dim_periodo_dia",
    "dim_natureza",
    "validacoes_qualidade",
]

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

ausentes = [
    tabela
    for tabela in TABELAS_NECESSARIAS
    if not spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{tabela}")
]
if ausentes:
    raise RuntimeError(
        "Execute os notebooks 00, 01 e 02 antes da análise. "
        f"Tabelas ausentes: {', '.join(ausentes)}"
    )

erros_qualidade = spark.table("validacoes_qualidade").filter(
    F.col("status") == "ERRO"
).count()
if erros_qualidade:
    raise RuntimeError(
        f"A análise foi interrompida: há {erros_qualidade} validação(ões) de qualidade com ERRO."
    )

fato = spark.table("fato_ocorrencia")
total_fato = fato.agg(F.sum("qtd_ocorrencia").alias("total")).first()["total"] or 0
if total_fato == 0:
    raise RuntimeError("A fato está vazia; não há dados para responder às perguntas.")

plt.rcParams.update({
    "figure.dpi": 120,
    "axes.titlesize": 11,
    "axes.labelsize": 9,
    "axes.grid": True,
})


def inteiro(valor):
    return f"{int(valor):,}".replace(",", ".")


def percentual(parte, total):
    return 100.0 * float(parte) / float(total) if total else 0.0


def texto_variacao(inicial, final):
    if inicial == 0:
        return "não é calculável porque o primeiro total é zero"
    variacao = 100.0 * (final - inicial) / inicial
    sentido = "aumento" if variacao > 0 else "queda" if variacao < 0 else "estabilidade"
    return f"{sentido} de {abs(variacao):.1f}%"


def mostrar_resposta(titulo, resposta, cobertura, limitacao, reconciliacao):
    displayHTML(f"""
    <div style="border-left:4px solid #2463a2;padding:10px 14px;margin:12px 0">
      <h3 style="margin:0 0 8px">{html.escape(titulo)}</h3>
      <p><strong>Resposta direta:</strong> {html.escape(resposta)}</p>
      <p><strong>Cobertura:</strong> {html.escape(cobertura)}</p>
      <p><strong>Limitação:</strong> {html.escape(limitacao)}</p>
      <p><strong>Reconciliação:</strong> {html.escape(reconciliacao)}</p>
    </div>
    """)


print(f"Fato disponível: {inteiro(total_fato)} ocorrências registradas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 1 — Como evoluiu o volume mensal e anual entre 2022 e 2025?

# COMMAND ----------

mensal = spark.sql(f"""
    SELECT
        t.ano,
        t.mes,
        t.nome_mes,
        SUM(f.qtd_ocorrencia) AS total_ocorrencias
    FROM fato_ocorrencia f
    INNER JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE t.ano BETWEEN {ANO_INICIAL} AND {ANO_FINAL}
    GROUP BY t.ano, t.mes, t.nome_mes
    ORDER BY t.ano, t.mes
""")

anual = mensal.groupBy("ano").agg(
    F.sum("total_ocorrencias").alias("total_ocorrencias")
).orderBy("ano")

display(anual)
display(mensal)

pdf_mensal = mensal.toPandas()
pdf_anual = anual.toPandas()

fig, eixos = plt.subplots(1, 2, figsize=(13, 4.5))
for ano, grupo in pdf_mensal.groupby("ano"):
    eixos[0].plot(
        grupo["mes"], grupo["total_ocorrencias"], marker="o", linewidth=1.8,
        label=str(int(ano)),
    )
eixos[0].set(title="Evolução mensal", xlabel="Mês", ylabel="Ocorrências registradas")
eixos[0].set_xticks(range(1, 13))
eixos[0].legend(title="Ano", ncols=2)
eixos[1].bar(pdf_anual["ano"].astype(str), pdf_anual["total_ocorrencias"], color="#2463a2")
eixos[1].set(title="Total anual", xlabel="Ano", ylabel="Ocorrências registradas")
for indice, valor in enumerate(pdf_anual["total_ocorrencias"]):
    eixos[1].text(indice, valor, inteiro(valor), ha="center", va="bottom", fontsize=8)
plt.tight_layout()
plt.show()

total_com_data_periodo = int(mensal.agg(F.sum("total_ocorrencias")).first()[0] or 0)
if total_com_data_periodo == 0:
    raise RuntimeError(
        f"Não há ocorrências com data entre {ANO_INICIAL} e {ANO_FINAL}."
    )
sem_data = fato.filter(F.col("sk_tempo") == -1).agg(
    F.sum("qtd_ocorrencia")
).first()[0] or 0
fora_periodo = spark.sql(f"""
    SELECT COALESCE(SUM(f.qtd_ocorrencia), 0) AS total
    FROM fato_ocorrencia f
    INNER JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    WHERE t.sk_tempo <> -1 AND (t.ano < {ANO_INICIAL} OR t.ano > {ANO_FINAL})
""").first()["total"]

if total_com_data_periodo + sem_data + fora_periodo != total_fato:
    raise AssertionError("A agregação temporal não reconcilia com a fato.")

anos_observados = {int(r["ano"]): int(r["total_ocorrencias"]) for r in anual.collect()}
primeiro = anos_observados.get(ANO_INICIAL, 0)
ultimo = anos_observados.get(ANO_FINAL, 0)
ano_max, total_max = max(anos_observados.items(), key=lambda item: item[1])
pico_mensal = mensal.orderBy(F.desc("total_ocorrencias"), "ano", "mes").first()

resposta_p1 = (
    f"Foram contabilizadas {inteiro(total_com_data_periodo)} ocorrências com data entre "
    f"{ANO_INICIAL} e {ANO_FINAL}. O total passou de {inteiro(primeiro)} em {ANO_INICIAL} "
    f"para {inteiro(ultimo)} em {ANO_FINAL}, uma {texto_variacao(primeiro, ultimo)}. "
    f"O maior total anual foi {inteiro(total_max)} em {ano_max}; o pico mensal foi "
    f"{pico_mensal['nome_mes']} de {pico_mensal['ano']}, com "
    f"{inteiro(pico_mensal['total_ocorrencias'])}."
)
cobertura_p1 = (
    f"{percentual(total_com_data_periodo, total_fato):.2f}% da fato entrou na janela; "
    f"{inteiro(sem_data)} registro(s) não têm data e {inteiro(fora_periodo)} têm data fora dela."
)
limite_p1 = (
    "A variação descreve registros administrativos. Ela pode refletir cobertura, registro "
    "ou classificação e não demonstra mudança causal da criminalidade."
)
reconciliacao_p1 = (
    f"{inteiro(total_com_data_periodo)} incluídos + {inteiro(sem_data)} sem data + "
    f"{inteiro(fora_periodo)} fora da janela = {inteiro(total_fato)} na fato."
)
mostrar_resposta("Resposta da pergunta 1", resposta_p1, cobertura_p1, limite_p1, reconciliacao_p1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 2 — Quais naturezas concentram maior volume e como variaram?

# COMMAND ----------

ranking_natureza = spark.sql(f"""
    SELECT
        n.natureza_apurada,
        SUM(f.qtd_ocorrencia) AS total_ocorrencias
    FROM fato_ocorrencia f
    INNER JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    INNER JOIN dim_natureza n ON f.sk_natureza = n.sk_natureza
    WHERE t.ano BETWEEN {ANO_INICIAL} AND {ANO_FINAL}
    GROUP BY n.natureza_apurada
    ORDER BY total_ocorrencias DESC, n.natureza_apurada
""")

top_classificadas = ranking_natureza.filter(
    F.col("natureza_apurada") != NAO_INFORMADO
).orderBy(F.desc("total_ocorrencias"), "natureza_apurada").limit(TOP_N_CRUZAMENTO)

naturezas_top = [r["natureza_apurada"] for r in top_classificadas.collect()]
if not naturezas_top:
    raise RuntimeError("Nenhuma natureza classificada foi encontrada para as perguntas 2 e 3.")

# A view temporária contém somente categorias derivadas da consulta Gold acima.
top_classificadas.select("natureza_apurada").createOrReplaceTempView("top_naturezas")
evolucao_top = spark.sql(f"""
    SELECT
        t.ano,
        n.natureza_apurada,
        SUM(f.qtd_ocorrencia) AS total_ocorrencias
    FROM fato_ocorrencia f
    INNER JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    INNER JOIN dim_natureza n ON f.sk_natureza = n.sk_natureza
    INNER JOIN top_naturezas top ON n.natureza_apurada = top.natureza_apurada
    WHERE t.ano BETWEEN {ANO_INICIAL} AND {ANO_FINAL}
    GROUP BY t.ano, n.natureza_apurada
    ORDER BY n.natureza_apurada, t.ano
""")

display(ranking_natureza.limit(TOP_N_RANKING))
display(evolucao_top)

pdf_ranking = ranking_natureza.limit(TOP_N_RANKING).toPandas().sort_values(
    "total_ocorrencias", ascending=True
)
pdf_evolucao = evolucao_top.toPandas()

fig, eixos = plt.subplots(1, 2, figsize=(14, 5))
eixos[0].barh(
    pdf_ranking["natureza_apurada"], pdf_ranking["total_ocorrencias"], color="#3a7d44"
)
eixos[0].set(title=f"Top {TOP_N_RANKING} no período", xlabel="Ocorrências registradas")
for natureza, grupo in pdf_evolucao.groupby("natureza_apurada"):
    eixos[1].plot(
        grupo["ano"], grupo["total_ocorrencias"], marker="o", linewidth=1.6,
        label=natureza,
    )
eixos[1].set(title=f"Variação anual das {TOP_N_CRUZAMENTO} principais", xlabel="Ano", ylabel="Ocorrências")
eixos[1].set_xticks(range(ANO_INICIAL, ANO_FINAL + 1))
eixos[1].legend(fontsize=7, loc="best")
plt.tight_layout()
plt.show()

total_ranking = int(ranking_natureza.agg(F.sum("total_ocorrencias")).first()[0] or 0)
if total_ranking != total_com_data_periodo:
    raise AssertionError("O ranking de naturezas não reconcilia com a análise temporal.")

linhas_ranking = ranking_natureza.collect()
top_conhecidas = [r for r in linhas_ranking if r["natureza_apurada"] != NAO_INFORMADO]
desconhecidas = next(
    (int(r["total_ocorrencias"]) for r in linhas_ranking if r["natureza_apurada"] == NAO_INFORMADO),
    0,
)
top_tres = top_conhecidas[:3]
lista_top = "; ".join(
    f"{r['natureza_apurada']} ({inteiro(r['total_ocorrencias'])}; "
    f"{percentual(r['total_ocorrencias'], total_ranking):.1f}%)"
    for r in top_tres
)

lider = top_conhecidas[0]
lider_anos = {
    int(r["ano"]): int(r["total_ocorrencias"])
    for r in evolucao_top.filter(
        F.col("natureza_apurada") == lider["natureza_apurada"]
    ).collect()
}
variacao_lider = texto_variacao(
    lider_anos.get(ANO_INICIAL, 0), lider_anos.get(ANO_FINAL, 0)
)
resposta_p2 = (
    f"As três naturezas classificadas com maior volume foram {lista_top}. "
    f"A líder, {lider['natureza_apurada']}, apresentou {variacao_lider} entre "
    f"{ANO_INICIAL} e {ANO_FINAL}. A tabela e o gráfico exibem a trajetória anual "
    f"das {TOP_N_CRUZAMENTO} principais."
)
cobertura_p2 = (
    f"O ranking reconcilia {inteiro(total_ranking)} registros na janela; "
    f"{inteiro(desconhecidas)} ({percentual(desconhecidas, total_ranking):.2f}%) "
    "não têm natureza classificada."
)
limite_p2 = (
    "Frequência de uma categoria depende da classificação administrativa e de eventuais "
    "mudanças de preenchimento; não equivale a gravidade nem a risco."
)
reconciliacao_p2 = (
    f"A soma de todas as naturezas é {inteiro(total_ranking)}, igual ao total temporal "
    "incluído na pergunta 1."
)
mostrar_resposta("Resposta da pergunta 2", resposta_p2, cobertura_p2, limite_p2, reconciliacao_p2)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 3 — Como as principais naturezas se distribuem por dia e período?

# COMMAND ----------

distribuicao = spark.sql(f"""
    SELECT
        n.natureza_apurada,
        t.dia_semana_num,
        t.dia_semana_nome,
        p.periodo_dia,
        SUM(f.qtd_ocorrencia) AS total_ocorrencias
    FROM fato_ocorrencia f
    INNER JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
    INNER JOIN dim_periodo_dia p ON f.sk_periodo_dia = p.sk_periodo_dia
    INNER JOIN dim_natureza n ON f.sk_natureza = n.sk_natureza
    INNER JOIN top_naturezas top ON n.natureza_apurada = top.natureza_apurada
    WHERE t.ano BETWEEN {ANO_INICIAL} AND {ANO_FINAL}
    GROUP BY n.natureza_apurada, t.dia_semana_num, t.dia_semana_nome, p.periodo_dia
    ORDER BY n.natureza_apurada, t.dia_semana_num, p.periodo_dia
""")

display(distribuicao)
pdf_distribuicao = distribuicao.toPandas()

ordem_dias = [
    "DOMINGO", "SEGUNDA-FEIRA", "TERÇA-FEIRA", "QUARTA-FEIRA",
    "QUINTA-FEIRA", "SEXTA-FEIRA", "SÁBADO",
]
ordem_periodos = ["MADRUGADA", "MANHÃ", "TARDE", "NOITE", "HORA INCERTA", NAO_INFORMADO]

n_colunas = 2
n_linhas = math.ceil(len(naturezas_top) / n_colunas)
fig, eixos = plt.subplots(n_linhas, n_colunas, figsize=(14, 4.2 * n_linhas), squeeze=False)
for eixo, natureza in zip(eixos.flat, naturezas_top):
    recorte = pdf_distribuicao[pdf_distribuicao["natureza_apurada"] == natureza]
    matriz = recorte.pivot_table(
        index="dia_semana_nome",
        columns="periodo_dia",
        values="total_ocorrencias",
        aggfunc="sum",
        fill_value=0,
    ).reindex(index=ordem_dias, columns=ordem_periodos, fill_value=0)
    imagem = eixo.imshow(matriz.values, aspect="auto", cmap="Blues")
    eixo.set_title(natureza)
    eixo.set_xticks(range(len(ordem_periodos)), ordem_periodos, rotation=35, ha="right", fontsize=7)
    eixo.set_yticks(range(len(ordem_dias)), ordem_dias, fontsize=7)
    fig.colorbar(imagem, ax=eixo, fraction=0.046, pad=0.04)
for eixo in eixos.flat[len(naturezas_top):]:
    eixo.axis("off")
fig.suptitle("Principais naturezas por dia da semana e período do dia", y=1.01)
plt.tight_layout()
plt.show()

total_distribuicao = int(distribuicao.agg(F.sum("total_ocorrencias")).first()[0] or 0)
total_top_esperado = int(evolucao_top.agg(F.sum("total_ocorrencias")).first()[0] or 0)
if total_distribuicao != total_top_esperado:
    raise AssertionError("A distribuição por dia/período não reconcilia com as naturezas selecionadas.")

picos = (
    distribuicao
    .withColumn(
        "posicao",
        F.row_number().over(
            Window.partitionBy("natureza_apurada")
            .orderBy(F.desc("total_ocorrencias"), "dia_semana_num", "periodo_dia")
        ),
    )
    .filter(F.col("posicao") == 1)
    .orderBy(F.desc("total_ocorrencias"))
    .collect()
)
descricao_picos = "; ".join(
    f"{r['natureza_apurada']}: {r['dia_semana_nome']} / {r['periodo_dia']} "
    f"({inteiro(r['total_ocorrencias'])})"
    for r in picos
)

sem_periodo_top = int(
    distribuicao.filter(F.col("periodo_dia") == NAO_INFORMADO)
    .agg(F.sum("total_ocorrencias")).first()[0] or 0
)
resposta_p3 = (
    "As combinações com maior volume para cada uma das naturezas principais foram: "
    f"{descricao_picos}. Os mapas de calor mostram também a distribuição completa, "
    "sem ocultar as demais combinações."
)
cobertura_p3 = (
    f"Foram cruzados {inteiro(total_distribuicao)} registros das "
    f"{len(naturezas_top)} principais naturezas; {inteiro(sem_periodo_top)} "
    f"({percentual(sem_periodo_top, total_distribuicao):.2f}%) têm período não informado."
)
limite_p3 = (
    "A ausência de data impede entrada na janela; período pode vir da fonte ou ser derivado "
    "da hora. Picos descritivos não demonstram causalidade nem orientam policiamento."
)
reconciliacao_p3 = (
    f"A soma da matriz ({inteiro(total_distribuicao)}) é igual ao total anual das mesmas "
    f"{len(naturezas_top)} naturezas ({inteiro(total_top_esperado)})."
)
mostrar_resposta("Resposta da pergunta 3", resposta_p3, cobertura_p3, limite_p3, reconciliacao_p3)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Conclusão geral e autoavaliação baseada na execução

# COMMAND ----------

conclusao = (
    f"O pipeline tornou comparáveis {inteiro(total_com_data_periodo)} registros com data "
    f"entre {ANO_INICIAL} e {ANO_FINAL}. Nesse recorte, o volume anual teve "
    f"{texto_variacao(primeiro, ultimo)} do primeiro para o último ano e atingiu seu "
    f"maior valor em {ano_max}. As categorias de maior frequência foram "
    f"{', '.join(r['natureza_apurada'] for r in top_tres)}, e o cruzamento temporal "
    f"das {len(naturezas_top)} principais foi reconciliado integralmente com a Gold. "
    "O objetivo descritivo foi atendido para os registros com classificação e data "
    "disponíveis; as ausências explicitadas nas coberturas impedem interpretar as "
    "frequências como total real de crimes, relação causal ou recomendação operacional."
)

autoavaliacao = (
    "As três perguntas foram respondidas com tabela, gráfico, cobertura e reconciliação. "
    "As principais dificuldades técnicas foram conciliar quatro schemas, ler XLSX grandes "
    "com recursos limitados e preservar o grão sem eliminar BOs legítimos. O uso de leitura "
    "em lotes, hash integral e chaves determinísticas resolveu essas dificuldades. Permanecem "
    "como limitações uma única fonte, ausência de denominador populacional, dependência do "
    "preenchimento administrativo e falta de validação externa de cada registro. Somente "
    "depois de concluir as evidências deste MVP faz sentido avaliar novas fontes ou anos."
)

displayHTML(f"""
<div style="border:1px solid #bbb;padding:14px;margin-top:14px">
  <h2>Conclusão geral</h2>
  <p>{html.escape(conclusao)}</p>
  <h2>Autoavaliação da execução</h2>
  <p>{html.escape(autoavaliacao)}</p>
</div>
""")

print("Análise concluída: três perguntas respondidas e reconciliadas com a Gold.")
dbutils.notebook.exit("ok")
