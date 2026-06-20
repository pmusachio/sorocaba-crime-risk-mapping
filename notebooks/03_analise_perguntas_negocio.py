# =============================================================================
# MVP Engenharia de Dados — Mapa de Risco Criminal em Sorocaba
# Análise / Solução do Problema — respostas às 6 perguntas de negócio do Objetivo
#
# Nota de reaproveitamento: notebook estruturado para também servir de ponto de
# partida da EDA do próximo MVP (ML / predição). Funções e DataFrames foram
# nomeados pensando em reuso.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (10, 5)

SCHEMA = "sorocaba_seguranca"
spark.sql(f"USE {SCHEMA}")  # permite consultar fato_ocorrencia, dim_* diretamente

print("Tabelas disponíveis:")
spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 1 — Concentração por bairro e estabilidade ao longo do tempo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT l.bairro, COUNT(*) AS total_ocorrencias
# MAGIC FROM fato_ocorrencia f
# MAGIC JOIN dim_local l ON f.id_local = l.id_local
# MAGIC WHERE l.bairro <> 'NÃO INFORMADO'
# MAGIC GROUP BY l.bairro
# MAGIC ORDER BY total_ocorrencias DESC
# MAGIC LIMIT 15

# COMMAND ----------

df_bairro_ano = spark.sql("""
    SELECT d.ano, l.bairro, COUNT(*) AS total_ocorrencias
    FROM fato_ocorrencia f
    JOIN dim_data d  ON f.id_data  = d.id_data
    JOIN dim_local l ON f.id_local = l.id_local
    WHERE l.bairro <> 'NÃO INFORMADO'
    GROUP BY d.ano, l.bairro
""").toPandas()

top10_bairros = (
    df_bairro_ano.groupby("bairro")["total_ocorrencias"].sum()
    .sort_values(ascending=False).head(10).index.tolist()
)
df_top = df_bairro_ano[df_bairro_ano["bairro"].isin(top10_bairros)]

pivot_ranking = df_top.pivot(index="bairro", columns="ano", values="total_ocorrencias").fillna(0)
display(pivot_ranking)  # display() é nativo do Databricks

plt.figure(figsize=(12, 6))
sns.lineplot(data=df_top, x="ano", y="total_ocorrencias", hue="bairro", marker="o")
plt.title("Top 10 bairros de Sorocaba — evolução do volume por ano")
plt.xlabel("Ano"); plt.ylabel("Total de ocorrências")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar com os números):**
# MAGIC - 3–5 bairros consistentemente no topo = concentração estrutural (não pontual).
# MAGIC - Mudanças abruptas de posição entre anos: cruzar com a qualidade antes de
# MAGIC   afirmar causalidade (pode ser cobertura/registro, não dinâmica criminal).
# MAGIC - 2026 é parcial (Jan-Abr): quedas no último ano podem ser artefato de cobertura.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 2 — Sazonalidade (dia da semana, mês, horário)

# COMMAND ----------

df_dia = spark.sql("""
    SELECT d.dia_semana_nome, d.dia_semana_num, COUNT(*) AS total
    FROM fato_ocorrencia f JOIN dim_data d ON f.id_data = d.id_data
    GROUP BY d.dia_semana_nome, d.dia_semana_num
    ORDER BY d.dia_semana_num
""").toPandas()

df_mes = spark.sql("""
    SELECT d.mes, COUNT(*) AS total
    FROM fato_ocorrencia f JOIN dim_data d ON f.id_data = d.id_data
    GROUP BY d.mes ORDER BY d.mes
""").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(15, 5))
sns.barplot(data=df_dia, x="dia_semana_nome", y="total", ax=axes[0], color="#bd93f9")
axes[0].set_title("Ocorrências por dia da semana"); axes[0].tick_params(axis="x", rotation=45)
sns.barplot(data=df_mes, x="mes", y="total", ax=axes[1], color="#50fa7b")
axes[1].set_title("Ocorrências por mês")
plt.tight_layout(); plt.show()

# --- Horário: reportar a cobertura ANTES de interpretar ---
total_fato = spark.table("fato_ocorrencia").count()
hora_preenchida = spark.sql(
    "SELECT COUNT(*) AS n FROM fato_ocorrencia WHERE hora_ocorrencia_bo IS NOT NULL"
).collect()[0]["n"]
pct_cobertura = round(100 * hora_preenchida / total_fato, 2)
print(f"Cobertura de hora_ocorrencia_bo: {hora_preenchida:,}/{total_fato:,} = {pct_cobertura}%")

# Distribuição por faixa horária (só onde há hora) — usa desc_periodo, que é completo
df_periodo = spark.sql("""
    SELECT desc_periodo, COUNT(*) AS total
    FROM fato_ocorrencia
    WHERE desc_periodo IS NOT NULL
    GROUP BY desc_periodo ORDER BY total DESC
""").toPandas()
plt.figure()
sns.barplot(data=df_periodo, x="desc_periodo", y="total", color="#8be9fd")
plt.title("Ocorrências por período do dia (desc_periodo)")
plt.xticks(rotation=30); plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar):**
# MAGIC - Reportar a cobertura de `hora_ocorrencia_bo` (impressa acima) ANTES de
# MAGIC   concluir sobre horário — parte da ausência é vedação proposital (não aleatória),
# MAGIC   o que pode enviesar. `desc_periodo` é mais completo e serve de proxy.
# MAGIC - Comparar picos por dia/mês com calendário (feriados, férias) se quiser aprofundar.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 3 — Tipos de ocorrência predominantes por bairro

# COMMAND ----------

df_tipo_bairro = spark.sql("""
    SELECT l.bairro, t.natureza_apurada, COUNT(*) AS total
    FROM fato_ocorrencia f
    JOIN dim_local l            ON f.id_local = l.id_local
    JOIN dim_tipo_ocorrencia t  ON f.id_tipo_ocorrencia = t.id_tipo_ocorrencia
    WHERE l.bairro <> 'NÃO INFORMADO'
    GROUP BY l.bairro, t.natureza_apurada
""").toPandas()

# Reaproveita os 5 bairros mais críticos da pergunta 1
df_heat = df_tipo_bairro[df_tipo_bairro["bairro"].isin(top10_bairros[:5])]
pivot_heat = df_heat.pivot_table(index="bairro", columns="natureza_apurada", values="total", fill_value=0)

plt.figure(figsize=(14, 6))
sns.heatmap(pivot_heat, cmap="rocket_r", cbar_kws={"label": "Total de ocorrências"})
plt.title("Tipos de ocorrência nos 5 bairros mais críticos")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar):** o tipo predominante muda entre bairros (dinâmicas
# MAGIC distintas) ou um único tipo domina a cidade toda? Relacionar com a pergunta 5.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 4 — Tendência ao longo dos anos (2026 parcial)
# MAGIC
# MAGIC Nota: analisamos por ano da **ocorrência** (`dim_data.ano`). A estatística
# MAGIC oficial da SSP-SP usa `ano_estatistica` (ano do registro); os dois podem
# MAGIC divergir para fatos registrados em ano posterior ao do acontecimento.

# COMMAND ----------

df_tend = spark.sql("""
    SELECT d.ano, d.mes, COUNT(*) AS total
    FROM fato_ocorrencia f JOIN dim_data d ON f.id_data = d.id_data
    GROUP BY d.ano, d.mes ORDER BY d.ano, d.mes
""").toPandas()

# Comparação justa: Jan-Abr de cada ano (2026 só tem esses meses)
df_jan_abr = df_tend[df_tend["mes"] <= 4].groupby("ano")["total"].sum().reset_index()
df_jan_abr.columns = ["ano", "total_jan_abr"]
print("Comparação justa Jan-Abr por ano (evita viés de 2026 parcial):")
print(df_jan_abr)

plt.figure()
sns.lineplot(data=df_jan_abr, x="ano", y="total_jan_abr", marker="o")
plt.title("Ocorrências Jan-Abr por ano (comparação normalizada)"); plt.ylabel("Total Jan-Abr")
plt.tight_layout(); plt.show()

# Visão complementar: total anual, com 2026 destacado como parcial
df_ano = df_tend.groupby("ano")["total"].sum().reset_index()
print("\nTotal anual (2026 é parcial, não comparável diretamente):")
print(df_ano)
plt.figure()
cores = ["#6272a4"] * (len(df_ano) - 1) + ["#ff5555"]
sns.barplot(data=df_ano, x="ano", y="total", hue="ano", palette=cores, legend=False)
plt.title("Total por ano da ocorrência (vermelho = 2026, parcial)")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar):** a leitura de tendência deve se basear na comparação
# MAGIC Jan-Abr entre anos, não no total bruto (2026 parcial). Opcional: projeção
# MAGIC simples (regra de três) só como estimativa ilustrativa, deixando claro que é projeção.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 5 — Tipo de local × tipo de ocorrência (somente 2025+)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.descr_tipolocal, t.natureza_apurada, COUNT(*) AS total_ocorrencias
# MAGIC FROM fato_ocorrencia f
# MAGIC JOIN dim_tipo_ocorrencia t ON f.id_tipo_ocorrencia = t.id_tipo_ocorrencia
# MAGIC WHERE t.descr_tipolocal <> 'NÃO INFORMADO'
# MAGIC GROUP BY t.descr_tipolocal, t.natureza_apurada
# MAGIC ORDER BY t.descr_tipolocal, total_ocorrencias DESC

# COMMAND ----------

df_local_tipo = spark.sql("""
    SELECT t.descr_tipolocal, t.natureza_apurada, COUNT(*) AS total
    FROM fato_ocorrencia f
    JOIN dim_tipo_ocorrencia t ON f.id_tipo_ocorrencia = t.id_tipo_ocorrencia
    WHERE t.descr_tipolocal <> 'NÃO INFORMADO'
    GROUP BY t.descr_tipolocal, t.natureza_apurada
""").toPandas()

print(f"Registros com descr_tipolocal preenchido (2025+): {df_local_tipo['total'].sum():,}")
pivot_lt = df_local_tipo.pivot_table(index="descr_tipolocal", columns="natureza_apurada",
                                     values="total", fill_value=0)
plt.figure(figsize=(14, 8))
sns.heatmap(pivot_lt, cmap="mako", cbar_kws={"label": "Total de ocorrências"})
plt.title("Tipo de local × tipo de ocorrência (2025-2026)")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar):** reportar que cobre só 2025-2026 (limitação da fonte).
# MAGIC Procurar combinações dominantes (ex.: "Via Pública" + furto/roubo).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 6 — Correlação espacial entre tipos (exploratória)

# COMMAND ----------

pivot_corr = df_tipo_bairro.pivot_table(index="bairro", columns="natureza_apurada",
                                        values="total", fill_value=0)
# Mantém só bairros com volume mínimo, p/ evitar correlação espúria
volume = pivot_corr.sum(axis=1)
pivot_corr = pivot_corr.loc[volume[volume >= 20].index]
matriz = pivot_corr.corr()

plt.figure(figsize=(10, 8))
sns.heatmap(matriz, cmap="coolwarm", center=0)
plt.title("Correlação entre tipos de ocorrência (perfil por bairro, vol >= 20)")
plt.tight_layout(); plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC **Discussão (completar):**
# MAGIC - É correlação entre PERFIS de bairro (não causal).
# MAGIC - O limiar de volume (20) é arbitrário — documentar e, se houver tempo, testar sensibilidade.
# MAGIC - Resultado pouco conclusivo é resposta válida — registrar na Autoavaliação.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Síntese geral (completar ao final, conectando as 6 respostas)
# MAGIC
# MAGIC 1. Retomar o problema original (falta de ferramenta granular de visualização).
# MAGIC 2. Resumir os 2-3 achados mais fortes (concentração geográfica, sazonalidade).
# MAGIC 3. Reconhecer limitações (cobertura de horário; `descr_tipolocal` só 2025+;
# MAGIC    2026 parcial; pergunta 6 exploratória).
# MAGIC 4. Conectar ao próximo MVP de ML: variáveis com maior potencial preditivo
# MAGIC    (bairro, dia da semana, tipo de local, período).
