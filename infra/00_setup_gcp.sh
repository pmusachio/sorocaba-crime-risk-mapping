#!/usr/bin/env bash
# =============================================================================
# MVP Engenharia de Dados — Criminalidade em Sorocaba
# Etapa 0: provisionamento do ambiente na Google Cloud Platform
#
# Cria a infraestrutura que hospeda os dois repositórios do ambiente de BI
# descritos na Aula 1 (Modelagem de DW) e na Aula 3 (Data Lakes e ETL):
#
#   - Data lake  -> bucket no Cloud Storage, com uma zona para os dados em
#                   formato nativo (bruta) e outra para os dados preparados
#                   para processamento distribuído (preparada);
#   - Data warehouse -> BigQuery, onde é implementado o esquema estrela
#                   (abordagem ROLAP: cada fato e cada dimensão em uma tabela).
#
# O script é idempotente: pode ser executado novamente sem duplicar recursos.
#
# Uso:
#   export PROJETO_ID="seu-projeto-gcp"
#   bash infra/00_setup_gcp.sh
# =============================================================================
set -euo pipefail

PROJETO_ID="${PROJETO_ID:-}"
REGIAO="${REGIAO:-southamerica-east1}"
BUCKET="${BUCKET:-gs://${PROJETO_ID}-datalake}"

if [[ -z "${PROJETO_ID}" ]]; then
  echo "ERRO: defina a variável PROJETO_ID antes de executar." >&2
  echo '       export PROJETO_ID="seu-projeto-gcp"' >&2
  exit 1
fi

echo "==> Projeto ...: ${PROJETO_ID}"
echo "==> Região ....: ${REGIAO}"
echo "==> Bucket ....: ${BUCKET}"
echo

# -----------------------------------------------------------------------------
# 1. Projeto corrente e APIs necessárias
# -----------------------------------------------------------------------------
echo "==> [1/5] Configurando projeto corrente e habilitando APIs..."
gcloud config set project "${PROJETO_ID}" --quiet

gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  dataproc.googleapis.com \
  compute.googleapis.com \
  --project "${PROJETO_ID}" --quiet

# -----------------------------------------------------------------------------
# 2. Data lake: bucket e zonas
#
# A zona bruta guarda o arquivo exatamente como publicado pela fonte ("os dados
# são armazenados em seu formato nativo", Aula 3). A zona preparada guarda o
# mesmo conteúdo em Parquet, formato colunar lido de forma nativa e distribuída
# pelo Spark. Nenhum dado é descartado entre as duas zonas.
# -----------------------------------------------------------------------------
echo "==> [2/5] Criando o bucket do data lake e suas zonas..."
if gcloud storage buckets describe "${BUCKET}" --project "${PROJETO_ID}" >/dev/null 2>&1; then
  echo "    bucket já existe — mantido"
else
  gcloud storage buckets create "${BUCKET}" \
    --project "${PROJETO_ID}" \
    --location "${REGIAO}" \
    --uniform-bucket-level-access
fi

# Marcadores das zonas (o Cloud Storage não tem diretórios de verdade; os
# prefixos passam a existir quando o primeiro objeto é gravado).
for ZONA in bruta preparada temp-dataproc; do
  echo "zona ${ZONA} do data lake do MVP" \
    | gcloud storage cp - "${BUCKET}/${ZONA}/_zona.txt" --quiet
done

# -----------------------------------------------------------------------------
# 3. Data warehouse: datasets do BigQuery
#
#   stg       -> área de staging: saída do job Spark, ainda em uma única tabela
#                desnormalizada, antes da modelagem dimensional
#   dw        -> o data warehouse propriamente dito (esquema estrela)
#   qualidade -> resultados persistidos da análise de qualidade de dados
# -----------------------------------------------------------------------------
echo "==> [3/5] Criando os datasets do BigQuery..."
criar_dataset() {
  local nome="$1" descricao="$2"
  if bq --project_id="${PROJETO_ID}" show --dataset "${PROJETO_ID}:${nome}" >/dev/null 2>&1; then
    echo "    dataset ${nome} já existe — mantido"
  else
    bq --project_id="${PROJETO_ID}" mk \
      --dataset \
      --location="${REGIAO}" \
      --description="${descricao}" \
      "${PROJETO_ID}:${nome}"
  fi
}

criar_dataset "stg" "Área de staging: dados de Sorocaba conformados pelo job Spark, antes da modelagem dimensional."
criar_dataset "dw"  "Data warehouse dimensional (esquema estrela) das ocorrências criminais registradas em Sorocaba."
criar_dataset "qualidade" "Resultados persistidos da análise de qualidade de dados, atributo a atributo."

# -----------------------------------------------------------------------------
# 4. Rede para o Dataproc Serverless
#
# O Dataproc Serverless executa sem IP externo e precisa alcançar as APIs do
# Google pela rede interna. Sem o Acesso privado ao Google habilitado na
# sub-rede, o job falha na inicialização.
# -----------------------------------------------------------------------------
echo "==> [4/5] Habilitando Acesso privado ao Google na sub-rede default..."
gcloud compute networks subnets update default \
  --region "${REGIAO}" \
  --enable-private-ip-google-access \
  --project "${PROJETO_ID}" --quiet

# -----------------------------------------------------------------------------
# 5. Conta de serviço usada pelo job Spark
# -----------------------------------------------------------------------------
echo "==> [5/5] Criando a conta de serviço do pipeline..."
SA="etl-criminalidade"
SA_EMAIL="${SA}@${PROJETO_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe "${SA_EMAIL}" --project "${PROJETO_ID}" >/dev/null 2>&1; then
  echo "    conta de serviço já existe — mantida"
else
  gcloud iam service-accounts create "${SA}" \
    --display-name "ETL do MVP de criminalidade em Sorocaba" \
    --project "${PROJETO_ID}"
fi

for PAPEL in roles/dataproc.worker roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "${PROJETO_ID}" \
    --member "serviceAccount:${SA_EMAIL}" \
    --role "${PAPEL}" \
    --condition=None --quiet >/dev/null
done
echo "    papéis concedidos a ${SA_EMAIL}"

echo
echo "==> Ambiente pronto."
echo "    Data lake ........: ${BUCKET}/{bruta,preparada}"
echo "    Data warehouse ...: ${PROJETO_ID}:{stg,dw,qualidade} (${REGIAO})"
echo "    Conta de serviço .: ${SA_EMAIL}"
echo
echo "    Verificação:"
echo "      gcloud storage ls ${BUCKET}"
echo "      bq ls --project_id=${PROJETO_ID}"
