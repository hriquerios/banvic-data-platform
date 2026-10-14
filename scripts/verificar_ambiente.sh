#!/usr/bin/env bash
# Verifica o ambiente sem alterar nada: pods, serviços, volumes, DAG e dados no PostgreSQL.
set -euo pipefail

CTX="${CONTEXTO:-kind-banvic}"
NS_DADOS="banvic-dados"
NS_ORQ="banvic-orquestracao"
passo() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }

passo "Pods";                       kubectl --context "$CTX" get pods -n "$NS_DADOS" -o wide; kubectl --context "$CTX" get pods -n "$NS_ORQ" -o wide
passo "Deployments, Services e PVCs"; kubectl --context "$CTX" get deploy,svc,pvc -n "$NS_DADOS"; kubectl --context "$CTX" get deploy,svc,pvc -n "$NS_ORQ"
passo "Secrets (somente nomes)";    kubectl --context "$CTX" get secrets -n "$NS_DADOS"; kubectl --context "$CTX" get secrets -n "$NS_ORQ"
passo "DAG registrada";             kubectl --context "$CTX" exec -n "$NS_ORQ" deploy/airflow-scheduler -c scheduler -- airflow dags list | grep banvic
passo "Últimas execuções";          kubectl --context "$CTX" exec -n "$NS_ORQ" deploy/airflow-scheduler -c scheduler -- airflow dags list-runs banvic_erp_ingestao_raw

passo "Dados no PostgreSQL (schema raw)"
kubectl --context "$CTX" exec -i -n "$NS_DADOS" deploy/postgres -- sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < "$(dirname "${BASH_SOURCE[0]}")/sql/resumo_raw.sql"

echo; echo "Checagem finalizada."
