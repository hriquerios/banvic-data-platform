#!/usr/bin/env bash
# Dispara manualmente a DAG banvic_erp_ingestao_raw e acompanha até terminar.
set -euo pipefail

CONTEXTO="${CONTEXTO:-kind-banvic}"
NAMESPACE="${NAMESPACE:-banvic-orquestracao}"
DAG_ID="banvic_erp_ingestao_raw"
RUN_ID="manual__$(date -u +%Y%m%dT%H%M%S)"

airflow_cli() { kubectl --context "$CONTEXTO" exec -n "$NAMESPACE" deploy/airflow-scheduler -c scheduler -- airflow "$@"; }

airflow_cli dags unpause "$DAG_ID" >/dev/null
airflow_cli dags trigger "$DAG_ID" --run-id "$RUN_ID" >/dev/null
echo "Run disparado: $RUN_ID (acompanhe em http://localhost:8080)"

for _ in $(seq 1 80); do
  sleep 15
  ESTADO="$(airflow_cli dags list-runs "$DAG_ID" --output json \
    | python3 -c "import json,sys; print(next((r['state'] for r in json.load(sys.stdin) if r['run_id']=='$RUN_ID'), 'queued'))")"
  echo "[$(date +%H:%M:%S)] estado: $ESTADO"
  case "$ESTADO" in
    success) echo "DAG concluída com sucesso."; exit 0 ;;
    failed)  echo "DAG falhou. Veja os logs na UI do Airflow."; exit 1 ;;
  esac
done
echo "Tempo limite atingido."; exit 1
