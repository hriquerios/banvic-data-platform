#!/usr/bin/env bash
# Sobe todo o ambiente do BanVic (Kind + PostgreSQL + Airflow + imagens) com Terraform.
# Uso: ./scripts/subir_ambiente.sh [-auto-approve]
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF="$RAIZ/terraform"
passo() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }

passo "Verificando pré-requisitos"
for cmd in docker kind kubectl terraform; do
  command -v "$cmd" >/dev/null || { echo "Pré-requisito não encontrado no PATH: $cmd"; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "O Docker não está respondendo."; exit 1; }

passo "Verificando os 7 arquivos de origem"
for t in agencias clientes colaboradores colaborador_agencia contas propostas_credito transacoes; do
  [[ -f "$RAIZ/data/banvic_erp/$t.csv" ]] || { echo "Arquivo ausente: data/banvic_erp/$t.csv"; exit 1; }
done
echo "7/7 arquivos encontrados"

passo "terraform init"
terraform -chdir="$TF" init -input=false

passo "terraform apply (a primeira execução leva alguns minutos)"
terraform -chdir="$TF" apply -input=false "$@"

passo "Ambiente pronto"
terraform -chdir="$TF" output
echo
echo "UI do Airflow : http://localhost:8080  (usuário: admin)"
echo "Senha do admin: terraform -chdir=terraform output -raw airflow_admin_password"
echo "Próximo passo : ./scripts/executar_dag.sh  e depois  ./scripts/verificar_ambiente.sh"
