<#
.SYNOPSIS
    Verifica o ambiente sem alterar nada: pods, serviços, volumes, DAG e dados no PostgreSQL.
#>
[CmdletBinding()]
param(
    [string]$Contexto = "kind-banvic",
    [string]$NsDados = "banvic-dados",
    [string]$NsOrquestracao = "banvic-orquestracao"
)

$ErrorActionPreference = "Stop"

function Write-Passo([string]$Mensagem) {
    Write-Host ""
    Write-Host "==> $Mensagem" -ForegroundColor Cyan
}

Write-Passo "Pods"
kubectl --context $Contexto get pods -n $NsDados -o wide
kubectl --context $Contexto get pods -n $NsOrquestracao -o wide

Write-Passo "Deployments, Services e PVCs"
kubectl --context $Contexto get deploy,svc,pvc -n $NsDados
kubectl --context $Contexto get deploy,svc,pvc -n $NsOrquestracao

Write-Passo "Secrets (somente nomes)"
kubectl --context $Contexto get secrets -n $NsDados
kubectl --context $Contexto get secrets -n $NsOrquestracao

Write-Passo "DAG registrada no Airflow"
kubectl --context $Contexto exec -n $NsOrquestracao deploy/airflow-scheduler -c scheduler -- airflow dags list | Select-String "banvic"

Write-Passo "Últimas execuções"
kubectl --context $Contexto exec -n $NsOrquestracao deploy/airflow-scheduler -c scheduler -- airflow dags list-runs banvic_erp_ingestao_raw

Write-Passo "Dados no PostgreSQL (schema raw)"
# Tabelas descobertas dinamicamente: volume, nº de colunas e tipos encontrados.
# O SQL vai pelo stdin (evita problemas de aspas do PowerShell) e roda com as
# credenciais que já existem dentro do pod do PostgreSQL.
Get-Content (Join-Path $PSScriptRoot "sql\resumo_raw.sql") -Raw |
    kubectl --context $Contexto exec -i -n $NsDados deploy/postgres -- sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'

Write-Host ""
Write-Host "Checagem finalizada." -ForegroundColor Green
