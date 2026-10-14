<#
.SYNOPSIS
    Dispara manualmente a DAG banvic_erp_ingestao_raw e acompanha até terminar.
#>
[CmdletBinding()]
param(
    [string]$Contexto = "kind-banvic",
    [string]$Namespace = "banvic-orquestracao",
    [int]$TimeoutMinutos = 20
)

$ErrorActionPreference = "Stop"
$DagId = "banvic_erp_ingestao_raw"

# Função simples (sem param): repassa todos os argumentos via $args.
function Invoke-Airflow {
    kubectl --context $Contexto exec -n $Namespace deploy/airflow-scheduler -c scheduler -- airflow @args
}

Write-Host "==> Garantindo que a DAG está ativa" -ForegroundColor Cyan
Invoke-Airflow dags unpause $DagId | Out-Null

Write-Host "==> Disparando $DagId" -ForegroundColor Cyan
$runId = "manual__" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")
Invoke-Airflow dags trigger $DagId --run-id $runId | Out-Null
Write-Host "Run: $runId  (acompanhe também em http://localhost:8080)"

$limite = (Get-Date).AddMinutes($TimeoutMinutos)
do {
    Start-Sleep -Seconds 15
    $runs = Invoke-Airflow dags list-runs $DagId --output json | ConvertFrom-Json
    $estado = ($runs | Where-Object { $_.run_id -eq $runId }).state
    Write-Host ("[{0:HH:mm:ss}] estado: {1}" -f (Get-Date), $estado)
} while ($estado -in @("queued", "running", $null) -and (Get-Date) -lt $limite)

if ($estado -eq "success") {
    Write-Host "DAG concluída com sucesso." -ForegroundColor Green
}
else {
    Write-Host "DAG terminou com estado '$estado'. Veja os logs na UI do Airflow." -ForegroundColor Red
    exit 1
}
