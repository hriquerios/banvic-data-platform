<#
.SYNOPSIS
    Sobe todo o ambiente do BanVic (Kind + PostgreSQL + Airflow + imagens) com Terraform.

.DESCRIPTION
    Wrapper fino sobre "terraform init/apply". Toda a infraestrutura é declarada
    em terraform/; este script só valida pré-requisitos e executa o Terraform.
    Pode ser executado várias vezes: o Terraform aplica apenas as diferenças.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\subir_ambiente.ps1
    powershell -ExecutionPolicy Bypass -File .\scripts\subir_ambiente.ps1 -AutoApprove
#>
[CmdletBinding()]
param(
    [switch]$AutoApprove
)

$ErrorActionPreference = "Stop"
$Raiz = Split-Path -Parent $PSScriptRoot
$Terraform = Join-Path $Raiz "terraform"

function Write-Passo([string]$Mensagem) {
    Write-Host ""
    Write-Host "==> $Mensagem" -ForegroundColor Cyan
}

function Assert-Comando([string]$Comando) {
    if (-not (Get-Command $Comando -ErrorAction SilentlyContinue)) {
        throw "Pré-requisito não encontrado no PATH: $Comando"
    }
}

Write-Passo "Verificando pré-requisitos"
foreach ($cmd in @("docker", "kind", "kubectl", "terraform")) { Assert-Comando $cmd }
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "O Docker não está respondendo. Abra o Docker Desktop e tente novamente." }
Write-Host "docker, kind, kubectl e terraform OK" -ForegroundColor Green

Write-Passo "Verificando os 7 arquivos de origem"
$Dados = Join-Path $Raiz "data\banvic_erp"
$Tabelas = @("agencias", "clientes", "colaboradores", "colaborador_agencia", "contas", "propostas_credito", "transacoes")
foreach ($tabela in $Tabelas) {
    $arquivo = Join-Path $Dados "$tabela.csv"
    if (-not (Test-Path $arquivo)) { throw "Arquivo ausente: $arquivo" }
}
Write-Host "7/7 arquivos encontrados em data\banvic_erp" -ForegroundColor Green

Write-Passo "terraform init"
terraform "-chdir=$Terraform" init -input=false
if ($LASTEXITCODE -ne 0) { throw "terraform init falhou" }

Write-Passo "terraform apply (cluster, banco, imagens e Airflow - a primeira execução leva alguns minutos)"
$tfArgs = @("-chdir=$Terraform", "apply", "-input=false")
if ($AutoApprove) { $tfArgs += "-auto-approve" }
terraform @tfArgs
if ($LASTEXITCODE -ne 0) { throw "terraform apply falhou" }

Write-Passo "Ambiente pronto"
terraform "-chdir=$Terraform" output
Write-Host ""
Write-Host "UI do Airflow : http://localhost:8080  (usuário: admin)" -ForegroundColor Green
Write-Host "Senha do admin: terraform -chdir=terraform output -raw airflow_admin_password"
Write-Host "Próximo passo : .\scripts\executar_dag.ps1  e depois  .\scripts\verificar_ambiente.ps1"
