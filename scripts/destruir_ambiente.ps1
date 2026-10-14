<#
.SYNOPSIS
    Remove todo o ambiente (cluster Kind e recursos) criado pelo Terraform.
#>
[CmdletBinding()]
param(
    [switch]$AutoApprove
)

$ErrorActionPreference = "Stop"
$Terraform = Join-Path (Split-Path -Parent $PSScriptRoot) "terraform"

$tfArgs = @("-chdir=$Terraform", "destroy", "-input=false")
if ($AutoApprove) { $tfArgs += "-auto-approve" }
terraform @tfArgs
if ($LASTEXITCODE -ne 0) { throw "terraform destroy falhou" }
Write-Host "Ambiente removido." -ForegroundColor Green
