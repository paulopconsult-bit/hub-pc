# ==============================================================================
# HUB-PC - ORQUESTRADOR OPERACIONAL E CAPTURA DE TELEMETRIA (LABORATÓRIO)
# ==============================================================================
# Este script gerencia o ciclo de vida local do container (Build Inteligente, Run).
# Ele automatiza o runtime do DOCKER e preenche a pasta 88 de forma cega.
# Gera imagem ou usa atual, depois dispara main-orchestrator e gera os logs na pasta 88.
# ==============================================================================
# Disparar no terminal do host (PowerShell): 
# & D:\cloud\ls\00-scripts\00-cliente\run-lab-container-cliente.ps1
# ==============================================================================
param (
    [switch]$lab = $true
)

Clear-Host
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  HUB-PC - GERENCIADOR DE RUNTIME E TELEMETRIA LAB       " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

$ScriptPath   = "D:\cloud\ls\00-scripts\00-cliente"
$LogDir       = "D:\cloud\ls\88-logs-execution\00-cliente"
$RootDataPath = "D:\cloud\ls"

$TimestampLog = Get-Date -Format "yyyyMMdd-HHmmss"
$LogFile      = "$LogDir\execution-run-$TimestampLog.log"

# ------------------------------------------------------------------------------
# PASSO 1: VERIFICAÇÃO INTELIGENTE DA IMAGEM
# ------------------------------------------------------------------------------
$ImagemExiste = docker images -q hub-pc/microsvc-cliente:latest

if ($null -eq $ImagemExiste -or $ImagemExiste -eq "") {
    Write-Host "[DOCKER BUILD] Compilando imagem pela primeira vez..." -ForegroundColor Yellow
    Set-Location $ScriptPath
    docker build -t hub-pc/microsvc-cliente:latest .
    if ($LASTEXITCODE -ne 0) { Exit $LASTEXITCODE }
} else {
    Write-Host "[DOCKER CACHE] Reutilizando imagem unica do host...`n" -ForegroundColor Green
}

# ------------------------------------------------------------------------------
# PASSO 2: EXECUÇÃO COM ABSTRAÇÃO S.A.L.
# ------------------------------------------------------------------------------
if ($lab) {
    if (-not (Test-Path $LogDir)) { $null = New-Item -ItemType Directory -Path $LogDir -Force }

    # Força encoding UTF-8 para capturar corretamente caracteres especiais do log
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8

    # A PONTE DA ABSTRAÇÃO:
    # O Docker pega o seu drive real "D:\cloud\ls" e joga em "/app/ls" no container.
    # O Python lê ROOT_PROJETO (/app/ls) e monta "/app/ls/77-schema-controls/...".
    # O Linux acha a pasta virtual e acessa o seu Windows na hora!
    docker run --rm `
        --user root `
        -v "${RootDataPath}:/app/ls" `
        hub-pc/microsvc-cliente:latest python main-orchestrator-cliente.py > $LogFile 2>&1

    Write-Host "`n[CONCLUIDO] O container encerrou sua execucao." -ForegroundColor Green
    Write-Host "[STATUS] Verifique o log gerado na pasta 88-logs-execution." -ForegroundColor Green
} else {
    docker run --rm hub-pc/microsvc-cliente:latest
}

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  PROCESSAMENTO CONCLUIDO COM EXIT CODE: $LASTEXITCODE   " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan