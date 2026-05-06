#requires -Version 5.1
<#
.SYNOPSIS
    Catch up the NBA ETL pipeline by backfilling any missed days.

.DESCRIPTION
    Auto-detects the latest game_date partition under ./out/processed/ and
    runs an Airflow backfill for every day from (latest + 1) through yesterday.
    If you're already current, it exits cleanly without doing any work.

    Idempotent: the DAG uses dynamic partition overwrite, so re-running a day
    that already exists cleanly replaces that partition with fresh data.

    Requires Docker Desktop to be running. The script will bring up the
    Airflow stack if it's not already running (no-op if it is).

    With -CleanStale, the script first detects any DagRuns left in 'running'
    state from prior crashed sessions and marks them failed via the Airflow
    REST API before submitting new work. This unblocks the recurring
    "max_active_runs limit has been reached" indefinite-poll failure mode
    without requiring manual UI clicks.

.EXAMPLE
    .\scripts\catch_up.ps1
    Detect the gap and backfill if needed. Run this each morning during
    the season.

.EXAMPLE
    .\scripts\catch_up.ps1 -CleanStale
    Same as above, but auto-mark any leftover 'running' DagRuns as failed
    before submitting work. Use this when a previous backfill crashed
    abruptly (Ctrl+C, Docker died, scheduler bounce mid-run).

.EXAMPLE
    .\scripts\catch_up.ps1 -From 2026-04-18 -To 2026-05-15
    Override auto-detection and force a specific date range. Useful for
    cold starts or backfilling a wider window than just yesterday.

.NOTES
    Tested against Windows PowerShell 5.1 + Docker Desktop on Windows 10/11.
    Run from the repo root.

    -CleanStale assumes default Airflow REST API auth (admin/admin) and the
    webserver listening on http://localhost:8080. If you've changed those,
    edit $AirflowApi and $AirflowUser/$AirflowPass below.
#>

[CmdletBinding()]
param(
    [string]$From = "",
    [string]$To = "",
    [switch]$CleanStale
)

$ErrorActionPreference = "Stop"

$ProcessedDir = "./out/processed/nba/team_game_stats"
$ComposeFile = "infra/docker-compose.yml"
$DagId = "nba_etl_pipeline"
$PlayoffsStart = "2026-04-18"

# Airflow REST API target (used only by -CleanStale).
$AirflowApi = "http://localhost:8080/api/v1"
$AirflowUser = "admin"
$AirflowPass = "admin"

function Read-LatestGameDate {
    if (-not (Test-Path $ProcessedDir)) {
        return $null
    }
    $dirs = Get-ChildItem -Path $ProcessedDir -Filter "game_date=*" -Recurse -Directory -ErrorAction SilentlyContinue
    if (-not $dirs -or $dirs.Count -eq 0) {
        return $null
    }
    $dates = $dirs | ForEach-Object { $_.Name -replace 'game_date=', '' } | Sort-Object -Unique
    $newest = $dates | Select-Object -Last 1
    return [datetime]::ParseExact($newest, 'yyyy-MM-dd', $null)
}

function Get-AirflowAuthHeader {
    $pair = "${AirflowUser}:${AirflowPass}"
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
    return "Basic $([Convert]::ToBase64String($bytes))"
}

function Get-StaleRunningRuns {
    # Use Airflow's CLI with JSON output to enumerate DagRuns currently in
    # 'running' state. Returns an array of objects with run_id and
    # execution_date, or an empty array if none / on error.
    $raw = docker compose -f $ComposeFile exec -T airflow-scheduler `
        airflow dags list-runs -d $DagId --state running -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $raw) {
        return @()
    }
    try {
        $parsed = $raw | ConvertFrom-Json
    } catch {
        return @()
    }
    if ($null -eq $parsed) { return @() }
    # Force array shape even when there's a single result.
    return @($parsed)
}

function Set-DagRunFailed {
    param(
        [Parameter(Mandatory=$true)][string]$RunId
    )
    $auth = Get-AirflowAuthHeader
    $uri = "$AirflowApi/dags/$DagId/dagRuns/$RunId"
    try {
        Invoke-RestMethod -Method Patch -Uri $uri `
            -Headers @{ Authorization = $auth } `
            -ContentType "application/json" `
            -Body '{"state":"failed"}' | Out-Null
        return $true
    } catch {
        Write-Host "    Failed to mark $RunId as failed: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-StaleRunCleanup {
    # Returns $true if it's safe to proceed with the backfill, $false if the
    # caller should bail out (stale runs found and -CleanStale not set).
    $stale = Get-StaleRunningRuns
    if ($null -eq $stale -or $stale.Count -eq 0) {
        return $true
    }

    Write-Host ""
    Write-Host "Found $($stale.Count) stale 'running' DAG run(s):" -ForegroundColor Yellow
    foreach ($r in $stale) {
        $exec = if ($r.execution_date) { $r.execution_date } else { "?" }
        Write-Host "  $($r.run_id)  (logical_date: $exec)" -ForegroundColor Yellow
    }

    if (-not $CleanStale) {
        Write-Host ""
        Write-Host "These will block the backfill (DAG has max_active_runs=1)." -ForegroundColor Yellow
        Write-Host "Re-run with -CleanStale to mark them failed via the Airflow REST API:" -ForegroundColor Yellow
        Write-Host "  .\scripts\catch_up.ps1 -CleanStale" -ForegroundColor Yellow
        Write-Host "Or mark them failed manually in the UI: http://localhost:8080" -ForegroundColor Yellow
        return $false
    }

    Write-Host ""
    Write-Host "Marking stale runs failed via Airflow REST API (-CleanStale)..."
    $cleared = 0
    foreach ($r in $stale) {
        if (Set-DagRunFailed -RunId $r.run_id) {
            Write-Host "  marked failed: $($r.run_id)" -ForegroundColor Green
            $cleared++
        }
    }
    Write-Host "Cleared $cleared of $($stale.Count) stale run(s)." -ForegroundColor Green
    return $true
}

# --- Resolve end date ---
if ($To) {
    $end = [datetime]::ParseExact($To, 'yyyy-MM-dd', $null)
} else {
    $end = (Get-Date).Date.AddDays(-1)
}
$endStr = $end.ToString('yyyy-MM-dd')

# --- Resolve start date ---
if ($From) {
    $start = [datetime]::ParseExact($From, 'yyyy-MM-dd', $null)
    Write-Host "Using explicit -From: $From"
} else {
    $latest = Read-LatestGameDate
    if ($null -eq $latest) {
        Write-Host "No processed/ data found yet (cold start)." -ForegroundColor Yellow
        Write-Host "Defaulting -From to playoffs start: $PlayoffsStart"
        $start = [datetime]::ParseExact($PlayoffsStart, 'yyyy-MM-dd', $null)
    } else {
        $latestStr = $latest.ToString('yyyy-MM-dd')
        Write-Host "Latest game_date in processed/: $latestStr"
        $start = $latest.AddDays(1)
    }
}
$startStr = $start.ToString('yyyy-MM-dd')

Write-Host "Target end (yesterday by default): $endStr"

# --- Short-circuit if already current ---
if ($start -gt $end) {
    Write-Host ""
    Write-Host "Already up to date - nothing to backfill." -ForegroundColor Green
    exit 0
}

$gapDays = ($end - $start).Days + 1
Write-Host ""
Write-Host "Backfill range: $startStr -> $endStr  [$gapDays days]" -ForegroundColor Cyan

# --- Bring up the stack (idempotent) ---
Write-Host ""
Write-Host "Ensuring Airflow stack is running..."
docker compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Failed to bring up Airflow stack. Is Docker Desktop running?" -ForegroundColor Red
    exit $LASTEXITCODE
}

# --- Detect and (optionally) clear stale running DagRuns ---
$ok = Invoke-StaleRunCleanup
if (-not $ok) {
    exit 1
}

# --- Trigger the backfill ---
Write-Host ""
Write-Host "Triggering Airflow backfill..."
docker compose -f $ComposeFile exec airflow-scheduler airflow dags backfill -s $startStr -e $endStr $DagId

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Backfill exited non-zero. Check Airflow logs:" -ForegroundColor Red
    Write-Host "  docker compose -f $ComposeFile logs --tail=200 airflow-scheduler" -ForegroundColor Red
    exit $LASTEXITCODE
}

# --- Confirm new state ---
$newLatest = Read-LatestGameDate
Write-Host ""
Write-Host "Catch-up complete." -ForegroundColor Green
if ($newLatest) {
    $newLatestStr = $newLatest.ToString('yyyy-MM-dd')
    Write-Host "Latest game_date in processed/ is now: $newLatestStr" -ForegroundColor Green
}
