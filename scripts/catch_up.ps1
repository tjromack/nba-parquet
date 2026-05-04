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

.EXAMPLE
    .\scripts\catch_up.ps1
    Detect the gap and backfill if needed. Run this each morning during
    the season.

.EXAMPLE
    .\scripts\catch_up.ps1 -From 2026-04-18 -To 2026-05-15
    Override auto-detection and force a specific date range. Useful for
    cold starts or backfilling a wider window than just yesterday.

.NOTES
    Uses powershell-native syntax. Tested against Windows PowerShell 5.1
    + Docker Desktop on Windows 10/11. Run from the repo root.
#>

[CmdletBinding()]
param(
    [string]$From = "",
    [string]$To = ""
)

$ErrorActionPreference = "Stop"

$ProcessedDir = "./out/processed/nba/team_game_stats"
$ComposeFile = "infra/docker-compose.yml"
$DagId = "nba_etl_pipeline"
$PlayoffsStart = "2026-04-18"

function Read-LatestGameDate {
    if (-not (Test-Path $ProcessedDir)) {
        return $null
    }
    $dirs = Get-ChildItem -Path $ProcessedDir -Filter "game_date=*" -Recurse -Directory `
        -ErrorAction SilentlyContinue
    if (-not $dirs -or $dirs.Count -eq 0) {
        return $null
    }
    $dates = $dirs | ForEach-Object { $_.Name -replace 'game_date=', '' } |
        Sort-Object -Unique
    return [datetime]::ParseExact(($dates | Select-Object -Last 1), 'yyyy-MM-dd', $null)
}

function Resolve-DateRange {
    $endTarget = if ($To) {
        [datetime]::ParseExact($To, 'yyyy-MM-dd', $null)
    } else {
        (Get-Date).Date.AddDays(-1)
    }

    if ($From) {
        $startTarget = [datetime]::ParseExact($From, 'yyyy-MM-dd', $null)
    } else {
        $latest = Read-LatestGameDate
        if ($null -eq $latest) {
            Write-Host "No processed/ data found yet (cold start)." -ForegroundColor Yellow
            Write-Host "Defaulting -From to playoffs start: $PlayoffsStart"
            $startTarget = [datetime]::ParseExact($PlayoffsStart, 'yyyy-MM-dd', $null)
        } else {
            Write-Host "Latest game_date in processed/: $($latest.ToString('yyyy-MM-dd'))"
            $startTarget = $latest.AddDays(1)
        }
    }

    return @{ Start = $startTarget; End = $endTarget }
}

# --- Main ---

$range = Resolve-DateRange
$start = $range.Start
$end = $range.End

Write-Host "Target end (yesterday by default): $($end.ToString('yyyy-MM-dd'))"

if ($start -gt $end) {
    Write-Host ""
    Write-Host "Already up to date — nothing to backfill." -ForegroundColor Green
    exit 0
}

$gapDays = ($end - $start).Days + 1
Write-Host ""
Write-Host "Backfill range: $($start.ToString('yyyy-MM-dd')) -> $($end.ToString('yyyy-MM-dd')) ($gapDays day(s))." -ForegroundColor Cyan

# Bring up the stack (idempotent; no-op if already running).
Write-Host ""
Write-Host "Ensuring Airflow stack is running..."
docker compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Failed to bring up Airflow stack. Is Docker Desktop running?" -ForegroundColor Red
    exit $LASTEXITCODE
}

# Trigger the backfill.
Write-Host ""
Write-Host "Triggering Airflow backfill..."
$startStr = $start.ToString('yyyy-MM-dd')
$endStr = $end.ToString('yyyy-MM-dd')

docker compose -f $ComposeFile exec airflow-scheduler `
    airflow dags backfill -s $startStr -e $endStr $DagId

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Backfill exited non-zero. Check Airflow logs:" -ForegroundColor Red
    Write-Host "  docker compose -f $ComposeFile logs --tail=200 airflow-scheduler" -ForegroundColor Red
    exit $LASTEXITCODE
}

# Confirm new state.
$newLatest = Read-LatestGameDate
Write-Host ""
Write-Host "Catch-up complete." -ForegroundColor Green
if ($newLatest) {
    Write-Host "Latest game_date in processed/ is now: $($newLatest.ToString('yyyy-MM-dd'))" -ForegroundColor Green
}
