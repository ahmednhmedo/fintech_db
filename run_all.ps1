<#
  ONE-COMMAND full pipeline run.
    Docker (Airflow+Spark)  ->  bronze + silver  ->  host gold load + dbt build.

  Usage:  open PowerShell in the fintech-lakehouse folder and run:  .\run_all.ps1
  Requires: Docker Desktop running; Python 3.11 host venv with requirements.txt;
            SQL Server ahmed\SQLEXPRESS reachable via Windows auth.
#>
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repo

Write-Host "`n[1/3] Starting Docker stack..." -ForegroundColor Cyan
docker compose up -d --build | Out-Null
Start-Sleep -Seconds 15

Write-Host "[2/3] Bronze + Silver (extract CSV -> bronze -> Spark silver)..." -ForegroundColor Cyan
# Compose service reference (not a fixed container name) -> survives folder renames.
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow && python ingestion/extract_to_bronze.py && spark-submit --driver-memory 4g --conf spark.local.dir=/opt/data/spark-tmp spark/bronze_to_silver.py"

Write-Host "[3/3] Gold: load silver -> SQL Server -> dbt build (on host, Windows auth)..." -ForegroundColor Cyan
& "$repo\run_gold.ps1"

Write-Host "`nDONE - warehouse rebuilt in fintech_db. Open Power BI and Refresh." -ForegroundColor Green
