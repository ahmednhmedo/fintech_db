<#
  GOLD layer runner (Windows host, Windows auth to ahmed\SQLEXPRESS).
  Runs after Docker/Spark have produced the silver Parquet in data\lake\silver.

  Steps:
    1. ensure schemas + the gold star DDL exist (silver schema; dbo.* star)
    2. load silver Parquet -> fintech_db.silver.*           (pyodbc, Windows auth)
    3. drop star FKs -> load gold (dbo.*) -> re-add FKs       (sqlcmd)
    4. dbt build  (reporting view + data-quality tests on the star)

  Usage:  .\run_gold.ps1
#>
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repo

# Load .env (if present) so WH_SERVER / WH_DATABASE / WH_DRIVER drive the host gold
# steps (sqlcmd here + the silver loader, which reads these from the environment).
$envFile = Join-Path $repo ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $k, $v = $line.Split("=", 2)
            [Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), "Process")
        }
    }
}

# Windows-auth defaults if .env is absent / a key is unset.
$server = if ($env:WH_SERVER)   { $env:WH_SERVER }   else { "ahmed\SQLEXPRESS" }
$db     = if ($env:WH_DATABASE) { $env:WH_DATABASE } else { "fintech_db" }

# Resolve a Python 3.11 interpreter that has the project deps (pyodbc, pyarrow, dbt).
$py = (& py -3.11 -c "import sys; print(sys.executable)").Trim()
$scripts = Join-Path (Split-Path -Parent $py) "Scripts"
$dbt = Join-Path $scripts "dbt.exe"

# Run a .sql file and STOP immediately on any SQL error. -b makes sqlcmd return a
# non-zero exit code on error severity >= 11, which we surface as a terminating error.
function Invoke-Sql($file) {
    sqlcmd -S $server -E -C -b -d $db -i $file
    if ($LASTEXITCODE -ne 0) { throw "SQL FAILED (exit $LASTEXITCODE): $file" }
}

Write-Host "== 1/4  Ensure warehouse schemas + gold star DDL ==" -ForegroundColor Cyan
Invoke-Sql "$repo\sql\setup_warehouse.sql"
# Create the star (your DDL) only if it isn't there yet.
$factExists = (& sqlcmd -S $server -E -C -d $db -h -1 -W -Q `
    "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.fact_transactions','U') IS NULL THEN 0 ELSE 1 END;").Trim()
if ($factExists -eq "0") {
    Write-Host "   creating star tables from sql\01_create_star.sql ..." -ForegroundColor DarkGray
    Invoke-Sql "$repo\sql\01_create_star.sql"
}

Write-Host "== 2/4  Load silver Parquet -> SQL Server [silver] ==" -ForegroundColor Cyan
& $py "ingestion\load_silver_to_sqlserver.py"
if ($LASTEXITCODE -ne 0) { throw "silver loader FAILED (exit $LASTEXITCODE)" }

Write-Host "== 3/4  Load gold: drop FKs -> dbo.* -> re-add FKs ==" -ForegroundColor Cyan
Invoke-Sql "$repo\sql\drop_star_constraints.sql"
Invoke-Sql "$repo\sql\load_gold.sql"
Invoke-Sql "$repo\sql\add_star_constraints.sql"

Write-Host "== 4/4  dbt build (reporting view + data-quality tests) ==" -ForegroundColor Cyan
Set-Location (Join-Path $repo "dbt\fintech")
& $dbt build
if ($LASTEXITCODE -ne 0) { throw "dbt build FAILED (exit $LASTEXITCODE)" }

Set-Location $repo
Write-Host "Gold build complete: dbo.* star in fintech_db + reporting.rpt_daily_transactions." -ForegroundColor Green
