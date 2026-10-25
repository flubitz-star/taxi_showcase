param(
    # Run taxi_silver incrementally (append only new Bronze files) instead of a full refresh.
    [switch]$Incremental,
    # Run Bronze and Silver even when the landing zone has no pending files.
    [switch]$Force
)

$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# The output is shown and also captured to read the pending count.
databricks bundle run ingest_silver --only taxi_landing_zone --target dev | Tee-Object -Variable lzOutput
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# Same rule as the job's condition task taxi_has_new_files.
$match = [regex]::Match(($lzOutput -join "`n"), "Pending files for Bronze: (\d+)")
if (-not $match.Success) {
    throw "Pending-Count nicht in der Ausgabe von taxi_landing_zone gefunden."
}
if (-not $Force -and [int]$match.Groups[1].Value -eq 0) {
    Write-Host "Keine neuen Dateien - taxi_bronze und taxi_silver werden uebersprungen (-Force erzwingt den Lauf)."
    exit 0
}

databricks bundle run ingest_silver --only taxi_bronze --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# --python-params replaces the task parameters, so the catalog is passed again.
$silverArgs = @()
if ($Incremental) {
    $silverArgs = @("--python-params", "--catalog,taxi_dev,--incremental")
}
databricks bundle run ingest_silver --only taxi_silver --target dev @silverArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
