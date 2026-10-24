$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Running the Gold job, including zone_revenue_hourly..."
databricks bundle run gold --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
