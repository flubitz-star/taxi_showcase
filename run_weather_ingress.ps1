$ErrorActionPreference = "Stop"

databricks bundle run ingest_silver --only weather_ingress --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
