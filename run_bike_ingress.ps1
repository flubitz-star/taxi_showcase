$ErrorActionPreference = "Stop"

databricks bundle run ingest_silver --only bike_ingress --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
