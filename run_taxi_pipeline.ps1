$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only taxi_landing_zone --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only taxi_ingress --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# databricks bundle run ingest_silver --only taxi_silver --target dev
# if ($LASTEXITCODE -ne 0) {
#     exit $LASTEXITCODE
# }
