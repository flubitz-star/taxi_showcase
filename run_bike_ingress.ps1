$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only bike_landing_zone --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only bike_bronze --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# databricks bundle run ingest_silver --only bike_silver --target dev
# if ($LASTEXITCODE -ne 0) {
#     exit $LASTEXITCODE
# }
