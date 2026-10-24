$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only weather_landing_zone --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only weather_bronze --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# databricks bundle run ingest_silver --only weather_silver --target dev
# if ($LASTEXITCODE -ne 0) {
#     exit $LASTEXITCODE
# }
