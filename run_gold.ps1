$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run gold --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
