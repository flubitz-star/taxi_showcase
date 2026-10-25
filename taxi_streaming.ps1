<#
.SYNOPSIS
Runs the Taxi Auto Loader Bronze and Silver tasks in the development target.

.DESCRIPTION
Deploys the Databricks bundle, processes currently available JSON files from the
Taxi streaming landing zone, and then runs the incremental Taxi Silver task.

.EXAMPLE
.\taxi_streaming.ps1
#>

$ErrorActionPreference = "Stop"

databricks bundle deploy --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only taxi_streaming_bronze --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

databricks bundle run ingest_silver --only taxi_silver --target dev
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
