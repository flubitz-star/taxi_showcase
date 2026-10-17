# taxi_showcase

NYC Taxi, Weather & CitiBike Medallion Pipeline – Databricks DABs Project.

## Architecture

```
Landing Zone (UC Volume) → Bronze (Delta) → Silver (Delta) → Gold (Delta)
```

Three data sources:
- **Taxi**: NYC TLC Yellow Trip Records (Parquet)
- **Weather**: Open-Meteo Historical Weather (CSV)
- **Bike**: CitiBike Trip Data (ZIP/CSV)

## Structure

```
taxi_showcase/
├── databricks.yml           # Bundle config + targets
├── pyproject.toml           # Wheel build config
├── resources/               # Job definitions
│   ├── ingest_silver.job.yml
│   └── gold.job.yml
└── src/taxi_showcase/       # Python package (wheel)
    ├── config.py            # Catalog, table names, and pipeline configuration
    ├── tracking.py          # Checksum dedup, file tracking
    ├── schema.py            # Schema validation & evolution
    ├── timestamp_utils.py   # Timestamp validation
    ├── ingest_helpers.py    # run_landing_zone_check, verify_bronze
    ├── silver_helpers.py    # Silver checks, DLQ, write_silver
    └── pipelines/           # Pipeline entry points and source schemas
        ├── shared/          # Shared tracking schema
        ├── weather/         # Pipeline scripts and schema.py
        ├── taxi/            # Pipeline scripts and schema.py
        └── bike/            # Pipeline scripts and schema.py
```

Source schemas live alongside their respective pipelines; the shared tracking
schema lives in `pipelines/shared/`. Silver, DLQ, and Gold schemas are derived
from their transformation DataFrames, preserving the existing schema-evolution
behavior.

## Deploy

Python 3.10 or newer must be installed and available as `python` on your `PATH`.
The jobs use Databricks serverless compute and require a workspace that supports serverless jobs.

```bash
# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run a job
databricks bundle run ingest_silver --target dev

# Run only the weather ingress task (files must already be registered)
databricks bundle run weather_ingress --target dev

# Or run only weather_ingress from ingest_silver in PowerShell
.\run_weather_ingress.ps1

# Run only bike_ingress from ingest_silver in PowerShell
.\run_bike_ingress.ps1
```

## Unity Catalog

All tables in catalog `workspace`:
- `workspace.raw_files.*` — tracking tables
- `workspace.bronze.*` — raw data
- `workspace.silver.*` — cleaned data + DLQ
- `workspace.gold.*` — analytics tables
