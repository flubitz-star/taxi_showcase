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
├── run_weather_ingress.ps1  # Deploy and run Weather Silver
├── run_bike_ingress.ps1     # Bike pipeline helper
├── run_taxi_pipeline.ps1    # Taxi pipeline helper through Silver
├── run_gold.ps1             # Gold pipeline helper
├── resources/               # Job definitions
│   ├── ingest_silver.job.yml
│   └── gold.job.yml
└── src/taxi_showcase/       # Python package (wheel)
    ├── tracking.py          # Checksum dedup, file tracking
    ├── schema.py            # Schema validation & evolution
    ├── timestamp_utils.py   # Timestamp validation
    ├── ingest_helpers.py    # run_landing_zone_check, verify_bronze
    ├── silver_helpers.py    # Silver checks, DLQ, write_silver
    └── pipelines/           # Pipeline entry points and source schemas
        ├── shared/          # Shared config and tracking schema
        ├── weather/         # Pipeline scripts, config.py, and schema.py
        ├── taxi/            # Pipeline scripts, config.py, and schema.py
        └── bike/            # Pipeline scripts, config.py, and schema.py
```

Source schemas and pipeline-specific table/input configuration live alongside
their respective pipelines. Shared catalog settings, metadata columns, Gold
table names, and the tracking schema live in `pipelines/shared/`. Silver, DLQ,
and Gold schemas are derived from their transformation DataFrames, preserving
the existing schema-evolution behavior.

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

# Run the full Gold job (Silver tables must already exist)
databricks bundle run gold --target dev

# Run pipeline helpers from PowerShell
.\run_weather_ingress.ps1
.\run_bike_ingress.ps1
.\run_taxi_pipeline.ps1
.\run_gold.ps1
```

The Taxi helper runs `taxi_landing_zone`, `taxi_ingress`, and `taxi_silver`
in sequence. The Bike helper runs `bike_ingress` and `bike_silver`; bike ingress
checks and registers its landing-zone files itself. The Gold helper deploys the
bundle and runs the `gold` job, which builds `gold_taxi_weather` followed by
`gold_mobility`. It does not run the upstream ingestion or Silver jobs, so run
those first when their tables need to be refreshed.

The Weather helper deploys the bundle and runs `weather_silver`; it does not
run the Landing Zone or ingress tasks. To run all Weather steps, execute the
following task commands in order. Use the `ingest_silver` job name with the
`--only` task selector:

```powershell
databricks bundle run ingest_silver --only weather_landing_zone --target dev
databricks bundle run ingest_silver --only weather_ingress --target dev
databricks bundle run ingest_silver --only weather_silver --target dev
```

## Unity Catalog

All tables in catalog `workspace`:
- `workspace.raw_files.*` — tracking tables
- `workspace.bronze.*` — raw data
- `workspace.silver.*` — cleaned data + DLQ
- `workspace.gold.*` — analytics tables
