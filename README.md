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
    ├── config.py            # PROJECT_CONFIG, GOLD_TABLES
    ├── tracking.py          # Checksum dedup, file tracking
    ├── schema.py            # Schema validation & evolution
    ├── timestamp_utils.py   # Timestamp validation
    ├── ingest_helpers.py    # run_landing_zone_check, verify_bronze
    ├── silver_helpers.py    # Silver checks, DLQ, write_silver
    └── pipelines/           # Pipeline entry points
        ├── weather/         # 3 scripts (landing → ingress → silver)
        ├── taxi/            # 5 scripts (landing → ingress → silver → gold → mobility)
        └── bike/            # 2 scripts (ingress → silver)
```

## Deploy

```bash
# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run a job
databricks bundle run ingest_silver --target dev
```

## Unity Catalog

All tables in catalog `workspace`:
- `workspace.raw_files.*` — tracking tables
- `workspace.bronze.*` — raw data
- `workspace.silver.*` — cleaned data + DLQ
- `workspace.gold.*` — analytics tables
