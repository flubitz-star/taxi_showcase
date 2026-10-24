"""Schema contracts for weather Bronze and Silver tables."""

from taxi_showcase.pipelines.weather.schema import EXPECTED_COLUMNS

# Bronze retains raw weather fields and adds ingestion lineage metadata.
BRONZE_SCHEMA = {
    **EXPECTED_COLUMNS,
    "_source_file": "string",
    "_ingested_at": "timestamp",
}

# Silver uses analytical numeric types and a timezone-free hourly timestamp.
SILVER_SCHEMA = {
    "latitude": "double",
    "longitude": "double",
    "utc_offset_seconds": "int",
    "timezone": "string",
    "timezone_abbreviation": "string",
    "_source_file": "string",
    "_ingested_at": "timestamp",
    "ts": "timestamp_ntz",
    "temperature_2m_c": "double",
    "rain_mm": "double",
    "snowfall_cm": "double",
    "precipitation_mm": "double",
    "snow_depth_m": "double",
    "_silver_processed_at": "timestamp",
}

# Rejected source rows retain their raw fields and add a reason and removal time.
DLQ_SCHEMA = {
    **BRONZE_SCHEMA,
    "_dlq_reason": "string",
    "_dlq_removed_at": "timestamp",
}
