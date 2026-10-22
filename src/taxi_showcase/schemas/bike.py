"""Schema contracts for bike Bronze, Silver, and DLQ tables."""

from taxi_showcase.pipelines.bike.schema import EXPECTED_COLUMNS

BRONZE_SCHEMA = {
    **EXPECTED_COLUMNS,
    "_source_file": "string",
    "_ingested_at": "timestamp",
}

SILVER_SCHEMA = {
    **BRONZE_SCHEMA,
    "started_at": "timestamp_ntz",
    "ended_at": "timestamp_ntz",
    "trip_duration_min": "double",
    "pickup_dayofweek": "int",
    "pickup_weekday": "string",
    "is_weekend": "boolean",
    "_silver_processed_at": "timestamp",
}

DLQ_SCHEMA = {
    **SILVER_SCHEMA,
    "_dlq_reason": "string",
    "_dlq_removed_at": "timestamp",
}
