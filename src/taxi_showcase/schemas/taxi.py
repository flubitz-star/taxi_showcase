"""Schema contracts for taxi Bronze, Silver, and DLQ tables."""

from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS

BRONZE_SCHEMA = {
    **EXPECTED_COLUMNS,
    "_source_file": "string",
    "_ingested_at": "timestamp",
}

SILVER_SCHEMA = {
    **BRONZE_SCHEMA,
    "trip_duration_min": "double",
    "pickup_dayofweek": "int",
    "pickup_weekday": "string",
    "is_weekend": "boolean",
    "_silver_processed_at": "timestamp",
    "_has_negative_fare": "boolean",
    "_passenger_count_imputed": "boolean",
}

DLQ_SCHEMA = {
    **BRONZE_SCHEMA,
    "trip_duration_min": "double",
    "pickup_dayofweek": "int",
    "pickup_weekday": "string",
    "is_weekend": "boolean",
    "_silver_processed_at": "timestamp",
    "_has_negative_fare": "boolean",
    "_dlq_reason": "string",
    "_dlq_removed_at": "timestamp",
}
