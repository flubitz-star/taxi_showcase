"""Central configuration for all projects (taxi, weather, bike).

Single source of truth for catalog, schemas, table names, and expected columns.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

CATALOG = "workspace"

# Landing Zone (UC Volume) – pro Projekt ein Unterordner
LANDING_ZONE_ROOT = f"/Volumes/{CATALOG}/raw_files/landing_zone"

# Metadaten-Spalten, die beim Bronze-Ingest hinzugefuegt werden
META_COLUMNS = {"_source_file", "_ingested_at"}

# Schema der Tracking-Tabellen (checksummenbasierte Deduplizierung)
TRACKING_SCHEMA = StructType([
    StructField("file_name",   StringType(),    False),
    StructField("file_path",   StringType(),    False),
    StructField("checksum",    StringType(),    False),
    StructField("file_size",   LongType(),      False),
    StructField("ingested_at", TimestampType(), False),
])

# ── Projekt-Konfiguration ──
PROJECT_CONFIG = {
    "taxi": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/taxi",
        "tracking_table":  f"{CATALOG}.raw_files.taxi_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.taxi_trips_raw",
        "silver_table":    f"{CATALOG}.silver.taxi_trips",
        "dlq_table":       f"{CATALOG}.silver.taxi_trips_dlq",
        "expected_columns": {
            "VendorID":              "int",
            "tpep_pickup_datetime":  "timestamp",
            "tpep_dropoff_datetime": "timestamp",
            "passenger_count":       "bigint",
            "trip_distance":         "double",
            "RatecodeID":            "bigint",
            "store_and_fwd_flag":    "string",
            "PULocationID":          "int",
            "DOLocationID":          "int",
            "payment_type":          "bigint",
            "fare_amount":           "double",
            "extra":                 "double",
            "mta_tax":               "double",
            "tip_amount":            "double",
            "tolls_amount":         "double",
            "improvement_surcharge": "double",
            "total_amount":         "double",
            "congestion_surcharge":  "double",
            "Airport_fee":           "double",
            "cbd_congestion_fee":    "double",
            "request_source":        "string",
        },
    },
    "weather": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/weather",
        "tracking_table":  f"{CATALOG}.raw_files.weather_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.weather_raw",
        "silver_table":    f"{CATALOG}.silver.weather_hourly",
        "expected_columns": {
            "latitude":               "string",
            "longitude":              "string",
            "elevation":              "string",
            "utc_offset_seconds":     "string",
            "timezone":               "string",
            "timezone_abbreviation":  "string",
            "time":                   "string",
            "temperature_2m (\u00b0C)":    "string",
            "rain (mm)":              "string",
            "snowfall (cm)":           "string",
            "precipitation (mm)":      "string",
            "snow_depth (m)":           "string",
        },
    },
    "bike": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/bike",
        "tracking_table":  f"{CATALOG}.raw_files.bike_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.citibike_trips_raw",
        "silver_table":    f"{CATALOG}.silver.citibike_trips",
        "dlq_table":       f"{CATALOG}.silver.citibike_trips_dlq",
        "expected_columns": {
            "ride_id":              "string",
            "rideable_type":        "string",
            "started_at":           "timestamp",
            "ended_at":             "timestamp",
            "start_station_name":   "string",
            "start_station_id":     "string",
            "end_station_name":     "string",
            "end_station_id":       "string",
            "start_lat":            "double",
            "start_lng":            "double",
            "end_lat":              "double",
            "end_lng":              "double",
            "member_casual":        "string",
        },
        "csv_options": {
            "header": True,
            "timestampFormat": "yyyy-MM-dd HH:mm:ss[.SSS]",
        },
    },
}

# ── Gold-Tabellen (projektuebergreifend) ──
GOLD_TABLES = {
    "taxi_weather_enriched":      f"{CATALOG}.gold.taxi_weather_enriched",
    "daily_metrics":              f"{CATALOG}.gold.daily_metrics",
    "hourly_ridership":           f"{CATALOG}.gold.hourly_ridership",
    "mobility_daily_comparison":  f"{CATALOG}.gold.mobility_daily_comparison",
    "mobility_hourly_comparison": f"{CATALOG}.gold.mobility_hourly_comparison",
}
