"""Central runtime configuration for all projects (taxi, weather, bike).

Catalog, table names, paths, and pipeline options live here.
Source schema contracts live with their respective pipelines.
"""

from taxi_showcase.pipelines.bike.schema import (
    CSV_OPTIONS as BIKE_CSV_OPTIONS,
    EXPECTED_COLUMNS as BIKE_EXPECTED_COLUMNS,
)
from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS as TAXI_EXPECTED_COLUMNS
from taxi_showcase.pipelines.weather.schema import EXPECTED_COLUMNS as WEATHER_EXPECTED_COLUMNS

CATALOG = "workspace"

# Landing Zone (UC Volume) – pro Projekt ein Unterordner
LANDING_ZONE_ROOT = f"/Volumes/{CATALOG}/raw_files/landing_zone"

# Metadaten-Spalten, die beim Bronze-Ingest hinzugefuegt werden
META_COLUMNS = {"_source_file", "_ingested_at"}

# ── Projekt-Konfiguration ──
PROJECT_CONFIG = {
    "taxi": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/taxi",
        "tracking_table":  f"{CATALOG}.raw_files.taxi_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.taxi_trips_raw",
        "silver_table":    f"{CATALOG}.silver.taxi_trips",
        "dlq_table":       f"{CATALOG}.silver.taxi_trips_dlq",
        "expected_columns": TAXI_EXPECTED_COLUMNS,
    },
    "weather": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/weather",
        "tracking_table":  f"{CATALOG}.raw_files.weather_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.weather_raw",
        "silver_table":    f"{CATALOG}.silver.weather_hourly",
        "expected_columns": WEATHER_EXPECTED_COLUMNS,
    },
    "bike": {
        "landing_zone":   f"{LANDING_ZONE_ROOT}/bike",
        "tracking_table":  f"{CATALOG}.raw_files.bike_ingested_files",
        "bronze_table":    f"{CATALOG}.bronze.bike_trips_raw",
        "silver_table":    f"{CATALOG}.silver.bike_trips",
        "dlq_table":       f"{CATALOG}.silver.bike_trips_dlq",
        "expected_columns": BIKE_EXPECTED_COLUMNS,
        "csv_options": BIKE_CSV_OPTIONS,
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
