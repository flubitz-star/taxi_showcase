"""Open-Meteo weather pipeline configuration."""

from taxi_showcase.pipelines.shared.config import CATALOG, LANDING_ZONE_ROOT
from taxi_showcase.pipelines.weather.schema import EXPECTED_COLUMNS

CONFIG = {
    "landing_zone": f"{LANDING_ZONE_ROOT}/weather",
    "tracking_table": f"{CATALOG}.raw_files.weather_ingested_files",
    "bronze_table": f"{CATALOG}.bronze.weather_raw",
    "silver_table": f"{CATALOG}.silver.weather_hourly",
    "expected_columns": EXPECTED_COLUMNS,
}
