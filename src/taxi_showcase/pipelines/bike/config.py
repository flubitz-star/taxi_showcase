"""CitiBike pipeline configuration."""

from taxi_showcase.pipelines.bike.schema import CSV_OPTIONS, EXPECTED_COLUMNS
from taxi_showcase.pipelines.shared.config import CATALOG, LANDING_ZONE_ROOT

CONFIG = {
    "landing_zone": f"{LANDING_ZONE_ROOT}/bike",
    "tracking_table": f"{CATALOG}.raw_files.bike_ingested_files",
    "bronze_table": f"{CATALOG}.bronze.bike_trips_raw",
    "silver_table": f"{CATALOG}.silver.bike_trips",
    "dlq_table": f"{CATALOG}.silver.bike_trips_dlq",
    "expected_columns": EXPECTED_COLUMNS,
    "csv_options": CSV_OPTIONS,
}
