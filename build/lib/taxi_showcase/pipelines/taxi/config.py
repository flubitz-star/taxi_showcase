"""NYC taxi pipeline configuration."""

from taxi_showcase.pipelines.shared.config import CATALOG, LANDING_ZONE_ROOT
from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS

CONFIG = {
    "landing_zone": f"{LANDING_ZONE_ROOT}/taxi",
    "tracking_table": f"{CATALOG}.raw_files.taxi_ingested_files",
    "bronze_table": f"{CATALOG}.bronze.taxi_trips_raw",
    "silver_table": f"{CATALOG}.silver.taxi_trips",
    "dlq_table": f"{CATALOG}.silver.taxi_trips_dlq",
    "expected_columns": EXPECTED_COLUMNS,
}
