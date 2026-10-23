"""CitiBike pipeline configuration."""

from collections.abc import Mapping
from dataclasses import dataclass

from taxi_showcase.pipelines.bike.schema import CSV_OPTIONS, EXPECTED_COLUMNS
from taxi_showcase.pipelines.shared.config import landing_zone_root


@dataclass(frozen=True)
class BikeConfig:
    """Paths, tables, source schema, and CSV options for CitiBike data."""

    landing_zone: str
    tracking_table: str
    bronze_table: str
    silver_table: str
    dlq_table: str
    expected_columns: Mapping[str, str]
    csv_options: Mapping[str, str | bool]


def create_config(catalog: str) -> BikeConfig:
    """Build CitiBike pipeline settings from the selected Unity Catalog."""
    return BikeConfig(
        landing_zone=f"{landing_zone_root(catalog)}/bike",
        tracking_table=f"{catalog}.raw_files.bike_ingested_files",
        bronze_table=f"{catalog}.bronze.bike_trips_raw",
        silver_table=f"{catalog}.silver.bike_trips",
        dlq_table=f"{catalog}.silver.bike_trips_dlq",
        expected_columns=EXPECTED_COLUMNS,
        csv_options=CSV_OPTIONS,
    )
