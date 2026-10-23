"""NYC taxi pipeline configuration."""

from collections.abc import Mapping
from dataclasses import dataclass

from taxi_showcase.pipelines.shared.config import landing_zone_root
from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS


@dataclass(frozen=True)
class TaxiConfig:
    """Paths, tables, and source schema used by the Taxi pipeline."""

    landing_zone: str
    tracking_table: str
    bronze_table: str
    silver_table: str
    dlq_table: str
    expected_columns: Mapping[str, str]


def create_config(catalog: str) -> TaxiConfig:
    """Build Taxi pipeline names from the selected Unity Catalog."""
    return TaxiConfig(
        landing_zone=f"{landing_zone_root(catalog)}/taxi",
        tracking_table=f"{catalog}.raw_files.taxi_ingested_files",
        bronze_table=f"{catalog}.bronze.taxi_trips_raw",
        silver_table=f"{catalog}.silver.taxi_trips",
        dlq_table=f"{catalog}.silver.taxi_trips_dlq",
        expected_columns=EXPECTED_COLUMNS,
    )
