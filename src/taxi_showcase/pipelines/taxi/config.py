"""NYC taxi pipeline configuration."""

from collections.abc import Mapping
from dataclasses import dataclass

from taxi_showcase.pipelines.shared.config import (
    DEFAULT_LANDING_CATALOG,
    landing_zone_root,
)
from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS


@dataclass(frozen=True)
class TaxiConfig:
    """Paths, tables, and source schema used by the Taxi pipeline.

    The batch and streaming inputs share the landing volume. The streaming
    checkpoint is isolated by target catalog so each environment tracks its
    own Auto Loader progress.
    """

    landing_zone: str
    tracking_table: str
    bronze_table: str
    silver_table: str
    dlq_table: str
    expected_columns: Mapping[str, str]
    streaming_landing_zone: str
    streaming_checkpoint: str


def create_config(catalog: str, landing_catalog: str = DEFAULT_LANDING_CATALOG) -> TaxiConfig:
    """Build Taxi paths and table names for the target and shared landing catalogs."""
    root = landing_zone_root(landing_catalog)
    return TaxiConfig(
        landing_zone=f"{root}/taxi",
        streaming_landing_zone=f"{root}/taxi_streaming",
        streaming_checkpoint=f"{root}/taxi_streaming_checkpoint/{catalog}",
        tracking_table=f"{catalog}.raw_files.taxi_ingested_files",
        bronze_table=f"{catalog}.bronze.taxi_trips_raw",
        silver_table=f"{catalog}.silver.taxi_trips",
        dlq_table=f"{catalog}.silver.taxi_trips_dlq",
        expected_columns=EXPECTED_COLUMNS,
    )
