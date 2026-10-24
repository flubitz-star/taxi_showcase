"""Open-Meteo weather pipeline configuration."""

from collections.abc import Mapping
from dataclasses import dataclass

from taxi_showcase.pipelines.shared.config import (
    DEFAULT_LANDING_CATALOG,
    landing_zone_root,
)
from taxi_showcase.pipelines.weather.schema import EXPECTED_COLUMNS


@dataclass(frozen=True)
class WeatherConfig:
    """Paths, tables, and source schema used by the weather pipeline."""

    landing_zone: str
    tracking_table: str
    bronze_table: str
    silver_table: str
    expected_columns: Mapping[str, str]


def create_config(catalog: str, landing_catalog: str = DEFAULT_LANDING_CATALOG) -> WeatherConfig:
    """Build weather settings; the landing zone lives in the shared landing catalog."""
    return WeatherConfig(
        landing_zone=f"{landing_zone_root(landing_catalog)}/weather",
        tracking_table=f"{catalog}.raw_files.weather_ingested_files",
        bronze_table=f"{catalog}.bronze.weather_raw",
        silver_table=f"{catalog}.silver.weather_hourly",
        expected_columns=EXPECTED_COLUMNS,
    )
