"""Open-Meteo weather pipeline configuration."""

from collections.abc import Mapping
from dataclasses import dataclass

from taxi_showcase.pipelines.shared.config import landing_zone_root
from taxi_showcase.pipelines.weather.schema import EXPECTED_COLUMNS


@dataclass(frozen=True)
class WeatherConfig:
    landing_zone: str
    tracking_table: str
    bronze_table: str
    silver_table: str
    expected_columns: Mapping[str, str]


def create_config(catalog: str) -> WeatherConfig:
    return WeatherConfig(
        landing_zone=f"{landing_zone_root(catalog)}/weather",
        tracking_table=f"{catalog}.raw_files.weather_ingested_files",
        bronze_table=f"{catalog}.bronze.weather_raw",
        silver_table=f"{catalog}.silver.weather_hourly",
        expected_columns=EXPECTED_COLUMNS,
    )
