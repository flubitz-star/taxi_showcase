"""Configuration shared by multiple pipelines."""

import argparse
import re
from dataclasses import dataclass
from typing import Protocol


def parse_catalog_argument() -> str:
    """Read the deployment's Unity Catalog name from the task arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    catalog = parser.parse_args().catalog
    # Restrict interpolation into table and volume names to simple identifiers.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError(
            "Catalog must be a simple Unity Catalog identifier containing "
            "letters, digits, or underscores."
        )
    return catalog


def landing_zone_root(catalog: str) -> str:
    """Return the shared Unity Catalog Volume root for incoming files."""
    return f"/Volumes/{catalog}/raw_files/landing_zone"


class LandingZoneConfig(Protocol):
    """Minimum configuration required by the shared landing-zone check."""

    landing_zone: str
    tracking_table: str


@dataclass(frozen=True)
class GoldTables:
    taxi_weather_enriched: str
    daily_metrics: str
    hourly_ridership: str
    mobility_daily_comparison: str
    mobility_hourly_comparison: str


def gold_tables(catalog: str) -> GoldTables:
    """Build fully qualified names for the project's Gold output tables."""
    return GoldTables(
        taxi_weather_enriched=f"{catalog}.gold.taxi_weather_enriched",
        daily_metrics=f"{catalog}.gold.daily_metrics",
        hourly_ridership=f"{catalog}.gold.hourly_ridership",
        mobility_daily_comparison=f"{catalog}.gold.mobility_daily_comparison",
        mobility_hourly_comparison=f"{catalog}.gold.mobility_hourly_comparison",
    )
