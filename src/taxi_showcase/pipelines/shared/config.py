"""Configuration shared by multiple pipelines."""

import argparse
import re
from dataclasses import dataclass
from typing import Protocol

DEFAULT_LANDING_CATALOG = "workspace"


def _validate_identifier(value: str) -> str:
    # Restrict interpolation into table and volume names to simple identifiers.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            "Catalog must be a simple Unity Catalog identifier containing "
            "letters, digits, or underscores."
        )
    return value


def parse_catalog_argument() -> str:
    """Read the environment's Unity Catalog name (Bronze/Silver/Gold) from the task arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args, _ = parser.parse_known_args()
    return _validate_identifier(args.catalog)


def parse_landing_catalog_argument() -> str:
    """Read the catalog holding the landing-zone Volume, shared by all environments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing-catalog", default=DEFAULT_LANDING_CATALOG)
    args, _ = parser.parse_known_args()
    return _validate_identifier(args.landing_catalog)


def landing_zone_root(landing_catalog: str = DEFAULT_LANDING_CATALOG) -> str:
    """Return the shared Unity Catalog Volume root for incoming files."""
    return f"/Volumes/{landing_catalog}/raw_files/landing_zone"


class LandingZoneConfig(Protocol):
    """Minimum configuration required by the shared landing-zone check."""

    landing_zone: str
    tracking_table: str


@dataclass(frozen=True)
class GoldTables:
    hourly_ridership: str
    mobility_daily_comparison: str
    mobility_hourly_comparison: str


def gold_tables(catalog: str) -> GoldTables:
    """Build fully qualified names for the project's Gold output tables."""
    return GoldTables(
        hourly_ridership=f"{catalog}.gold.hourly_ridership",
        mobility_daily_comparison=f"{catalog}.gold.mobility_daily_comparison",
        mobility_hourly_comparison=f"{catalog}.gold.mobility_hourly_comparison",
    )
