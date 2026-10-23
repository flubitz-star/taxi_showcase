"""Weather Landing Zone Check \u2013 Step 01.

Lists files in the weather landing zone, registers new files via checksum tracking.
"""

from taxi_showcase.ingest_helpers import run_landing_zone_check
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.weather.config import create_config


def main() -> None:
    # Resolve the weather landing-zone and tracking-table names for this catalog.
    cfg = create_config(parse_catalog_argument())
    new_files, dup_files = run_landing_zone_check(cfg)
    # Report files discovered by the shared checksum-based landing-zone check.
    print(f"\nNew files: {len(new_files)}, Duplicates: {len(dup_files)}")


if __name__ == "__main__":
    main()
