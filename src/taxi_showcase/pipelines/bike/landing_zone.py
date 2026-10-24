"""Bike Landing Zone Check \u2013 Step 01.

Lists ZIP archives in the bike landing zone, registers new files via checksum tracking.
"""

from taxi_showcase.ingest_helpers import run_landing_zone_check
from taxi_showcase.pipelines.bike.config import create_config
from taxi_showcase.pipelines.shared.config import parse_catalog_argument


def main() -> None:
    # Resolve the same catalog-specific settings used by downstream tasks.
    cfg = create_config(parse_catalog_argument())
    new_files, dup_files = run_landing_zone_check(cfg)
    # The shared helper has registered new files; report this run's discovery.
    print(f"\nNew files: {len(new_files)}, Duplicates: {len(dup_files)}")


if __name__ == "__main__":
    main()
