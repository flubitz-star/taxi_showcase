"""Taxi Landing Zone Check \u2013 Step 01.

Lists files in the taxi landing zone, registers new files via checksum tracking.
"""

from taxi_showcase.ingest_helpers import publish_pending_files, run_landing_zone_check
from taxi_showcase.pipelines.shared.config import (
    parse_catalog_argument,
    parse_landing_catalog_argument,
)
from taxi_showcase.pipelines.taxi.config import create_config


def main() -> None:
    # Resolve the same catalog-specific settings used by downstream tasks.
    cfg = create_config(parse_catalog_argument(), parse_landing_catalog_argument())
    new_files, dup_files = run_landing_zone_check(cfg)
    # The shared helper has registered new files; report this run's discovery.
    print(f"\nNew files: {len(new_files)}, Duplicates: {len(dup_files)}")
    publish_pending_files(cfg.tracking_table, cfg.bronze_table)


if __name__ == "__main__":
    main()
