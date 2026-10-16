"""Taxi Landing Zone Check \u2013 Step 01.

Lists files in the taxi landing zone, registers new files via checksum tracking.
"""

from taxi_showcase.config import PROJECT_CONFIG
from taxi_showcase.ingest_helpers import run_landing_zone_check


def main():
    cfg = PROJECT_CONFIG["taxi"]
    new_files, dup_files = run_landing_zone_check(cfg)
    print(f"\nNew files: {len(new_files)}, Duplicates: {len(dup_files)}")


if __name__ == "__main__":
    main()
