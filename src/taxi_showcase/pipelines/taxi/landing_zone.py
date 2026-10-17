"""Taxi Landing Zone Check \u2013 Step 01.

Lists files in the taxi landing zone, registers new files via checksum tracking.
"""

from taxi_showcase.pipelines.taxi.config import CONFIG
from taxi_showcase.ingest_helpers import run_landing_zone_check


def main():
    new_files, dup_files = run_landing_zone_check(CONFIG)
    print(f"\nNew files: {len(new_files)}, Duplicates: {len(dup_files)}")


if __name__ == "__main__":
    main()
