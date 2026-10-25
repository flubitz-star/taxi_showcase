"""Pipeline helpers for ingest (Landing Zone -> Bronze)."""

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from taxi_showcase.pipelines.shared.config import LandingZoneConfig
from taxi_showcase.tracking import (
    LandingFile,
    ensure_tracking_table,
    get_known_checksums,
    get_known_file_checksums,
    get_pending_ingest_files,
    list_landing_files,
    register_new_files,
    split_new_vs_known,
)

# Reuse the active Spark session for table and DataFrame operations.
spark = SparkSession.builder.getOrCreate()


def run_landing_zone_check(
    project_cfg: LandingZoneConfig,
) -> tuple[list[LandingFile], list[LandingFile]]:
    """Find, deduplicate, and register files in a project's landing zone."""
    # Read the project-specific source path and file-tracking table.
    lz_path = project_cfg.landing_zone
    tracking_table = project_cfg.tracking_table
    print(f"Landing Zone: {lz_path}")
    print(f"Tracking:     {tracking_table}\n")

    # List the files currently available and make sure their tracking table exists.
    # Tracked files with unchanged name and size reuse their checksum instead of
    # being hashed again, so runs do not re-read the whole landing zone.
    ensure_tracking_table(tracking_table)
    files = list_landing_files(lz_path, get_known_file_checksums(tracking_table))

    # Compare file checksums so renamed copies of known files are also recognized.
    known = get_known_checksums(tracking_table)
    new_files, dup_files = split_new_vs_known(files, known)

    # Register only new files for the following Bronze step.
    register_new_files(new_files, tracking_table)
    return new_files, dup_files


def publish_pending_files(tracking_table: str, bronze_table: str) -> int:
    """Count tracked files missing in Bronze and expose the count to the job.

    Every tracked file counts, not only this run's new ones, so files left over
    from a failed earlier run are still picked up. The job's condition task
    reads the task value to skip Bronze and Silver when nothing is pending.
    """
    pending = len(get_pending_ingest_files(tracking_table, bronze_table))
    print(f"Pending files for Bronze: {pending}")
    dbutils.jobs.taskValues.set(key="pending_files", value=pending)
    return pending


def verify_bronze(bronze_table: str) -> None:
    """Show row counts and ingestion timestamps for each source file in Bronze."""
    print(f"Uebersicht {bronze_table}:")

    # Summarize each input file to verify what was loaded and when.
    spark.sql(f"""
        SELECT _source_file,
               COUNT(*) AS row_count,
               MIN(_ingested_at) AS first_loaded,
               MAX(_ingested_at) AS last_loaded
        FROM {bronze_table}
        GROUP BY _source_file
        ORDER BY first_loaded DESC
    """).show(truncate=False)
