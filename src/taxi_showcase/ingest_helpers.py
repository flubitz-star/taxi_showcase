"""Pipeline helpers for ingest (Landing Zone -> Bronze)."""

from pyspark.sql import SparkSession

from taxi_showcase.pipelines.shared.config import LandingZoneConfig
from taxi_showcase.tracking import (
    LandingFile,
    ensure_tracking_table,
    get_known_checksums,
    list_landing_files,
    register_new_files,
    split_new_vs_known,
)

spark = SparkSession.builder.getOrCreate()


def run_landing_zone_check(
    project_cfg: LandingZoneConfig,
) -> tuple[list[LandingFile], list[LandingFile]]:
    """Kompletter Landing-Zone-Check fuer ein Projekt (Schritt 01)."""
    lz_path = project_cfg.landing_zone
    tracking_table = project_cfg.tracking_table
    print(f"Landing Zone: {lz_path}")
    print(f"Tracking:     {tracking_table}\n")
    files = list_landing_files(lz_path)
    ensure_tracking_table(tracking_table)
    known = get_known_checksums(tracking_table)
    new_files, dup_files = split_new_vs_known(files, known)
    register_new_files(new_files, tracking_table)
    return new_files, dup_files


def verify_bronze(bronze_table: str) -> None:
    """Zeigt Zeilen pro Quelldatei in einer Bronze-Tabelle (Verifikation nach dem Ingest)."""
    print(f"Uebersicht {bronze_table}:")
    spark.sql(f"""
        SELECT _source_file,
               COUNT(*) AS row_count,
               MIN(_ingested_at) AS first_loaded,
               MAX(_ingested_at) AS last_loaded
        FROM {bronze_table}
        GROUP BY _source_file
        ORDER BY first_loaded DESC
    """).show(truncate=False)
