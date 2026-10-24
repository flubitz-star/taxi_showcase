"""File tracking and checksum-based deduplication."""

import hashlib
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TypedDict

from databricks.sdk.runtime import dbutils
from pyspark.sql import Row, SparkSession

from taxi_showcase.pipelines.shared.tracking_schema import (
    TRACKING_COLUMNS,
    TRACKING_SCHEMA,
)
from taxi_showcase.schema import validate_table_schema, write_table

spark = SparkSession.builder.getOrCreate()


class LandingFile(TypedDict):
    file_name: str
    file_path: str
    checksum: str
    file_size: int


def compute_md5(file_path: str) -> str:
    """Calculate a file's MD5 checksum without loading it all into memory."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        # Process fixed-size chunks to keep memory use bounded for large files.
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def list_landing_files(lz_path: str) -> list[LandingFile]:
    """List landing-zone files and return their paths, sizes, and checksums."""
    files = []
    for entry in dbutils.fs.ls(lz_path):
        # Ignore subdirectories; each listed file is tracked independently.
        if not entry.isDir():
            full_path = entry.path.replace("dbfs:", "")
            file_name = os.path.basename(full_path.rstrip("/"))
            checksum = compute_md5(full_path)
            files.append({
                "file_name": file_name,
                "file_path": full_path,
                "checksum": checksum,
                "file_size": entry.size,
            })
            print(f"  {file_name}  ({entry.size} bytes)  -> {checksum}")
    print(f"\nGefundene Dateien in Landing Zone: {len(files)}")
    return files


def _tracking_ddl_from_schema(table_name: str) -> str:
    """Build CREATE TABLE DDL using the shared tracking schema contract."""
    cols = []
    for field in TRACKING_SCHEMA.fields:
        # Preserve non-nullable fields in the generated table definition.
        nullable = "" if field.nullable else " NOT NULL"
        cols.append(f"  {field.name:<14} {field.dataType.simpleString()}{nullable}")
    cols_sql = ",\n".join(cols)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
{cols_sql}
        )
        COMMENT 'Tracking-Tabelle fuer bereits ingested Dateien (Checksum-basierte Deduplizierung)'
    """


def ensure_tracking_table(tracking_table: str) -> None:
    """Create the tracking table if needed and validate its schema."""
    if not spark.catalog.tableExists(tracking_table):
        print(f"Tracking-Tabelle {tracking_table} existiert nicht \u2013 wird erstellt.")
        catalog, schema_name = tracking_table.split(".")[:2]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        spark.sql(_tracking_ddl_from_schema(tracking_table))
        print(f"Tracking-Tabelle {tracking_table} erstellt.")
    # Validate existing tables too, so incompatible metadata fails early.
    validate_table_schema(tracking_table, TRACKING_COLUMNS)


def get_known_checksums(tracking_table: str) -> dict[str, str]:
    """Return known checksums mapped to their registered file names."""
    ensure_tracking_table(tracking_table)
    df = spark.sql(f"SELECT file_name, checksum FROM {tracking_table}")
    # Collect the small tracking metadata set for local checksum comparisons.
    known = {row.checksum: row.file_name for row in df.collect()}
    print(f"Bereits registrierte Dateien in Tracking-Tabelle: {len(known)}")
    return known


def split_new_vs_known(
    files: Sequence[LandingFile],
    known_checksums: Mapping[str, str],
) -> tuple[list[LandingFile], list[LandingFile]]:
    """Split discovered files into unseen checksums and known duplicates."""
    new_files, duplicate_files = [], []
    for f in files:
        # Check content rather than file name so renamed copies are deduplicated.
        if f["checksum"] in known_checksums:
            duplicate_files.append(f)
        else:
            new_files.append(f)

    print("\n--- Ergebnis ---")
    print(f"Neue Dateien:      {len(new_files)}")
    print(f"Bekannte Dateien:  {len(duplicate_files)}")

    if duplicate_files:
        print("\nDuplikate (\u00fcbersprungen):")
        for f in duplicate_files:
            print(f"  {f['file_name']}  (identisch mit '{known_checksums[f['checksum']]}')")
    if new_files:
        print("\nNeue Dateien (zum Ingest bereit):")
        for f in new_files:
            print(f"  {f['file_name']}  -> {f['checksum']}")

    return new_files, duplicate_files


def register_new_files(new_files: Sequence[LandingFile], tracking_table: str) -> int:
    """Append new file metadata to the tracking table and return the count."""
    if not new_files:
        print("Keine neuen Dateien zum Registrieren.")
        return 0

    # Use one registration time for every file in this batch.
    now = datetime.now()
    rows = [
        (f["file_name"], f["file_path"], f["checksum"], f["file_size"], now)
        for f in new_files
    ]
    tracking_df = spark.createDataFrame(rows, TRACKING_SCHEMA)
    write_table(tracking_df, tracking_table, TRACKING_COLUMNS, mode="append")

    print(f"{len(new_files)} neue Datei(en) in {tracking_table} registriert.")
    return len(new_files)


def get_pending_ingest_files(tracking_table: str, bronze_table: str) -> list[Row]:
    """Return tracked files whose names have not yet appeared in Bronze."""
    if spark.catalog.tableExists(bronze_table):
        # A left anti join excludes any file already represented in Bronze.
        return spark.sql(f"""
            SELECT t.file_name, t.file_path
            FROM {tracking_table} t
            LEFT ANTI JOIN {bronze_table} b
              ON t.file_name = b._source_file
        """).collect()

    print("\u26a0\ufe0f  Bronze-Tabelle existiert noch nicht \u2013 Erstladung, alle Dateien werden geladen.")
    # Without Bronze history, every tracked file is pending for the initial load.
    return spark.sql(f"SELECT file_name, file_path FROM {tracking_table}").collect()
