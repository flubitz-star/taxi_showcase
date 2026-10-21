"""File tracking and checksum-based deduplication."""

import hashlib
import os
from datetime import datetime

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from taxi_showcase.pipelines.shared.tracking_schema import TRACKING_SCHEMA

spark = SparkSession.builder.getOrCreate()


def compute_md5(file_path):
    """Berechnet die MD5-Checksumme einer Datei (chunked, speicherschonend)."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def list_landing_files(lz_path):
    """Listet alle Dateien in einer Landing Zone inkl. MD5-Checksumme auf."""
    files = []
    for entry in dbutils.fs.ls(lz_path):
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


def _tracking_ddl_from_schema(table_name):
    """Generiert CREATE TABLE DDL aus TRACKING_SCHEMA."""
    cols = []
    for field in TRACKING_SCHEMA.fields:
        nullable = "" if field.nullable else " NOT NULL"
        cols.append(f"  {field.name:<14} {field.dataType.simpleString()}{nullable}")
    cols_sql = ",\n".join(cols)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
{cols_sql}
        )
        COMMENT 'Tracking-Tabelle fuer bereits ingested Dateien (Checksum-basierte Deduplizierung)'
    """


def ensure_tracking_table(tracking_table):
    """Stellt sicher, dass die Tracking-Tabelle existiert (CREATE TABLE IF NOT EXISTS)."""
    if not spark.catalog.tableExists(tracking_table):
        print(f"Tracking-Tabelle {tracking_table} existiert nicht \u2013 wird erstellt.")
        spark.sql(_tracking_ddl_from_schema(tracking_table))
        print(f"Tracking-Tabelle {tracking_table} erstellt.")


def get_known_checksums(tracking_table):
    """Laedt bekannte Checksums aus der Tracking-Tabelle -> {checksum: file_name}."""
    ensure_tracking_table(tracking_table)
    df = spark.sql(f"SELECT file_name, checksum FROM {tracking_table}")
    known = {row.checksum: row.file_name for row in df.collect()}
    print(f"Bereits registrierte Dateien in Tracking-Tabelle: {len(known)}")
    return known


def split_new_vs_known(files, known_checksums):
    """Trennt Dateien anhand der Checksumme in neue und bereits bekannte."""
    new_files, duplicate_files = [], []
    for f in files:
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


def register_new_files(new_files, tracking_table):
    """Traegt neue Dateien in die Tracking-Tabelle ein (append)."""
    if not new_files:
        print("Keine neuen Dateien zum Registrieren.")
        return 0

    now = datetime.now()
    rows = [
        (f["file_name"], f["file_path"], f["checksum"], f["file_size"], now)
        for f in new_files
    ]
    spark.createDataFrame(rows, TRACKING_SCHEMA).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(tracking_table)

    print(f"{len(new_files)} neue Datei(en) in {tracking_table} registriert.")
    return len(new_files)


def get_pending_ingest_files(tracking_table, bronze_table):
    """Dateien, die registriert, aber noch nicht in Bronze geladen wurden (LEFT ANTI JOIN)."""
    if spark.catalog.tableExists(bronze_table):
        return spark.sql(f"""
            SELECT t.file_name, t.file_path
            FROM {tracking_table} t
            LEFT ANTI JOIN {bronze_table} b
              ON t.file_name = b._source_file
        """).collect()

    print("\u26a0\ufe0f  Bronze-Tabelle existiert noch nicht \u2013 Erstladung, alle Dateien werden geladen.")
    return spark.sql(f"SELECT file_name, file_path FROM {tracking_table}").collect()
