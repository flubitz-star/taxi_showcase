"""Taxi Ingress \u2013 Step 02: Landing Zone \u2192 Bronze.

Reads Parquet files (NYC TLC Yellow Trip Records) into bronze.
Strict schema validation against the declared source contract.
"""

import os

from pyspark.sql import SparkSession

from taxi_showcase.ingest_helpers import verify_bronze
from taxi_showcase.pipelines.taxi.config import CONFIG
from taxi_showcase.schema import (
    load_to_bronze,
    validate_dataframe_schema,
    validate_table_schema,
)
from taxi_showcase.schemas.taxi import BRONZE_SCHEMA
from taxi_showcase.tracking import get_pending_ingest_files

spark = SparkSession.builder.getOrCreate()


def main():
    cfg = CONFIG
    tracking_table = cfg["tracking_table"]
    bronze_table = cfg["bronze_table"]

    print(f"Bronze:   {bronze_table}")
    print(f"Tracking: {tracking_table}")

    new_files = get_pending_ingest_files(tracking_table, bronze_table)
    print(f"\nZu ladende Dateien: {len(new_files)}")
    for f in new_files:
        print(f"  {f.file_name}")

    if not new_files:
        print("Keine neuen Dateien \u2013 Schema-Check gegen bestehende Bronze-Tabelle.")
        validate_table_schema(bronze_table, BRONZE_SCHEMA)
    else:
        pending_dataframes = []
        for file_info in new_files:
            if not os.path.exists(file_info.file_path):
                print(f"\u26a0\ufe0f  Datei nicht gefunden: {file_info.file_name}")
                continue
            print(f"Schema-Check gegen: {file_info.file_name}")
            raw_df = spark.read.parquet(file_info.file_path)
            validate_dataframe_schema(
                raw_df,
                cfg["expected_columns"],
                context=f"Quelldatei '{file_info.file_name}'",
            )
            pending_dataframes.append((file_info, raw_df))

    # Load into Bronze
    if new_files:
        total_rows = 0
        loaded_count = 0
        for f, df in pending_dataframes:
            print(f"Lade {f.file_name} ...")
            row_count = df.count()
            load_to_bronze(
                df,
                f.file_name,
                bronze_table,
                cfg["expected_columns"],
                BRONZE_SCHEMA,
            )
            total_rows += row_count
            loaded_count += 1
            print(f"  -> {row_count:,} Zeilen geladen")

        if loaded_count > 0:
            print(f"\nIngest abgeschlossen: {loaded_count} Datei(en), {total_rows:,} Zeilen in {bronze_table} geladen.")
        else:
            print("\nKeine Dateien geladen.")
    else:
        print("Keine neuen Dateien zum Laden. Bronze ist bereits aktuell.")

    verify_bronze(bronze_table)


if __name__ == "__main__":
    main()
