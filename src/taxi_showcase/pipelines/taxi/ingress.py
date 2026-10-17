"""Taxi Ingress \u2013 Step 02: Landing Zone \u2192 Bronze.

Reads Parquet files (NYC TLC Yellow Trip Records) into bronze.
Schema validation against TLC Data Dictionary, schema evolution for new columns.
"""

import os

from pyspark.sql import SparkSession

from taxi_showcase.pipelines.taxi.config import CONFIG
from taxi_showcase.tracking import get_pending_ingest_files
from taxi_showcase.schema import validate_schema, load_with_schema_evolution
from taxi_showcase.ingest_helpers import verify_bronze

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

    # Schema validation against Data Dictionary
    expected_columns = cfg["expected_columns"]

    if not new_files:
        print("Keine neuen Dateien \u2013 Schema-Check gegen bestehende Bronze-Tabelle.")
        bronze_schema = spark.sql(f"DESCRIBE TABLE {bronze_table}").collect()
        actual_cols = {row.col_name: row.data_type for row in bronze_schema}
    else:
        sample_file = new_files[0]
        if os.path.exists(sample_file.file_path):
            print(f"Schema-Check gegen: {sample_file.file_name}")
            raw_schema = spark.read.parquet(sample_file.file_path).schema
            actual_cols = {f.name: f.dataType.simpleString() for f in raw_schema.fields}
        else:
            print(f"\u26a0\ufe0f  Datei nicht gefunden: {sample_file.file_name}")
            bronze_schema = spark.sql(f"DESCRIBE TABLE {bronze_table}").collect()
            actual_cols = {row.col_name: row.data_type for row in bronze_schema}

    validate_schema(actual_cols, expected_columns)

    # Load into Bronze
    if new_files:
        total_rows = 0
        loaded_count = 0
        for f in new_files:
            if not os.path.exists(f.file_path):
                print(f"\u26a0\ufe0f  Datei nicht gefunden, \u00fcberspringe: {f.file_name}")
                continue

            print(f"Lade {f.file_name} ...")
            df = spark.read.parquet(f.file_path)
            row_count = df.count()
            load_with_schema_evolution(df, f.file_name, bronze_table)
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
