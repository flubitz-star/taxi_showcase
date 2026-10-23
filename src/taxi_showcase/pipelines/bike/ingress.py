"""CitiBike Ingress \u2013 Step 01: Landing Zone \u2192 Bronze.

Landing zone check, ZIP extraction, CSV read with schema, load to bronze.
Lese-Schema und CSV-Optionen kommen aus der Bike-Pipeline-Konfiguration.
"""

import os
import shutil
import zipfile

from pyspark.sql import SparkSession

from taxi_showcase.ingest_helpers import run_landing_zone_check, verify_bronze
from taxi_showcase.pipelines.bike.config import create_config
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.schema import (
    expected_to_struct_type,
    load_to_bronze,
    validate_dataframe_schema,
    validate_table_schema,
)
from taxi_showcase.schemas.bike import BRONZE_SCHEMA
from taxi_showcase.tracking import get_pending_ingest_files

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    # Load pipeline-specific paths and source schema from the catalog argument.
    cfg = create_config(parse_catalog_argument())
    bronze_table = cfg.bronze_table
    lz_path = cfg.landing_zone

    print(f"Bronze:       {bronze_table}")

    # Discover and register ZIP archives before checking Bronze for pending work.
    run_landing_zone_check(cfg)

    # Select registered files that have not yet been loaded into Bronze.
    pending = get_pending_ingest_files(cfg.tracking_table, bronze_table)
    print(f"\nZu ladende Dateien: {len(pending)}")
    for f in pending:
        print(f"  {f.file_name}")

    # Derive the Spark schema and CSV reader options from the pipeline contract.
    bike_csv_schema = expected_to_struct_type(cfg.expected_columns)
    csv_options = cfg.csv_options
    print(f"Lese-Schema abgeleitet: {len(bike_csv_schema.fields)} Spalten")

    if pending:
        zip_info = pending[0]
        zip_path = zip_info.file_path
        extract_dir = os.path.join(lz_path, "_tmp_csvs")

        # Clear this run's extraction folder to avoid reading stale CSVs.
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir)
        os.makedirs(extract_dir, exist_ok=True)

        # Extract source CSVs from the pending archive into the temporary folder.
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            zf.extractall(extract_dir)
            print(f"Entpackt: {len(csv_names)} CSV-Datei(en) aus {zip_info.file_name}")
            for n in csv_names:
                print(f"  {n}")

        expected_header = list(cfg.expected_columns)
        for csv_name in csv_names:
            csv_file = os.path.join(extract_dir, csv_name)
            # Check the real header, including order, before applying the schema.
            actual_header = spark.read.options(**csv_options).csv(csv_file).columns
            if actual_header != expected_header:
                raise ValueError(
                    f"Schema-Pruefung fehlgeschlagen fuer '{csv_name}'. "
                    f"Erwartete Spalten in Reihenfolge: {expected_header}; "
                    f"gefunden: {actual_header}"
                )

        # Read all extracted CSVs together using the explicit source schema.
        csv_path = os.path.join(extract_dir, "*.csv")
        df = spark.read.csv(csv_path, schema=bike_csv_schema, **csv_options)
        row_count = df.count()
        print(f"\nGelesen: {row_count:,} Zeilen aus {len(csv_names)} CSV-Datei(en)")

        # Confirm parsing produced exactly the expected columns and types.
        validate_dataframe_schema(
            df,
            cfg.expected_columns,
            context=f"Bike-Quelldatei '{zip_info.file_name}'",
        )

        # Append the parsed rows and source-file metadata to Bronze.
        print(f"Lade {zip_info.file_name} ...")
        load_to_bronze(
            df,
            zip_info.file_name,
            bronze_table,
            cfg.expected_columns,
            BRONZE_SCHEMA,
        )
        print(f"\nIngest abgeschlossen: {row_count:,} Zeilen in {bronze_table} geladen.")
    else:
        print("Keine neuen Dateien zum Laden. Bronze ist bereits aktuell.")
        validate_table_schema(bronze_table, BRONZE_SCHEMA)

    verify_bronze(bronze_table)


if __name__ == "__main__":
    # Run the ingestion task when invoked as a Databricks Python file.
    main()
