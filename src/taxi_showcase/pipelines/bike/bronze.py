"""CitiBike Ingress \u2013 Step 01: Landing Zone \u2192 Bronze.

Landing zone check, ZIP extraction, CSV read with schema, load to bronze.
Lese-Schema und CSV-Optionen kommen aus der Bike-Pipeline-Konfiguration.
"""

import os
import shutil
import zipfile

from pyspark.sql import DataFrame, SparkSession

from taxi_showcase.pipelines.bike.config import BikeConfig, create_config
from taxi_showcase.pipelines.shared.bronze import run_bronze
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.schema import expected_to_struct_type
from taxi_showcase.schemas.bike import BRONZE_SCHEMA

spark = SparkSession.builder.getOrCreate()


def read_bike_zip(cfg: BikeConfig, zip_path: str) -> DataFrame:
    """Extract a CitiBike ZIP, verify each CSV header, and read all CSVs with the source schema."""
    # One folder per archive keeps lazily-read CSVs available until they are loaded.
    zip_name = os.path.splitext(os.path.basename(zip_path))[0]
    extract_dir = os.path.join(cfg.landing_zone, "_tmp_csvs", zip_name)
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        zf.extractall(extract_dir)
    print(f"Entpackt: {len(csv_names)} CSV-Datei(en) aus {os.path.basename(zip_path)}")

    expected_header = list(cfg.expected_columns)
    for csv_name in csv_names:
        # Check the real header, including order, before applying the schema.
        actual_header = (
            spark.read.options(**cfg.csv_options)
            .csv(os.path.join(extract_dir, csv_name))
            .columns
        )
        if actual_header != expected_header:
            raise ValueError(
                f"Schema-Pruefung fehlgeschlagen fuer '{csv_name}'. "
                f"Erwartete Spalten in Reihenfolge: {expected_header}; "
                f"gefunden: {actual_header}"
            )

    return spark.read.csv(
        os.path.join(extract_dir, "*.csv"),
        schema=expected_to_struct_type(cfg.expected_columns),
        **cfg.csv_options,
    )


def main() -> None:
    # Load pipeline-specific paths and source schema from the catalog argument.
    cfg = create_config(parse_catalog_argument())
    run_bronze(cfg, BRONZE_SCHEMA, lambda path: read_bike_zip(cfg, path))


if __name__ == "__main__":
    # Run the ingestion task when invoked as a Databricks Python file.
    main()