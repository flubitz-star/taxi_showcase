"""Weather Ingress \u2013 Step 02: Landing Zone \u2192 Bronze.

Reads Open-Meteo CSV files (3-line header format) into bronze as raw strings.
Metadata from CSV lines 1+2 integrated as columns. Column Mapping Mode for special chars.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from taxi_showcase.ingest_helpers import verify_bronze
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.weather.config import create_config
from taxi_showcase.schema import (
    load_to_bronze,
    validate_dataframe_schema,
    validate_table_schema,
)
from taxi_showcase.schemas.weather import BRONZE_SCHEMA
from taxi_showcase.tracking import get_pending_ingest_files

spark = SparkSession.builder.getOrCreate()


def read_open_meteo_csv(file_path: str) -> DataFrame:
    """
    Reads an Open-Meteo CSV file raw.
    - Metadata from lines 1+2 as additional string columns
    - Data column names from line 3 (original, including units)
    - All values as string (no type casting)
    - Only data rows (timestamp pattern) are written
    """
    raw = spark.read.csv(file_path, header=False)

    header_rows = raw.take(3)
    meta_names = [str(header_rows[0][i]) for i in range(len(header_rows[0]))]
    meta_values = [str(header_rows[1][i]) for i in range(len(header_rows[1]))]
    data_names = [str(header_rows[2][i]) for i in range(len(header_rows[2]))]

    meta_dict = dict(zip(meta_names, meta_values))

    data = raw.filter(col("_c0").rlike("^\\d{4}-\\d{2}-\\d{2}"))

    select_exprs = [lit(meta_dict[name]).alias(name) for name in meta_names]
    select_exprs += [col(f"_c{i}").alias(data_names[i]) for i in range(len(data_names))]
    return data.select(*select_exprs)


def main() -> None:
    cfg = create_config(parse_catalog_argument())
    tracking_table = cfg.tracking_table
    bronze_table = cfg.bronze_table

    print(f"Bronze:   {bronze_table}")
    print(f"Tracking: {tracking_table}")

    new_files = get_pending_ingest_files(tracking_table, bronze_table)
    print(f"\nZu ladende Dateien: {len(new_files)}")
    for f in new_files:
        print(f"  {f.file_name}")

    pending_dataframes = []
    for file_info in new_files:
        if not os.path.exists(file_info.file_path):
            print(f"\u26a0\ufe0f  Datei nicht gefunden: {file_info.file_name}")
            continue
        print(f"Schema-Check gegen: {file_info.file_name}")
        raw_df = read_open_meteo_csv(file_info.file_path)
        validate_dataframe_schema(
            raw_df,
            cfg.expected_columns,
            context=f"Quelldatei '{file_info.file_name}'",
        )
        pending_dataframes.append((file_info, raw_df))

    # Load into Bronze (with Column Mapping for special characters)
    if pending_dataframes:
        if not spark.catalog.tableExists(bronze_table):
            sample_df = pending_dataframes[0][1]
            col_defs = [f"`{f.name}` {f.dataType.simpleString()}" for f in sample_df.schema.fields]
            col_defs.append("_source_file string")
            col_defs.append("_ingested_at timestamp")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {bronze_table} (
                    {', '.join(col_defs)}
                ) TBLPROPERTIES (
                    'delta.columnMapping.mode' = 'name',
                    'delta.minReaderVersion'    = '2',
                    'delta.minWriterVersion'   = '5'
                )
            """)
            print(f"Tabelle {bronze_table} mit Column Mapping erstellt.")

        total_rows = 0
        loaded_count = 0
        for f, df in pending_dataframes:
            if not os.path.exists(f.file_path):
                print(f"\u26a0\ufe0f  Datei nicht gefunden, \u00fcberspringe: {f.file_name}")
                continue

            print(f"Lade {f.file_name} ...")
            row_count = df.count()
            load_to_bronze(
                df,
                f.file_name,
                bronze_table,
                cfg.expected_columns,
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
        validate_table_schema(bronze_table, BRONZE_SCHEMA)

    verify_bronze(bronze_table)


if __name__ == "__main__":
    main()
