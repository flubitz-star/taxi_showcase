"""Weather Ingress \u2013 Step 02: Landing Zone \u2192 Bronze.

Reads Open-Meteo CSV files (3-line header format) into bronze as raw strings.
Metadata from CSV lines 1+2 integrated as columns. Column Mapping Mode for special chars.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from taxi_showcase.pipelines.shared.bronze import run_bronze
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.weather.config import create_config
from taxi_showcase.schema import expected_to_struct_type
from taxi_showcase.schemas.weather import BRONZE_SCHEMA

spark = SparkSession.builder.getOrCreate()


def read_open_meteo_csv(file_path: str) -> DataFrame:
    """
    Reads an Open-Meteo CSV file raw.
    - Metadata from lines 1+2 as additional string columns
    - Data column names from line 3 (original, including units)
    - All values as string (no type casting)
    - Only data rows (timestamp pattern) are written
    """
    # Read without a header so the three metadata/header rows remain available.
    raw = spark.read.csv(file_path, header=False)

    # Open-Meteo places metadata names, metadata values, and data headers in rows 1-3.
    header_rows = raw.take(3)
    meta_names = [str(header_rows[0][i]) for i in range(len(header_rows[0]))]
    meta_values = [str(header_rows[1][i]) for i in range(len(header_rows[1]))]
    data_names = [str(header_rows[2][i]) for i in range(len(header_rows[2]))]

    meta_dict = dict(zip(meta_names, meta_values))

    # Keep only rows whose first field begins with an ISO-format date.
    data = raw.filter(col("_c0").rlike("^\\d{4}-\\d{2}-\\d{2}"))

    # Repeat file-level metadata on each observation and rename data columns.
    select_exprs = [lit(meta_dict[name]).alias(name) for name in meta_names]
    select_exprs += [col(f"_c{i}").alias(data_names[i]) for i in range(len(data_names))]
    return data.select(*select_exprs)


def ensure_bronze_table(table_name: str) -> None:
    """Create Weather Bronze with column mapping for source headers with special characters."""
    if spark.catalog.tableExists(table_name):
        return

    # The schema does not exist yet in a fresh environment catalog.
    catalog, schema_name = table_name.split(".")[:2]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

    bronze_schema = expected_to_struct_type(BRONZE_SCHEMA)
    col_defs = [
        f"`{field.name.replace('`', '``')}` {field.dataType.simpleString()}"
        for field in bronze_schema.fields
    ]
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(col_defs)}
        ) TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion'    = '2',
            'delta.minWriterVersion'   = '5'
        )
    """)
    print(f"Tabelle {table_name} mit Column Mapping erstellt.")


def main() -> None:
    # Resolve the catalog-specific tracking and Bronze tables.
    cfg = create_config(parse_catalog_argument())
    # Open-Meteo headers contain spaces and special characters (e.g. temperature_2m (°C)),
    # so Bronze is created up front with name-based column mapping.
    run_bronze(
        cfg,
        BRONZE_SCHEMA,
        read_open_meteo_csv,
        prepare_table=ensure_bronze_table,
    )


if __name__ == "__main__":
    # Execute this file as the weather bronze task.
    main()
