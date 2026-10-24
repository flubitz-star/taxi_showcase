"""Weather Silver \u2013 Step 03: Bronze \u2192 Silver.

Type casting, dedup, null checks, range validation, completeness check.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit
from pyspark.sql.functions import count as spark_count

from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.weather.config import create_config
from taxi_showcase.schema import expected_to_struct_type, validate_table_schema
from taxi_showcase.schemas.weather import BRONZE_SCHEMA, DLQ_SCHEMA, SILVER_SCHEMA
from taxi_showcase.silver_helpers import (
    dedup_keep_first,
    report_null_counts,
    write_dlq,
    write_silver,
)
from taxi_showcase.timestamp_utils import (
    ISO_MINUTE_FORMAT,
    normalize_timestamp,
    print_timestamp_report,
    validate_timestamp_column,
)

spark = SparkSession.builder.getOrCreate()


def ensure_dlq_table(table_name: str) -> None:
    """Create the Weather DLQ with column mapping for raw source headers."""
    if spark.catalog.tableExists(table_name):
        return

    catalog, schema_name = table_name.split(".")[:2]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

    dlq_schema = expected_to_struct_type(DLQ_SCHEMA)
    col_defs = [
        f"`{field.name.replace('`', '``')}` {field.dataType.simpleString()}"
        for field in dlq_schema.fields
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
    # Resolve the weather Bronze and Silver tables for this catalog.
    cfg = create_config(parse_catalog_argument())
    bronze_table = cfg.bronze_table
    silver_table = cfg.silver_table
    dlq_table = cfg.dlq_table

    print(f"Bronze: {bronze_table}")
    print(f"Silver: {silver_table}")

    validate_table_schema(bronze_table, BRONZE_SCHEMA)
    df_bronze = spark.table(bronze_table)
    print(f"Zeilen in Bronze: {df_bronze.count():,}")

    # Check 1: Timestamp validation
    # Fail early if source timestamps cannot be interpreted as local date-times.
    ts_result = validate_timestamp_column(
        df_bronze, "time", expected_format=ISO_MINUTE_FORMAT
    )
    print_timestamp_report(ts_result, "time")
    if ts_result["unparseable"] > 0:
        raise ValueError("Timestamp-Validierung fehlgeschlagen \u2013 Bronze-Daten pruefen!")

    df_dlq = df_bronze.filter(col("time").isNull()).withColumn(
        "_dlq_reason", lit("null_timestamp")
    )
    df_bronze = df_bronze.filter(col("time").isNotNull())
    print(f"  NULL time in DLQ: {df_dlq.count():,}")

    # Transformation: String -> typed columns
    # Cast raw strings to analytical types and retain processing lineage.
    df_silver = (
        df_bronze
        .transform(
            lambda df: normalize_timestamp(
                df,
                "time",
                output_col="ts",
                expected_format=ISO_MINUTE_FORMAT,
            )
        )
        .withColumn("temperature_2m_c",   col("temperature_2m (\u00b0C)").cast("double"))
        .withColumn("rain_mm",            col("rain (mm)").cast("double"))
        .withColumn("snowfall_cm",        col("snowfall (cm)").cast("double"))
        .withColumn("precipitation_mm",   col("precipitation (mm)").cast("double"))
        .withColumn("snow_depth_m",      col("snow_depth (m)").cast("double"))
        .withColumn("latitude",           col("latitude").cast("double"))
        .withColumn("longitude",          col("longitude").cast("double"))
        .withColumn("utc_offset_seconds", col("utc_offset_seconds").cast("int"))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    print(f"\nZeilen nach Transformation: {df_silver.count():,}")

    # Keep the most recently ingested observation when an hour is duplicated.
    df_silver, df_dlq_dups = dedup_keep_first(
        df_silver,
        ["ts"],
        order_col="_ingested_at",
        keep="latest",
    )
    # Duplicates were already cast to Silver types; restore the raw Bronze types for the DLQ.
    df_dlq_dups = df_dlq_dups.select(
        *[
            col(c).cast(t).alias(c) if c in ("latitude", "longitude", "utc_offset_seconds") else col(c)
            for c, t in BRONZE_SCHEMA.items()
        ]
    ).withColumn("_dlq_reason", lit("duplicate"))
    df_dlq = df_dlq.unionByName(df_dlq_dups)

    cols_to_drop = [
        "time", "temperature_2m (\u00b0C)", "rain (mm)", "snowfall (cm)",
        "precipitation (mm)", "snow_depth (m)", "elevation",
    ]
    df_silver = df_silver.drop(*cols_to_drop)

    # Report missing weather measurements without dropping those observations.
    weather_cols = ["temperature_2m_c", "rain_mm", "snowfall_cm", "precipitation_mm", "snow_depth_m"]
    report_null_counts(df_silver, weather_cols)

    # Measure out-of-range values as quality metrics; do not filter them here.
    print("\n\u2500\u2500 Wertebereich-Check \u2500\u2500")
    range_checks = [
        ("temperature_2m_c", -50.0, 60.0, "Temperatur (\u00b0C)"),
        ("rain_mm",          0.0,  None,   "Regen (mm)"),
        ("snowfall_cm",      0.0,  None,   "Schneefall (cm)"),
        ("precipitation_mm",  0.0,  None,   "Niederschlag (mm)"),
        ("snow_depth_m",     0.0,  None,   "Schneehoehe (m)"),
    ]
    for c, lo, hi, label in range_checks:
        if hi is not None:
            out_of_range = df_silver.filter((col(c) < lo) | (col(c) > hi)).count()
        else:
            out_of_range = df_silver.filter(col(c) < lo).count()
        status = "\u2705" if out_of_range == 0 else "\u26a0\ufe0f"
        hi_display = hi if hi is not None else "\u221e"
        print(f"  {status} {label}: {out_of_range} Werte ausserhalb [{lo}, {hi_display}]")

    # Check that each date has the expected 24 hourly observations.
    print("\n\u2500\u2500 Vollstaendigkeit (Stunden pro Tag) \u2500\u2500")
    hours_per_day = (
        df_silver
        .groupBy(date_format("ts", "yyyy-MM-dd").alias("date"))
        .agg(spark_count("*").alias("hours"))
        .orderBy("date")
    )
    incomplete = hours_per_day.filter("hours < 24").count()
    total_days = hours_per_day.count()
    print(f"  Tage gesamt:       {total_days}")
    print(f"  Tage < 24 Stunden:  {incomplete}")
    if incomplete == 0:
        print("  \u2705 Alle Tage haben 24 Stunden.")
    else:
        print("  \u26a0\ufe0f  Tage mit fehlenden Stunden:")
        hours_per_day.filter("hours < 24").show(truncate=False)

    # Write rejected null-timestamp rows and the Silver table.
    ensure_dlq_table(dlq_table)
    write_dlq(df_dlq, dlq_table, DLQ_SCHEMA)
    write_silver(df_silver, silver_table, SILVER_SCHEMA)

    # Display a small ordered sample as a post-write verification.
    print("\nStichprobe (5 Zeilen):")
    spark.sql(f"""
        SELECT ts, temperature_2m_c, rain_mm, snowfall_cm,
               precipitation_mm, snow_depth_m
        FROM {silver_table}
        ORDER BY ts
        LIMIT 5
    """).show(truncate=False)


if __name__ == "__main__":
    # Execute this file as the weather Silver task.
    main()
