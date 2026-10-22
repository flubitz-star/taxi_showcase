"""Weather Silver \u2013 Step 03: Bronze \u2192 Silver.

Type casting, dedup, null checks, range validation, completeness check.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format
from pyspark.sql.functions import count as spark_count

from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.weather.config import create_config
from taxi_showcase.schema import validate_table_schema
from taxi_showcase.schemas.weather import BRONZE_SCHEMA, SILVER_SCHEMA
from taxi_showcase.silver_helpers import (
    dedup_keep_first,
    report_null_counts,
    write_silver,
)
from taxi_showcase.timestamp_utils import (
    normalize_timestamp,
    print_timestamp_report,
    validate_timestamp_column,
)

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    cfg = create_config(parse_catalog_argument())
    bronze_table = cfg.bronze_table
    silver_table = cfg.silver_table

    print(f"Bronze: {bronze_table}")
    print(f"Silver: {silver_table}")

    validate_table_schema(bronze_table, BRONZE_SCHEMA)
    df_bronze = spark.table(bronze_table)
    print(f"Zeilen in Bronze: {df_bronze.count():,}")

    # Check 1: Timestamp validation
    ts_result = validate_timestamp_column(df_bronze, "time")
    print_timestamp_report(ts_result, "time")
    if ts_result["unparseable"] > 0:
        raise ValueError("Timestamp-Validierung fehlgeschlagen \u2013 Bronze-Daten pruefen!")

    # Transformation: String -> typed columns
    df_silver = (
        df_bronze
        .transform(
            lambda df: normalize_timestamp(
                df,
                "time",
                output_col="ts",
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
    cols_to_drop = [
        "time", "temperature_2m (\u00b0C)", "rain (mm)", "snowfall (cm)",
        "precipitation (mm)", "snow_depth (m)", "elevation",
    ]
    df_silver = df_silver.drop(*cols_to_drop)
    print(f"\nZeilen nach Transformation: {df_silver.count():,}")

    # Check 2: Dedup (keep latest per hour)
    df_silver, _ = dedup_keep_first(df_silver, ["ts"], order_col="_ingested_at", keep="latest")

    # Check 3: Null checks
    weather_cols = ["temperature_2m_c", "rain_mm", "snowfall_cm", "precipitation_mm", "snow_depth_m"]
    report_null_counts(df_silver, weather_cols)

    # Check 4: Range validation
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

    # Check 5: Completeness (24 hours per day)
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

    # Write Silver table
    write_silver(df_silver, silver_table, SILVER_SCHEMA)

    # Verify
    print("\nStichprobe (5 Zeilen):")
    spark.sql(f"""
        SELECT ts, temperature_2m_c, rain_mm, snowfall_cm,
               precipitation_mm, snow_depth_m
        FROM {silver_table}
        ORDER BY ts
        LIMIT 5
    """).show(truncate=False)


if __name__ == "__main__":
    main()
