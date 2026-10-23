"""Taxi Silver \u2013 Step 03: Bronze \u2192 Silver.

Timestamp validation, outlier detection, logical consistency, dedup,
passenger_count imputation, range checks, DLQ, write silver.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, lit, when

from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config
from taxi_showcase.schema import validate_table_schema
from taxi_showcase.schemas.taxi import BRONZE_SCHEMA, DLQ_SCHEMA, SILVER_SCHEMA
from taxi_showcase.silver_helpers import (
    add_trip_features,
    dedup_keep_first,
    filter_temporal_outliers,
    report_null_counts,
    validate_timestamp_columns,
    write_dlq,
    write_silver,
)

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    # Resolve source, target, and quarantine tables for the selected catalog.
    cfg = create_config(parse_catalog_argument())
    bronze_table = cfg.bronze_table
    silver_table = cfg.silver_table
    dlq_table = cfg.dlq_table

    print(f"Bronze: {bronze_table}")
    print(f"Silver: {silver_table}")

    # Reject unexpected Bronze table drift before applying transformations.
    validate_table_schema(bronze_table, BRONZE_SCHEMA)
    df_bronze = spark.table(bronze_table)
    print(f"Zeilen in Bronze: {df_bronze.count():,}")

    # Check 1: Timestamp validation
    validate_timestamp_columns(df_bronze, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # Check 2: Temporal outlier detection
    df_filtered, df_dlq = filter_temporal_outliers(
        df_bronze, "tpep_pickup_datetime",
        example_cols=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                      "PULocationID", "trip_distance"],
    )

    # Check 3: Logical consistency (project-specific)
    invalid_duration = df_filtered.filter(
        col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")
    ).count()
    neg_fare = df_filtered.filter(col("fare_amount") < 0).count()
    neg_distance = df_filtered.filter(col("trip_distance") < 0).count()

    print("\u2500\u2500 Logische Konsistenz \u2500\u2500")
    print(f"  Ungueltige Trip-Dauer (dropoff <= pickup): {invalid_duration:,}")
    print(f"  Negativer fare_amount:                     {neg_fare:,} (Flag, nicht entfernt)")
    print(f"  Negativer trip_distance:                   {neg_distance:,}")

    df_consistent = df_filtered.filter(
        col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")
    )
    removed = df_filtered.count() - df_consistent.count()
    print(f"  Entfernt durch Konsistenz-Filter: {removed:,}")

    df_dlq_invalid = df_filtered.filter(
        col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")
    ).withColumn("_dlq_reason", lit("invalid_consistency"))
    # Keep logically invalid trips available for investigation in the DLQ.
    df_dlq = df_dlq.unionByName(df_dlq_invalid, allowMissingColumns=True)

    # Add derived columns
    df_silver = add_trip_features(df_consistent, "tpep_pickup_datetime", "tpep_dropoff_datetime")
    # Flag negative fares (not removed)
    df_silver = df_silver.withColumn("_has_negative_fare", col("fare_amount") < 0)
    print("  Abgeleitete Spalten: trip_duration_min, _has_negative_fare, pickup_dayofweek, pickup_weekday, is_weekend")
    print(f"  Zeilen nach Konsistenz-Filter: {df_silver.count():,}")

    # Check 4: Keep the first trip for each business-key combination.
    dup_keys = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID", "trip_distance", "fare_amount",
    ]
    df_silver, df_dlq_dups = dedup_keep_first(df_silver, dup_keys)
    df_dlq = df_dlq.unionByName(df_dlq_dups, allowMissingColumns=True)

    # Check 5: Null handling (passenger_count imputation)
    null_passengers = df_silver.filter(isnull(col("passenger_count"))).count()
    print("\n\u2500\u2500 Null-Behandlung \u2500\u2500")
    print(f"  passenger_count NULLs vor Imputation: {null_passengers:,}")
    if null_passengers > 0:
        df_silver = df_silver.withColumn(
            "_passenger_count_imputed",
            when(isnull(col("passenger_count")), True).otherwise(False)
        )
        df_silver = df_silver.fillna(1, subset=["passenger_count"])
    else:
        df_silver = df_silver.withColumn("_passenger_count_imputed", lit(False))

    print("  -> NULLs mit 1 gefuellt, _passenger_count_imputed Flag gesetzt.")

    report_null_counts(df_silver, [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "trip_distance", "fare_amount", "total_amount", "passenger_count",
    ])

    # Range checks (metrics, no filter)
    print("\n\u2500\u2500 Wertebereich-Check \u2500\u2500")
    range_checks = [
        ("trip_distance", 0.0, 500.0, "trip_distance (mi)"),
        ("fare_amount",  -100.0, 1000.0, "fare_amount ($)"),
        ("total_amount", -100.0, 1000.0, "total_amount ($)"),
        ("trip_duration_min", 0.0, 600.0, "trip_duration (min)"),
    ]
    for c, lo, hi, label in range_checks:
        out_of_range = df_silver.filter((col(c) < lo) | (col(c) > hi)).count()
        status = "\u2705" if out_of_range == 0 else "\u26a0\ufe0f"
        print(f"  {status} {label}: {out_of_range} Werte ausserhalb [{lo}, {hi}]")

    # Write DLQ and Silver
    write_dlq(df_dlq, dlq_table, DLQ_SCHEMA)
    write_silver(df_silver, silver_table, SILVER_SCHEMA)


if __name__ == "__main__":
    main()
