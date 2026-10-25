"""Taxi Silver \u2013 Step 03: Bronze \u2192 Silver.

Timestamp validation, outlier detection, logical consistency, dedup,
passenger_count imputation, range checks, DLQ, write silver.

With an existing Silver table, the default is incremental: only Bronze files
not yet represented in Silver or the DLQ are processed and appended. Trips
already present in Silver are skipped as duplicates. Use --full-refresh to
rebuild Silver and the DLQ from all Bronze data.
"""

import argparse
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnull, lit, when
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import min as min_

from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config
from taxi_showcase.pipelines.taxi.schema import EXPECTED_COLUMNS
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

OUTLIER_WINDOW = timedelta(days=30)


def _incremental_requested() -> bool:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--incremental", dest="incremental", action="store_true")
    mode.add_argument("--full-refresh", dest="incremental", action="store_false")
    parser.set_defaults(incremental=True)
    args, _ = parser.parse_known_args()
    return args.incremental


def _select_unprocessed(df_bronze: DataFrame, silver_table: str, dlq_table: str) -> DataFrame:
    """Keep Bronze rows of files that neither Silver nor the DLQ contain yet."""
    processed = spark.table(silver_table).select("_source_file")
    if spark.catalog.tableExists(dlq_table):
        processed = processed.union(spark.table(dlq_table).select("_source_file"))
    return df_bronze.join(processed.distinct(), "_source_file", "left_anti")


def _existing_lower_bound(silver_table: str) -> datetime:
    first_pickup = spark.table(silver_table).agg(
        min_("tpep_pickup_datetime")
    ).collect()[0][0]
    return first_pickup - OUTLIER_WINDOW


def _drop_already_seen(df: DataFrame, silver_table: str) -> tuple[DataFrame, DataFrame]:
    """Split off trips that already exist in Silver (all source columns equal)."""
    row = df.agg(
        min_("tpep_pickup_datetime").alias("lo"), max_("tpep_pickup_datetime").alias("hi")
    ).collect()[0]
    # Silver stores imputed passenger counts; restore NULL to compare with the source.
    existing = spark.table(silver_table).filter(
        col("tpep_pickup_datetime").between(row["lo"], row["hi"])
    ).select(*[
        when(col("_passenger_count_imputed"), lit(None)).otherwise(col(c)).alias(c)
        if c == "passenger_count" else col(c)
        for c in EXPECTED_COLUMNS
    ])
    same_trip = reduce(
        lambda a, b: a & b,
        [df[c].eqNullSafe(existing[c]) for c in EXPECTED_COLUMNS],
    )
    df_new = df.join(existing, same_trip, "left_anti")
    df_seen = df.join(existing, same_trip, "left_semi").withColumn(
        "_dlq_reason", lit("duplicate")
    )
    return df_new, df_seen


def main() -> None:
    # Resolve source, target, and quarantine tables for the selected catalog.
    cfg = create_config(parse_catalog_argument())
    bronze_table = cfg.bronze_table
    silver_table = cfg.silver_table
    dlq_table = cfg.dlq_table
    incremental = _incremental_requested() and spark.catalog.tableExists(silver_table)

    print(f"Bronze: {bronze_table}")
    print(f"Silver: {silver_table}")
    print(f"Modus:  {'inkrementell' if incremental else 'Full Refresh'}")

    # Reject unexpected Bronze table drift before applying transformations.
    validate_table_schema(bronze_table, BRONZE_SCHEMA)
    df_bronze = spark.table(bronze_table)
    lower_bound = None
    if incremental:
        validate_table_schema(silver_table, SILVER_SCHEMA)
        df_bronze = _select_unprocessed(df_bronze, silver_table, dlq_table)
        lower_bound = _existing_lower_bound(silver_table)
    bronze_count = df_bronze.count()
    print(f"Zeilen in Bronze: {bronze_count:,}")
    if incremental and bronze_count == 0:
        print("Keine neuen Bronze-Dateien \u2013 Silver ist bereits aktuell.")
        return

    # Check 1: Timestamp validation
    validate_timestamp_columns(df_bronze, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # Check 2: Temporal outlier detection
    df_filtered, df_dlq = filter_temporal_outliers(
        df_bronze, "tpep_pickup_datetime",
        example_cols=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                      "PULocationID", "trip_distance"],
        lower_bound=lower_bound,
    )

    # Check 3: Logical consistency (project-specific)
    invalid_duration = df_filtered.filter(
        col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")
    ).count()
    null_dropoff = df_filtered.filter(isnull(col("tpep_dropoff_datetime"))).count()
    neg_fare = df_filtered.filter(col("fare_amount") < 0).count()
    neg_distance = df_filtered.filter(col("trip_distance") < 0).count()

    print("\u2500\u2500 Logische Konsistenz \u2500\u2500")
    print(f"  Ungueltige Trip-Dauer (dropoff <= pickup): {invalid_duration:,}")
    print(f"  NULL tpep_dropoff_datetime:                {null_dropoff:,} (DLQ)")
    print(f"  Negativer fare_amount:                     {neg_fare:,} (Flag, nicht entfernt)")
    print(f"  Negativer trip_distance:                   {neg_distance:,}")

    df_consistent = df_filtered.filter(
        col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")
    )
    removed = df_filtered.count() - df_consistent.count()
    print(f"  Entfernt durch Konsistenz-Filter: {removed:,}")

    invalid_consistency = (
        isnull(col("tpep_dropoff_datetime"))
        | (col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"))
    )
    df_dlq_invalid = df_filtered.filter(invalid_consistency).withColumn(
        "_dlq_reason",
        when(isnull(col("tpep_dropoff_datetime")), lit("null_timestamp"))
        .otherwise(lit("invalid_consistency")),
    )
    # Keep logically invalid trips available for investigation in the DLQ.
    df_dlq = df_dlq.unionByName(df_dlq_invalid, allowMissingColumns=True)

    # Add derived columns
    # Add derived trip features such as duration, pickup day, and weekend flag.
    df_silver = add_trip_features(df_consistent, "tpep_pickup_datetime", "tpep_dropoff_datetime")

    # Flag negative fares (not removed)
    df_silver = df_silver.withColumn("_has_negative_fare", col("fare_amount") < 0)
    print("  Abgeleitete Spalten: trip_duration_min, _has_negative_fare, pickup_dayofweek, pickup_weekday, is_weekend")
    print(f"  Zeilen nach Konsistenz-Filter: {df_silver.count():,}")

    # Only exact duplicate source records are duplicates; ingestion metadata is
    # intentionally excluded so re-ingested copies can still be identified.
    dup_keys = list(EXPECTED_COLUMNS)
    df_silver, df_dlq_dups = dedup_keep_first(df_silver, dup_keys)
    df_dlq = df_dlq.unionByName(df_dlq_dups, allowMissingColumns=True)
    if incremental:
        # Trips already in Silver (e.g. re-delivered snippets) are not appended again.
        df_silver, df_dlq_seen = _drop_already_seen(df_silver, silver_table)
        df_dlq = df_dlq.unionByName(df_dlq_seen, allowMissingColumns=True)

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

    report_null_counts(df_silver, [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "trip_distance", "fare_amount", "total_amount", "passenger_count",
    ])

    # Range checks (metrics, no filter)
    print("\n\u2500\u2500 Wertebereich-Check \u2500\u2500")
    range_checks = [
        ("trip_distance", 0.0, 500.0, "trip_distance (mi)"),
        ("fare_amount",    0.0, 1000.0, "fare_amount ($)"),
        ("total_amount",   0.0, 1000.0, "total_amount ($)"),
        ("trip_duration_min", 0.0, 600.0, "trip_duration (min)"),
    ]
    for c, lo, hi, label in range_checks:
        out_of_range = df_silver.filter((col(c) < lo) | (col(c) > hi)).count()
        status = "\u2705" if out_of_range == 0 else "\u26a0\ufe0f"
        print(f"  {status} {label}: {out_of_range} Werte ausserhalb [{lo}, {hi}]")

    # Write DLQ and Silver
    # Silver first: a file already in Silver or the DLQ counts as processed.
    mode = "append" if incremental else "overwrite"
    write_silver(df_silver, silver_table, SILVER_SCHEMA, mode=mode)
    write_dlq(df_dlq, dlq_table, DLQ_SCHEMA, mode=mode)


if __name__ == "__main__":
    main()
