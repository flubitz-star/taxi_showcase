"""Pipeline helpers for Silver (checks & writes)."""

from collections.abc import Mapping, Sequence
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    asc,
    col,
    current_timestamp,
    date_format,
    dayofweek,
    desc,
    isnull,
    lit,
    percentile_approx,
    row_number,
    unix_timestamp,
)
from pyspark.sql.window import Window

from taxi_showcase.schema import write_table
from taxi_showcase.timestamp_utils import (
    print_timestamp_report,
    validate_timestamp_column,
)

spark = SparkSession.builder.getOrCreate()


def validate_timestamp_columns(df: DataFrame, ts_cols: Sequence[str]) -> None:
    """Validate each timestamp column and fail when non-null values cannot parse."""
    for ts_col in ts_cols:
        result = validate_timestamp_column(df, ts_col)
        print_timestamp_report(result, ts_col)
        print()
        if result["unparseable"] > 0:
            raise ValueError(f"Timestamp-Validierung fehlgeschlagen fuer {ts_col}!")


def filter_temporal_outliers(
    df: DataFrame,
    ts_col: str,
    example_cols: Sequence[str] | None = None,
    dlq_reason: str = "outlier_timestamp",
) -> tuple[DataFrame, DataFrame]:
    """Keep timestamps within the allowed range and send outliers to the DLQ."""
    # Use source distribution to define the lower bound, with a fixed upper bound.
    percentiles = df.agg(
        percentile_approx(ts_col, [0.05, 0.95]).alias("pcts")
    ).collect()[0]["pcts"]
    p5_ts, p95_ts = percentiles[0], percentiles[1]

    lower_bound = p5_ts - timedelta(days=30)
    upper_bound = spark.sql("SELECT current_date()").collect()[0][0]

    print("\u2500\u2500 Ausreisser-Erkennung \u2500\u2500")
    print(f"  p5 der {ts_col}-Timestamps:  {p5_ts}")
    print(f"  p95 der {ts_col}-Timestamps: {p95_ts}")
    print(f"  Untere Grenze (p5 - 30T):    {lower_bound}")
    print(f"  Obere Grenze (heute):        {upper_bound}")

    outliers_old = df.filter(col(ts_col) < lower_bound).count()
    outliers_future = df.filter(col(ts_col) > upper_bound).count()
    print(f"  Ausreisser (zu alt):         {outliers_old:,}")
    print(f"  Ausreisser (Zukunft):        {outliers_future:,}")

    if outliers_old > 0 and example_cols:
        print("  Beispiel Ausreisser (zu alt):")
        df.filter(col(ts_col) < lower_bound) \
          .select(*example_cols).limit(5).show(truncate=False)

    total_before = df.count()
    df_filtered = df.filter(
        (col(ts_col) >= lower_bound) &
        (col(ts_col) <= upper_bound)
    )
    total_after = df_filtered.count()
    print(f"\n  Zeilen vor Filter:  {total_before:,}")
    print(f"  Zeilen nach Filter: {total_after:,}")
    print(f"  Entfernt:           {total_before - total_after:,}")

    # Preserve excluded rows with a reason so they can be inspected downstream.
    df_dlq = df.filter(
        (col(ts_col) < lower_bound) |
        (col(ts_col) > upper_bound)
    ).withColumn("_dlq_reason", lit(dlq_reason))
    return df_filtered, df_dlq


def add_trip_features(df: DataFrame, start_col: str, end_col: str) -> DataFrame:
    """Add trip duration, pickup weekday, weekend, and processing-time fields."""
    # Spark's day-of-week convention uses 1 for Sunday and 7 for Saturday.
    return (
        df
        .withColumn("trip_duration_min",
            (unix_timestamp(end_col) - unix_timestamp(start_col)) / 60.0)
        .withColumn("pickup_dayofweek", dayofweek(start_col))
        .withColumn("pickup_weekday", date_format(start_col, "EEEE"))
        .withColumn("is_weekend", dayofweek(start_col).isin(1, 7))
        .withColumn("_silver_processed_at", current_timestamp())
    )


def dedup_keep_first(
    df: DataFrame,
    key_cols: Sequence[str],
    order_col: str = "_ingested_at",
    keep: str = "first",
    dlq_reason: str = "duplicate",
) -> tuple[DataFrame, DataFrame]:
    """Keep one ordered row per key and return the remaining copies for the DLQ."""
    total_before = df.count()
    dup_count = (
        df.groupBy(*key_cols)
        .count()
        .filter("count > 1")
        .count()
    )
    print("-- Duplikat-Pruefung --")
    print(f"  Schluessel:         {', '.join(key_cols)}")
    print(f"  Zeilen vor Dedup:   {total_before:,}")
    print(f"  Duplikat-Gruppen:   {dup_count:,}")

    if dup_count == 0:
        print("  \u2705 Keine Duplikate gefunden.")
        empty_dlq = df.limit(0).withColumn("_dlq_reason", lit(dlq_reason))
        return df, empty_dlq

    # The ordering determines which duplicate is retained; default is earliest.
    order_fn = desc if keep == "latest" else asc
    w = Window.partitionBy(*key_cols).orderBy(order_fn(order_col))
    df_with_rn = df.withColumn("_rn", row_number().over(w))
    df_dlq_dups = (
        df_with_rn.filter("_rn > 1")
        .drop("_rn")
        .withColumn("_dlq_reason", lit(dlq_reason))
    )
    df_dedup = df_with_rn.filter("_rn = 1").drop("_rn")
    total_after = df_dedup.count()
    print(f"  Zeilen nach Dedup:  {total_after:,}")
    print(f"  Entfernt:           {total_before - total_after:,}")
    return df_dedup, df_dlq_dups


def report_null_counts(df: DataFrame, cols: Sequence[str]) -> None:
    """Report null counts for selected columns without filtering any rows."""
    print("\u2500\u2500 Null-Check (wichtige Spalten) \u2500\u2500")
    for c in cols:
        cnt = df.filter(isnull(col(c))).count()
        status = "\u2705" if cnt == 0 else "\u26a0\ufe0f"
        print(f"  {status} {c}: {cnt} NULLs")


def write_dlq(
    df_dlq: DataFrame,
    dlq_table: str,
    expected_schema: Mapping[str, str],
) -> None:
    """Validate and write a DLQ table."""
    # Mark when rejected rows were written for later operational auditing.
    df_dlq = df_dlq.withColumn("_dlq_removed_at", current_timestamp())

    dlq_count = df_dlq.count()
    print("\u2500\u2500 DLQ (Dead Letter Queue) \u2500\u2500")
    print(f"  Gesamt entfernte Zeilen: {dlq_count:,}")
    print("  Gruende:")
    df_dlq.groupBy("_dlq_reason").count().orderBy("count", ascending=False) \
        .show(truncate=False)

    print(f"Schreibe in {dlq_table} ...")
    write_table(df_dlq, dlq_table, expected_schema)
    print(f"\u2705 {dlq_count:,} Zeilen in {dlq_table} geschrieben.")


def write_silver(
    df: DataFrame,
    silver_table: str,
    expected_schema: Mapping[str, str],
) -> None:
    """Validate and write a Silver table."""
    catalog = silver_table.split(".")[0]
    # Ensure the target schema exists before saveAsTable creates the table.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

    print(f"Schreibe in {silver_table} ...")
    write_table(df, silver_table, expected_schema)
    print(f"\u2705 {df.count():,} Zeilen in {silver_table} geschrieben.")
