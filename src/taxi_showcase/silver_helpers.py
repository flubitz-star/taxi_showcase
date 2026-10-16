"""Pipeline helpers for Silver (checks & writes)."""

from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, isnull, current_timestamp,
    unix_timestamp, dayofweek, date_format,
    percentile_approx, row_number, asc, desc,
)
from pyspark.sql.window import Window

from taxi_showcase.timestamp_utils import (
    validate_timestamp_column, print_timestamp_report,
)

spark = SparkSession.builder.getOrCreate()


def validate_timestamp_columns(df, ts_cols):
    """Check 1: Validiert mehrere Timestamp-Spalten und bricht bei Fehlern ab."""
    for ts_col in ts_cols:
        result = validate_timestamp_column(df, ts_col)
        print_timestamp_report(result, ts_col)
        print()
        if result["unparseable"] > 0:
            raise ValueError(f"Timestamp-Validierung fehlgeschlagen fuer {ts_col}!")


def filter_temporal_outliers(df, ts_col, example_cols=None, dlq_reason="outlier_timestamp"):
    """Check 2: Entfernt zeitliche Ausreisser."""
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

    df_dlq = df.filter(
        (col(ts_col) < lower_bound) |
        (col(ts_col) > upper_bound)
    ).withColumn("_dlq_reason", lit(dlq_reason))
    return df_filtered, df_dlq


def add_trip_features(df, start_col, end_col):
    """Fuegt abgeleitete Spalten hinzu (trip_duration_min, weekday, is_weekend)."""
    return (
        df
        .withColumn("trip_duration_min",
            (unix_timestamp(end_col) - unix_timestamp(start_col)) / 60.0)
        .withColumn("pickup_dayofweek", dayofweek(start_col))
        .withColumn("pickup_weekday", date_format(start_col, "EEEE"))
        .withColumn("is_weekend", dayofweek(start_col).isin(1, 7))
        .withColumn("_silver_processed_at", current_timestamp())
    )


def dedup_keep_first(df, key_cols, order_col="_ingested_at",
                     keep="first", dlq_reason="duplicate"):
    """Check 4: Entfernt Duplikate je key_cols."""
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


def report_null_counts(df, cols):
    """Check 5: Null-Zaehlung fuer wichtige Spalten (Report, kein Filter)."""
    print("\u2500\u2500 Null-Check (wichtige Spalten) \u2500\u2500")
    for c in cols:
        cnt = df.filter(isnull(col(c))).count()
        status = "\u2705" if cnt == 0 else "\u26a0\ufe0f"
        print(f"  {status} {c}: {cnt} NULLs")


def write_dlq(df_dlq, dlq_table):
    """Schreibt die DLQ-Tabelle (overwrite, mergeSchema)."""
    df_dlq = df_dlq.withColumn("_dlq_removed_at", current_timestamp())

    dlq_count = df_dlq.count()
    print("\u2500\u2500 DLQ (Dead Letter Queue) \u2500\u2500")
    print(f"  Gesamt entfernte Zeilen: {dlq_count:,}")
    print("  Gruende:")
    df_dlq.groupBy("_dlq_reason").count().orderBy("count", ascending=False) \
        .show(truncate=False)

    print(f"Schreibe in {dlq_table} ...")
    (
        df_dlq.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(dlq_table)
    )
    print(f"\u2705 {dlq_count:,} Zeilen in {dlq_table} geschrieben.")


def write_silver(df, silver_table):
    """Schreibt die Silver-Tabelle (overwrite, mergeSchema)."""
    catalog = silver_table.split(".")[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

    print(f"Schreibe in {silver_table} ...")
    (
        df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
    print(f"\u2705 {df.count():,} Zeilen in {silver_table} geschrieben.")
