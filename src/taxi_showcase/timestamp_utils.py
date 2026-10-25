"""Timestamp validation and normalization functions."""

from datetime import datetime
from typing import TypedDict

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp_ntz,
    try_to_timestamp,
    when,
)
from pyspark.sql.functions import (
    count as _spark_count,
)
from pyspark.sql.functions import (
    max as _spark_max,
)
from pyspark.sql.functions import (
    min as _spark_min,
)
from pyspark.sql.types import StringType

# Open-Meteo local timestamps, e.g. 2024-01-31T23:00.
ISO_MINUTE_FORMAT = "yyyy-MM-dd'T'HH:mm"


class TimestampValidationResult(TypedDict):
    total_rows: int
    parseable: int
    unparseable: int
    null_count: int
    min_ts: datetime | None
    max_ts: datetime | None
    unparseable_sample: list[str]


def _try_parse(df: DataFrame, col_name: str, expected_format: str | None) -> Column:
    """Parse to a timestamp, yielding NULL (not an error) for invalid values."""
    column = col(col_name)
    # Formats only apply to strings; typed timestamp columns are already valid.
    if isinstance(df.schema[col_name].dataType, StringType) and expected_format:
        return try_to_timestamp(column, lit(expected_format))
    return try_to_timestamp(column)


def validate_timestamp_column(
    df: DataFrame,
    col_name: str,
    expected_format: str | None = None,
) -> TimestampValidationResult:
    """Check whether non-null values in a column can be parsed as timestamps."""
    parsed_expr = _try_parse(df, col_name, expected_format)
    df_checked = df.withColumn("_ts_check", parsed_expr)

    # One scan computes every metric; null inputs stay separate from parse failures.
    row = df_checked.agg(
        _spark_count(lit(1)).alias("total"),
        _spark_count(when(col(col_name).isNull(), 1)).alias("nulls"),
        _spark_count(
            when(col(col_name).isNotNull() & col("_ts_check").isNull(), 1)
        ).alias("unparseable"),
        _spark_min("_ts_check").alias("min_ts"),
        _spark_max("_ts_check").alias("max_ts"),
    ).collect()[0]
    total = row["total"]
    null_cnt = row["nulls"]
    unparseable_cnt = row["unparseable"]
    parseable_cnt = total - null_cnt - unparseable_cnt
    min_ts = row["min_ts"]
    max_ts = row["max_ts"]

    # Return a few raw examples so callers can diagnose malformed values.
    if unparseable_cnt > 0:
        samples = [
            str(r[col_name])
            for r in df_checked.filter(
                col(col_name).isNotNull() & col("_ts_check").isNull()
            ).select(col_name).limit(5).collect()
        ]
    else:
        samples = []

    return {
        "total_rows": total,
        "parseable": parseable_cnt,
        "unparseable": unparseable_cnt,
        "null_count": null_cnt,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "unparseable_sample": samples,
    }


def print_timestamp_report(result: TimestampValidationResult, col_name: str) -> None:
    """Print a concise report for a timestamp validation result."""
    print(f"\u2500\u2500 Timestamp-Validierung: '{col_name}' \u2500\u2500")
    print(f"  Gesamtzeilen:      {result['total_rows']:,}")
    print(f"  Parsebar:          {result['parseable']:,}")
    print(f"  Nicht parsebar:    {result['unparseable']:,}")
    print(f"  NULL-Werte:        {result['null_count']:,}")
    if result['min_ts']:
        print(f"  Zeitraum:          {result['min_ts']}  ->  {result['max_ts']}")
    if result['unparseable_sample']:
        print(f"  Beispiele nicht parsebar: {result['unparseable_sample']}")
    if result['unparseable'] == 0 and result['null_count'] == 0:
        print("  \u2705 Alle Werte parsebar, keine NULLs.")
    elif result['unparseable'] > 0:
        print(f"  \u26a0\ufe0f  {result['unparseable']} Wert(e) konnten nicht geparst werden!")


def normalize_timestamp(
    df: DataFrame,
    col_name: str,
    output_col: str | None = None,
    expected_format: str | None = None,
) -> DataFrame:
    """Parse a local-time column into Spark's timestamp_ntz type."""
    if output_col is None:
        # Replace the input column by default; callers may choose a separate output.
        output_col = col_name

    # Keep local wall-clock values timezone-free rather than converting time zones.
    if expected_format:
        # The format must be a literal; a plain string would be read as a column name.
        parsed = to_timestamp_ntz(col(col_name), lit(expected_format))
    else:
        parsed = to_timestamp_ntz(col(col_name))

    return df.withColumn(output_col, parsed)
