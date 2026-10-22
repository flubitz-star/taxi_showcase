"""Timestamp validation and normalization functions."""

from datetime import datetime
from typing import TypedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    isnull,
    to_timestamp,
    to_timestamp_ntz,
)
from pyspark.sql.functions import (
    max as _spark_max,
)
from pyspark.sql.functions import (
    min as _spark_min,
)

spark = SparkSession.builder.getOrCreate()


class TimestampValidationResult(TypedDict):
    total_rows: int
    parseable: int
    unparseable: int
    null_count: int
    min_ts: datetime | None
    max_ts: datetime | None
    unparseable_sample: list[str]


def validate_timestamp_column(
    df: DataFrame,
    col_name: str,
    expected_format: str | None = None,
) -> TimestampValidationResult:
    """Prueft eine Spalte auf Timestamp-Parsebarkeit."""
    if expected_format:
        parsed_expr = to_timestamp(col(col_name), expected_format)
    else:
        parsed_expr = to_timestamp(col(col_name))

    df_checked = df.withColumn("_ts_check", parsed_expr)

    total = df.count()
    null_cnt = df.filter(isnull(col(col_name))).count()

    unparseable_df = df_checked.filter(
        isnull(col("_ts_check")) & ~isnull(col(col_name))
    )
    unparseable_cnt = unparseable_df.count()
    parseable_cnt = total - null_cnt - unparseable_cnt

    valid_df = df_checked.filter(~isnull(col("_ts_check")))
    valid_count = valid_df.count()
    if valid_count > 0:
        min_ts = valid_df.agg(_spark_min("_ts_check")).collect()[0][0]
        max_ts = valid_df.agg(_spark_max("_ts_check")).collect()[0][0]
    else:
        min_ts = None
        max_ts = None

    if unparseable_cnt > 0:
        samples = [str(r[col_name])
                   for r in unparseable_df.select(col_name).limit(5).collect()]
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
    """Druckt einen formatierten Bericht fuer validate_timestamp_column()."""
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
    """Parst eine lokale Zeitspalte zu timestamp_ntz."""
    if output_col is None:
        output_col = col_name

    if expected_format:
        parsed = to_timestamp_ntz(col(col_name), expected_format)
    else:
        parsed = to_timestamp_ntz(col(col_name))

    return df.withColumn(output_col, parsed)
