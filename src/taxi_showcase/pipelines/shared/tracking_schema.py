"""Schema contract for checksum-based file tracking tables."""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TRACKING_SCHEMA = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("checksum", StringType(), False),
    StructField("file_size", LongType(), False),
    StructField("ingested_at", TimestampType(), False),
])
