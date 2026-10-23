"""Schema contract for checksum-based file tracking tables."""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Keep Spark's physical schema and the tracking-table DDL contract in sync.
TRACKING_SCHEMA = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("checksum", StringType(), False),
    StructField("file_size", LongType(), False),
    StructField("ingested_at", TimestampType(), False),
])

# Reuse canonical Spark type names when validating persisted tracking tables.
TRACKING_COLUMNS = {
    field.name: field.dataType.simpleString()
    for field in TRACKING_SCHEMA.fields
}
