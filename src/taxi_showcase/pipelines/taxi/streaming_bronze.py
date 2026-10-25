"""Ingest JSON taxi files with Auto Loader into the streaming Bronze table.

Auto Loader tracks discovered files in a persistent checkpoint and applies the
same source-column contract as batch ingestion. The ``availableNow`` trigger
processes all currently available files and then stops, which suits scheduled
jobs. Streaming and batch ingestion share the Bronze destination table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType

from taxi_showcase.pipelines.shared.config import (
    parse_catalog_argument,
    parse_landing_catalog_argument,
)
from taxi_showcase.pipelines.taxi.config import create_config
from taxi_showcase.schemas.taxi import BRONZE_SCHEMA

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    """Run one bounded Auto Loader pass and report the resulting Bronze size."""
    cfg = create_config(parse_catalog_argument(), parse_landing_catalog_argument())
    print(f"Quelle:     {cfg.streaming_landing_zone}")
    print(f"Ziel:       {cfg.bronze_table}")
    print(f"Checkpoint: {cfg.streaming_checkpoint}")

    # Reuse the batch source contract so streaming and batch records stay aligned.
    schema = StructType.fromDDL(
        ", ".join(f"`{name}` {dtype}" for name, dtype in cfg.expected_columns.items())
    )

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(cfg.streaming_landing_zone)
        .withColumn("_source_file", col("_metadata.file_name"))
        .withColumn("_ingested_at", current_timestamp())
        .select(*BRONZE_SCHEMA)
    )

    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.streaming_checkpoint)
        .trigger(availableNow=True)
        .toTable(cfg.bronze_table)
    )
    query.awaitTermination()

    count = spark.table(cfg.bronze_table).count()
    print(f"Streaming fertig: {count:,} Zeilen in {cfg.bronze_table}")


if __name__ == "__main__":
    main()
