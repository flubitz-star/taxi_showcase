"""Taxi Ingress \u2013 Step 02: Landing Zone \u2192 Bronze.

Reads Parquet files (NYC TLC Yellow Trip Records) into bronze.
Strict schema validation against the declared source contract.
"""

from pyspark.sql import SparkSession

from taxi_showcase.pipelines.shared.bronze import run_bronze
from taxi_showcase.pipelines.shared.config import parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config
from taxi_showcase.schemas.taxi import BRONZE_SCHEMA

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    # Resolve the configured source and destination tables for this catalog.
    cfg = create_config(parse_catalog_argument())
    run_bronze(cfg, BRONZE_SCHEMA, spark.read.parquet)


if __name__ == "__main__":
    # Execute this entry point when Databricks runs the file as a task.
    main()
