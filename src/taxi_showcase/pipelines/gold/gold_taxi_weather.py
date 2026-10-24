"""Taxi Gold \u2013 Step 04: Silver \u2192 Gold (Taxi + Weather enriched).

Builds the hourly_ridership Gold table.
LEFT JOIN Trips x Weather on the pickup hour, so trips without a weather
observation are kept with NULL weather columns. Weather Silver holds one row
per hour, so the join does not duplicate trips. Coverage guard: skip the write
if there are no trips or no trip matches a weather observation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    date_trunc,
    when,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    round as round_,
)
from pyspark.sql.functions import (
    sum as sum_,
)

from taxi_showcase.pipelines.shared.config import gold_tables, parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config as create_taxi_config
from taxi_showcase.pipelines.weather.config import (
    create_config as create_weather_config,
)
from taxi_showcase.schema import validate_table_schema, write_table
from taxi_showcase.schemas.gold import HOURLY_RIDERSHIP_SCHEMA
from taxi_showcase.schemas.taxi import SILVER_SCHEMA as TAXI_SILVER_SCHEMA
from taxi_showcase.schemas.weather import SILVER_SCHEMA as WEATHER_SILVER_SCHEMA

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    # Resolve Gold output names and prepare the catalog schema.
    catalog = parse_catalog_argument()
    tables = gold_tables(catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    taxi_silver = create_taxi_config(catalog).silver_table
    weather_silver = create_weather_config(catalog).silver_table
    hourly_table = tables.hourly_ridership

    print(f"Silver Taxi:    {taxi_silver}")
    print(f"Silver Weather: {weather_silver}")
    print(f"Gold Hourly:    {hourly_table}")

    # Verify upstream contracts before building any cross-source outputs.
    validate_table_schema(taxi_silver, TAXI_SILVER_SCHEMA)
    validate_table_schema(weather_silver, WEATHER_SILVER_SCHEMA)

    # Match trips to weather observations by local pickup hour (both timestamp_ntz).
    trips = spark.read.table(taxi_silver)
    weather = spark.read.table(weather_silver)

    trips_join = trips.withColumn(
        "pickup_hour_ts",
        date_trunc("hour", col("tpep_pickup_datetime")).cast("timestamp_ntz"),
    )
    tw_enriched = trips_join.join(
        weather, trips_join["pickup_hour_ts"] == weather["ts"], "left"
    ).drop("pickup_hour_ts")

    count_total = trips.count()
    count_matched = tw_enriched.filter(col("ts").isNotNull()).count()
    coverage = 100.0 * count_matched / (count_total if count_total > 0 else 1)

    print(f"Gesamt Trips: {count_total}")
    print(f"Mit Wetter gematcht: {count_matched}")
    print(f"Coverage: {coverage:.4f}%")

    # Avoid replacing valid Gold outputs with empty data when there are no trips.
    if count_total == 0:
        print("WARNUNG: Keine Taxi-Daten vorhanden, kein Gold-Write!")
        return

    if count_matched == 0:
        print("WARNUNG: Kein Trip hat eine Wetter-Zuordnung, kein Gold-Write!")
        return

    print("Gold Tabellen werden erstellt.")

    # Aggregate the same enriched data at pickup-hour granularity.
    hourly_ridership = (
        tw_enriched
        .withColumn(
            "pickup_hour_ts",
            date_trunc("hour", col("tpep_pickup_datetime")).cast("timestamp_ntz"),
        )
        .groupBy("pickup_hour_ts", "pickup_dayofweek", "is_weekend")
        .agg(
            count_("*").alias("trips"),
            round_(sum_("total_amount"), 2).alias("total_revenue"),
            round_(avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
            round_(avg("passenger_count"), 2).alias("avg_passengers"),
            round_(avg("trip_distance"), 2).alias("avg_distance_miles"),
            round_(avg("temperature_2m_c"), 2).alias("avg_temperature_c"),
            sum_(when(col("rain_mm") > 0, 1).otherwise(0)).alias("rainy_trips"),
        )
    )
    write_table(hourly_ridership, hourly_table, HOURLY_RIDERSHIP_SCHEMA)
    print(f"hourly_ridership geschrieben: {hourly_ridership.count():,} Rows")

    count_hourly = spark.table(hourly_table).count()
    print(f"  hourly_ridership: {count_hourly:,} rows")


if __name__ == "__main__":
    main()
