"""Taxi Gold \u2013 Step 04: Silver \u2192 Gold (Taxi + Weather enriched).

Builds 3 Gold tables: taxi_weather_enriched, daily_metrics, hourly_ridership.
INNER JOIN Trips x Weather per pickup-hour. Coverage guard: skip write if 0 matches.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    date_trunc,
    hour,
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
from taxi_showcase.schemas.gold import (
    DAILY_METRICS_SCHEMA,
    HOURLY_RIDERSHIP_SCHEMA,
    TAXI_WEATHER_ENRICHED_SCHEMA,
)
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
    enriched_table = tables.taxi_weather_enriched
    daily_table = tables.daily_metrics
    hourly_table = tables.hourly_ridership

    print(f"Silver Taxi:    {taxi_silver}")
    print(f"Silver Weather: {weather_silver}")
    print(f"Gold Enriched:  {enriched_table}")
    print(f"Gold Daily:     {daily_table}")
    print(f"Gold Hourly:    {hourly_table}")

    # Verify upstream contracts before building any cross-source outputs.
    validate_table_schema(taxi_silver, TAXI_SILVER_SCHEMA)
    validate_table_schema(weather_silver, WEATHER_SILVER_SCHEMA)

    # Match trips to weather observations by local pickup date and hour.
    trips = spark.read.table(taxi_silver)
    weather = spark.read.table(weather_silver)

    trips_join = trips.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    weather_join = weather.withColumn("weather_hour", hour(col("ts")))

    tw_enriched = trips_join.join(weather_join,
        (trips_join.pickup_hour == weather_join.weather_hour) &
        (trips_join.tpep_pickup_datetime.cast("date") == weather_join.ts.cast("date")),
        'left')

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

    print("Gold Tabellen werden erstellt.")

    # Persist the trip-level result with the weather fields used for enrichment.
    enriched = (
        tw_enriched.select(
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID",
            "PULocationID", "DOLocationID", "payment_type",
            "fare_amount", "tip_amount", "total_amount", "trip_duration_min",
            "pickup_dayofweek", "pickup_weekday", "is_weekend",
            "ts", "temperature_2m_c", "rain_mm", "snowfall_cm",
            "precipitation_mm", "snow_depth_m",
        )
    )
    write_table(enriched, enriched_table, TAXI_WEATHER_ENRICHED_SCHEMA)
    print(f"taxi_weather_enriched geschrieben: {count_total:,} Rows")

    # Aggregate all taxi trips independently of weather coverage.
    daily_taxi_metrics = (
        trips
        .withColumn("trip_date", col("tpep_pickup_datetime").cast("date"))
        .groupBy("trip_date", "pickup_dayofweek", "pickup_weekday", "is_weekend")
        .agg(
            count_("*").alias("trips"),
            round_(sum_("total_amount"), 2).alias("total_revenue"),
            round_(avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
            round_(avg("passenger_count"), 2).alias("avg_passengers"),
            round_(avg("trip_distance"), 2).alias("avg_distance_miles"),
        )
    )

    # Aggregate weather once per observed hour, not once per matching trip.
    daily_weather_metrics = (
        weather
        .groupBy(col("ts").cast("date").alias("trip_date"))
        .agg(
            round_(avg("temperature_2m_c"), 2).alias("avg_temperature_c"),
            countDistinct(when(col("rain_mm") > 0, hour(col("ts")))).alias("rain_hours"),
            countDistinct(when(col("snowfall_cm") > 0, hour(col("ts")))).alias("snowfall_hours"),
        )
    )
    daily_metrics = (
        daily_taxi_metrics
        .join(daily_weather_metrics, on="trip_date", how="left")
        .select(
            "trip_date", "pickup_dayofweek", "pickup_weekday", "is_weekend",
            "trips", "total_revenue", "avg_trip_duration_min", "avg_passengers",
            "avg_distance_miles", "avg_temperature_c", "rain_hours", "snowfall_hours",
        )
    )
    write_table(daily_metrics, daily_table, DAILY_METRICS_SCHEMA)
    print(f"daily_metrics geschrieben: {daily_metrics.count():,} Rows")

    # Aggregate the same enriched data at pickup-hour granularity.
    hourly_ridership = (
        enriched
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

    # Report final row counts for all outputs written by this task.
    for name, table in [
        ("taxi_weather_enriched", enriched_table),
        ("daily_metrics", daily_table),
        ("hourly_ridership", hourly_table),
    ]:
        cnt = spark.table(table).count()
        print(f"  {name}: {cnt:,} rows")


if __name__ == "__main__":
    main()
