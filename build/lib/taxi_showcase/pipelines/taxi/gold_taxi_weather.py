"""Taxi Gold \u2013 Step 04: Silver \u2192 Gold (Taxi + Weather enriched).

Builds 3 Gold tables: taxi_weather_enriched, daily_metrics, hourly_ridership.
INNER JOIN Trips x Weather per pickup-hour. Coverage guard: skip write if 0 matches.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, count, avg, sum as sum_, count as count_,
    round as round_, when, countDistinct, date_trunc,
)

from taxi_showcase.config import CATALOG, PROJECT_CONFIG, GOLD_TABLES

spark = SparkSession.builder.getOrCreate()


def main():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

    taxi_silver = PROJECT_CONFIG["taxi"]["silver_table"]
    weather_silver = PROJECT_CONFIG["weather"]["silver_table"]
    enriched_table = GOLD_TABLES["taxi_weather_enriched"]
    daily_table = GOLD_TABLES["daily_metrics"]
    hourly_table = GOLD_TABLES["hourly_ridership"]

    print(f"Silver Taxi:    {taxi_silver}")
    print(f"Silver Weather: {weather_silver}")
    print(f"Gold Enriched:  {enriched_table}")
    print(f"Gold Daily:     {daily_table}")
    print(f"Gold Hourly:    {hourly_table}")

    # Coverage check & join
    trips = spark.read.table(taxi_silver)
    weather = spark.read.table(weather_silver)

    trips_join = trips.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    weather_join = weather.withColumn("weather_hour", hour(col("ts")))

    tw_enriched = trips_join.join(weather_join,
        (trips_join.pickup_hour == weather_join.weather_hour) &
        (trips_join.tpep_pickup_datetime.cast("date") == weather_join.ts.cast("date")),
        'inner')

    count_total = trips.count()
    count_matched = tw_enriched.count()
    coverage = 100.0 * count_matched / (count_total if count_total > 0 else 1)

    print(f"Gesamt Trips: {count_total}")
    print(f"Mit Wetter gematcht: {count_matched}")
    print(f"Coverage: {coverage:.4f}%")

    if count_matched == 0:
        print("WARNUNG: Taxi-Daten und Wetter-Daten ueberlappen nicht, kein Gold-Write!")
        return

    print("Gold Tabellen werden erstellt.")

    # Step 2: taxi_weather_enriched
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
    enriched.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite").saveAsTable(enriched_table)
    print(f"taxi_weather_enriched geschrieben: {count_matched:,} Rows")

    # Step 3: daily_metrics
    daily_metrics = (
        enriched
        .withColumn("trip_date", col("tpep_pickup_datetime").cast("date"))
        .groupBy("trip_date", "pickup_dayofweek", "pickup_weekday", "is_weekend")
        .agg(
            count_("*").alias("trips"),
            round_(sum_("total_amount"), 2).alias("total_revenue"),
            round_(avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
            round_(avg("passenger_count"), 2).alias("avg_passengers"),
            round_(avg("trip_distance"), 2).alias("avg_distance_miles"),
            round_(avg("temperature_2m_c"), 2).alias("avg_temperature_c"),
            countDistinct(when(col("rain_mm") > 0, hour(col("tpep_pickup_datetime")))).alias("rain_hours"),
            countDistinct(when(col("snowfall_cm") > 0, hour(col("tpep_pickup_datetime")))).alias("snowfall_hours"),
        )
    )
    daily_metrics.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite").saveAsTable(daily_table)
    print(f"daily_metrics geschrieben: {daily_metrics.count():,} Rows")

    # Step 4: hourly_ridership
    hourly_ridership = (
        enriched
        .withColumn("pickup_hour_ts", date_trunc("hour", col("tpep_pickup_datetime")))
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
    hourly_ridership.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite").saveAsTable(hourly_table)
    print(f"hourly_ridership geschrieben: {hourly_ridership.count():,} Rows")

    # Summary
    for name, table in [
        ("taxi_weather_enriched", enriched_table),
        ("daily_metrics", daily_table),
        ("hourly_ridership", hourly_table),
    ]:
        cnt = spark.table(table).count()
        print(f"  {name}: {cnt:,} rows")


if __name__ == "__main__":
    main()
