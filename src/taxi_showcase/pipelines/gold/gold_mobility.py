"""Gold Mobility \u2013 Step 04b: Taxi x Bike x Weather daily & hourly comparison.

Combines daily Taxi and CitiBike trip counts with weather data
to compare usage of both transport modes depending on weather.
"""

from pyspark.sql import SparkSession

from taxi_showcase.pipelines.bike.config import create_config as create_bike_config
from taxi_showcase.pipelines.shared.config import gold_tables, parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config as create_taxi_config
from taxi_showcase.pipelines.weather.config import (
    create_config as create_weather_config,
)
from taxi_showcase.schema import validate_table_schema, write_table
from taxi_showcase.schemas.bike import SILVER_SCHEMA as BIKE_SILVER_SCHEMA
from taxi_showcase.schemas.gold import (
    MOBILITY_DAILY_COMPARISON_SCHEMA,
    MOBILITY_HOURLY_COMPARISON_SCHEMA,
)
from taxi_showcase.schemas.taxi import SILVER_SCHEMA as TAXI_SILVER_SCHEMA
from taxi_showcase.schemas.weather import SILVER_SCHEMA as WEATHER_SILVER_SCHEMA

spark = SparkSession.builder.getOrCreate()


def main() -> None:
    # Resolve table names and ensure the Gold schema exists.
    catalog = parse_catalog_argument()
    tables = gold_tables(catalog)
    daily_table = tables.mobility_daily_comparison
    hourly_table = tables.mobility_hourly_comparison
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    taxi_silver = create_taxi_config(catalog).silver_table
    bike_silver = create_bike_config(catalog).silver_table
    weather_silver = create_weather_config(catalog).silver_table

    # Check each upstream table against its declared schema before querying it.
    validate_table_schema(taxi_silver, TAXI_SILVER_SCHEMA)
    validate_table_schema(bike_silver, BIKE_SILVER_SCHEMA)
    validate_table_schema(weather_silver, WEATHER_SILVER_SCHEMA)

    df_taxi = spark.table(taxi_silver)
    df_bike = spark.table(bike_silver)
    df_weather = spark.table(weather_silver)

    print(f"Taxi Silver:    {df_taxi.count():,} Zeilen")
    print(f"Bike Silver:    {df_bike.count():,} Zeilen")
    print(f"Weather Silver: {df_weather.count():,} Zeilen")

    # Aggregate taxi, bike, and hourly weather rows separately by calendar day.
    df_taxi_daily = spark.sql(f"""
        SELECT
            TO_DATE(tpep_pickup_datetime) AS pickup_date,
            MAX(pickup_dayofweek) AS pickup_dayofweek,
            MAX(pickup_weekday) AS pickup_weekday,
            MAX(is_weekend) AS is_weekend,
            COUNT(*) AS taxi_trips,
            ROUND(AVG(trip_duration_min), 1) AS taxi_avg_duration_min,
            ROUND(SUM(total_amount), 2) AS taxi_revenue,
            ROUND(AVG(trip_distance), 2) AS taxi_avg_distance
        FROM {taxi_silver}
        GROUP BY TO_DATE(tpep_pickup_datetime)
    """)

    df_bike_daily = spark.sql(f"""
        SELECT
            TO_DATE(started_at) AS pickup_date,
            MAX(pickup_dayofweek) AS pickup_dayofweek,
            MAX(pickup_weekday) AS pickup_weekday,
            MAX(is_weekend) AS is_weekend,
            COUNT(*) AS bike_trips,
            ROUND(AVG(trip_duration_min), 1) AS bike_avg_duration_min,
            SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS bike_member_trips,
            SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS bike_casual_trips,
            SUM(CASE WHEN rideable_type = 'electric_bike' THEN 1 ELSE 0 END) AS bike_electric_trips,
            SUM(CASE WHEN rideable_type = 'classic_bike' THEN 1 ELSE 0 END) AS bike_classic_trips
        FROM {bike_silver}
        GROUP BY TO_DATE(started_at)
    """)

    df_weather_daily = spark.sql(f"""
        SELECT
            TO_DATE(ts) AS pickup_date,
            ROUND(AVG(temperature_2m_c), 1) AS avg_temp_c,
            ROUND(MAX(temperature_2m_c), 1) AS max_temp_c,
            ROUND(MIN(temperature_2m_c), 1) AS min_temp_c,
            ROUND(SUM(rain_mm), 2) AS total_rain_mm,
            ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
            ROUND(SUM(snowfall_cm), 2) AS total_snowfall_cm,
            SUM(CASE WHEN rain_mm > 0 THEN 1 ELSE 0 END) AS hours_rain,
            SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS hours_precipitation,
            SUM(CASE WHEN temperature_2m_c < 0 THEN 1 ELSE 0 END) AS hours_freezing
        FROM {weather_silver}
        GROUP BY TO_DATE(ts)
    """)

    df_taxi_daily.createOrReplaceTempView("taxi_daily")
    df_bike_daily.createOrReplaceTempView("bike_daily")
    df_weather_daily.createOrReplaceTempView("weather_daily")

    # Full-join taxi and bike dates so activity from either mode is retained.
    # Weather is optional and joined to whichever mobility date is available.
    df_mobility = spark.sql("""
        SELECT
            COALESCE(t.pickup_date, b.pickup_date) AS pickup_date,
            COALESCE(t.pickup_dayofweek, b.pickup_dayofweek) AS pickup_dayofweek,
            COALESCE(t.pickup_weekday, b.pickup_weekday) AS pickup_weekday,
            COALESCE(t.is_weekend, b.is_weekend) AS is_weekend,
            COALESCE(t.taxi_trips, 0) AS taxi_trips,
            t.taxi_avg_duration_min,
            t.taxi_revenue,
            t.taxi_avg_distance,
            COALESCE(b.bike_trips, 0) AS bike_trips,
            b.bike_avg_duration_min,
            b.bike_member_trips,
            b.bike_casual_trips,
            b.bike_electric_trips,
            b.bike_classic_trips,
            w.avg_temp_c,
            w.max_temp_c,
            w.min_temp_c,
            w.total_rain_mm,
            w.total_precipitation_mm,
            w.total_snowfall_cm,
            w.hours_rain,
            w.hours_precipitation,
            w.hours_freezing,
            COALESCE(t.taxi_trips, 0) + COALESCE(b.bike_trips, 0) AS total_trips,
            CASE
                WHEN COALESCE(t.taxi_trips, 0) + COALESCE(b.bike_trips, 0) > 0
                THEN ROUND(COALESCE(b.bike_trips, 0) / (COALESCE(t.taxi_trips, 0) + COALESCE(b.bike_trips, 0)) * 100, 1)
                ELSE 0.0
            END AS bike_share_pct,
            CASE WHEN COALESCE(w.total_rain_mm, 0) > 0 THEN true ELSE false END AS has_rain,
            CASE WHEN COALESCE(w.total_precipitation_mm, 0) > 0 THEN true ELSE false END AS has_precipitation
        FROM taxi_daily t
        FULL OUTER JOIN bike_daily b ON t.pickup_date = b.pickup_date
        LEFT JOIN weather_daily w ON COALESCE(t.pickup_date, b.pickup_date) = w.pickup_date
    """)

    # Sort persisted daily results for predictable downstream presentation.
    print(f"Mobility Daily Zeilen: {df_mobility.count()}")

    print(f"Schreibe in {daily_table} ...")
    write_table(
        df_mobility.orderBy("pickup_date"),
        daily_table,
        MOBILITY_DAILY_COMPARISON_SCHEMA,
    )
    row_count = spark.table(daily_table).count()
    print(f"\u2705 {row_count:,} Zeilen in {daily_table} geschrieben.")

    # Aggregate taxi and bike activity by date and hour for a finer comparison.
    df_taxi_hourly = spark.sql(f"""
        SELECT
            TO_DATE(tpep_pickup_datetime) AS pickup_date,
            HOUR(tpep_pickup_datetime) AS pickup_hour,
            MAX(pickup_dayofweek) AS pickup_dayofweek,
            MAX(pickup_weekday) AS pickup_weekday,
            MAX(is_weekend) AS is_weekend,
            COUNT(*) AS taxi_trips,
            ROUND(AVG(trip_duration_min), 1) AS taxi_avg_duration_min,
            ROUND(SUM(total_amount), 2) AS taxi_revenue
        FROM {taxi_silver}
        GROUP BY TO_DATE(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)
    """)

    df_bike_hourly = spark.sql(f"""
        SELECT
            TO_DATE(started_at) AS pickup_date,
            HOUR(started_at) AS pickup_hour,
            MAX(pickup_dayofweek) AS pickup_dayofweek,
            MAX(pickup_weekday) AS pickup_weekday,
            MAX(is_weekend) AS is_weekend,
            COUNT(*) AS bike_trips,
            ROUND(AVG(trip_duration_min), 1) AS bike_avg_duration_min,
            SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS bike_member_trips,
            SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS bike_casual_trips
        FROM {bike_silver}
        GROUP BY TO_DATE(started_at), HOUR(started_at)
    """)

    # Weather Silver already contains one observation per timestamp/hour.
    df_weather_hourly = spark.sql(f"""
        SELECT
            TO_DATE(ts) AS pickup_date,
            HOUR(ts) AS pickup_hour,
            temperature_2m_c,
            rain_mm,
            precipitation_mm,
            snowfall_cm,
            CASE WHEN rain_mm > 0 THEN true ELSE false END AS has_rain,
            CASE WHEN precipitation_mm > 0 THEN true ELSE false END AS has_precipitation
        FROM {weather_silver}
    """)

    df_taxi_hourly.createOrReplaceTempView("taxi_hourly")
    df_bike_hourly.createOrReplaceTempView("bike_hourly")
    df_weather_hourly.createOrReplaceTempView("weather_hourly")

    # Retain hours with either mode and attach matching weather when available.
    df_mobility_hourly = spark.sql("""
        SELECT
            COALESCE(t.pickup_date, b.pickup_date) AS pickup_date,
            COALESCE(t.pickup_hour, b.pickup_hour) AS pickup_hour,
            COALESCE(t.pickup_dayofweek, b.pickup_dayofweek) AS pickup_dayofweek,
            COALESCE(t.pickup_weekday, b.pickup_weekday) AS pickup_weekday,
            COALESCE(t.is_weekend, b.is_weekend) AS is_weekend,
            COALESCE(t.taxi_trips, 0) AS taxi_trips,
            t.taxi_avg_duration_min,
            t.taxi_revenue,
            COALESCE(b.bike_trips, 0) AS bike_trips,
            b.bike_avg_duration_min,
            b.bike_member_trips,
            b.bike_casual_trips,
            w.temperature_2m_c,
            w.rain_mm,
            w.precipitation_mm,
            w.snowfall_cm,
            w.has_rain,
            w.has_precipitation
        FROM taxi_hourly t
        FULL OUTER JOIN bike_hourly b
            ON t.pickup_date = b.pickup_date AND t.pickup_hour = b.pickup_hour
        LEFT JOIN weather_hourly w
            ON COALESCE(t.pickup_date, b.pickup_date) = w.pickup_date
            AND COALESCE(t.pickup_hour, b.pickup_hour) = w.pickup_hour
    """)

    print(f"Mobility Hourly Zeilen: {df_mobility_hourly.count()}")
    print(f"Schreibe in {hourly_table} ...")
    write_table(
        df_mobility_hourly.orderBy("pickup_date", "pickup_hour"),
        hourly_table,
        MOBILITY_HOURLY_COMPARISON_SCHEMA,
    )
    row_count_h = spark.table(hourly_table).count()
    print(f"\u2705 {row_count_h:,} Zeilen in {hourly_table} geschrieben.")


if __name__ == "__main__":
    # Execute this file as the mobility Gold task.
    main()
