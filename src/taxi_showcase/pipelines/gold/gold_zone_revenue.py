"""Build hourly taxi revenue metrics grouped by pickup zone."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, hour, to_date
from pyspark.sql.functions import count as count_
from pyspark.sql.functions import sum as sum_
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from taxi_showcase.pipelines.shared.config import gold_tables, parse_catalog_argument
from taxi_showcase.pipelines.taxi.config import create_config as create_taxi_config
from taxi_showcase.pipelines.taxi.zone_lookup import ZONE_LOOKUP
from taxi_showcase.schema import validate_table_schema, write_table
from taxi_showcase.schemas.gold import ZONE_REVENUE_HOURLY_SCHEMA
from taxi_showcase.schemas.taxi import SILVER_SCHEMA as TAXI_SILVER_SCHEMA

spark = SparkSession.builder.getOrCreate()

ZONE_LOOKUP_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), nullable=False),
        StructField("Borough", StringType(), nullable=False),
        StructField("Zone", StringType(), nullable=False),
        StructField("service_zone", StringType(), nullable=False),
    ]
)


def main() -> None:
    catalog = parse_catalog_argument()
    taxi_silver_table = create_taxi_config(catalog).silver_table
    output_table = gold_tables(catalog).zone_revenue_hourly

    validate_table_schema(taxi_silver_table, TAXI_SILVER_SCHEMA)
    trips = spark.read.table(taxi_silver_table).filter(
        col("PULocationID").isNotNull() & (col("total_amount") > 0)
    )
    if trips.limit(1).count() == 0:
        print("WARNUNG: Keine gueltigen Taxi-Daten, kein Gold-Write!")
        return

    zone_rows = [
        (location_id, borough, zone, service_zone)
        for location_id, (borough, zone, service_zone) in ZONE_LOOKUP.items()
    ]
    zones = spark.createDataFrame(zone_rows, ZONE_LOOKUP_SCHEMA)
    joined = trips.alias("t").join(
        broadcast(zones).alias("z"),
        col("t.PULocationID") == col("z.LocationID"),
        "left",
    )

    unmatched = joined.filter(col("z.LocationID").isNull()).count()
    if unmatched:
        print(
            "WARNUNG: "
            f"{unmatched:,} Taxi-Zeilen haben keine Zuordnung in der "
            "statischen Zone-Map."
        )

    zone_revenue = (
        joined.select(
            to_date(col("t.tpep_pickup_datetime")).alias("pickup_date"),
            hour(col("t.tpep_pickup_datetime")).alias("pickup_hour"),
            col("t.pickup_dayofweek").alias("pickup_dayofweek"),
            col("t.pickup_weekday").alias("pickup_weekday"),
            col("t.is_weekend").alias("is_weekend"),
            col("t.PULocationID").alias("pu_location_id"),
            col("z.Zone").alias("pu_zone"),
            col("z.Borough").alias("pu_borough"),
            col("t.total_amount").alias("total_amount"),
            col("t.fare_amount").alias("fare_amount"),
            col("t.tip_amount").alias("tip_amount"),
            col("t.trip_distance").alias("trip_distance"),
            col("t.trip_duration_min").alias("trip_duration_min"),
        )
        .groupBy(
            "pickup_date",
            "pickup_hour",
            "pickup_dayofweek",
            "pickup_weekday",
            "is_weekend",
            "pu_location_id",
            "pu_zone",
            "pu_borough",
        )
        .agg(
            count_("*").alias("trips"),
            sum_("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("trip_duration_min").alias("avg_trip_duration_min"),
        )
    )
    write_table(
        zone_revenue,
        output_table,
        ZONE_REVENUE_HOURLY_SCHEMA,
        partition_by=["pickup_date"],
    )
    print(f"zone_revenue_hourly geschrieben: {zone_revenue.count():,} Zeilen")


if __name__ == "__main__":
    main()
