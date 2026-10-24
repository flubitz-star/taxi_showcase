"""Schema contracts for all Gold tables."""

# Taxi ridership and weather metrics grouped by pickup hour.
HOURLY_RIDERSHIP_SCHEMA = {
    "pickup_hour_ts": "timestamp_ntz",
    "pickup_dayofweek": "int",
    "is_weekend": "boolean",
    "trips": "bigint",
    "total_revenue": "double",
    "avg_trip_duration_min": "double",
    "avg_passengers": "double",
    "avg_distance_miles": "double",
    "avg_temperature_c": "double",
    "rainy_trips": "bigint",
}

# Daily taxi-versus-bike measures with corresponding daily weather summaries.
MOBILITY_DAILY_COMPARISON_SCHEMA = {
    "pickup_date": "date",
    "pickup_dayofweek": "int",
    "pickup_weekday": "string",
    "is_weekend": "boolean",
    "taxi_trips": "bigint",
    "taxi_avg_duration_min": "double",
    "taxi_revenue": "double",
    "taxi_avg_distance": "double",
    "bike_trips": "bigint",
    "bike_avg_duration_min": "double",
    "bike_member_trips": "bigint",
    "bike_casual_trips": "bigint",
    "bike_electric_trips": "bigint",
    "bike_classic_trips": "bigint",
    "weather_hours": "bigint",
    "avg_temp_c": "double",
    "max_temp_c": "double",
    "min_temp_c": "double",
    "total_rain_mm": "double",
    "total_precipitation_mm": "double",
    "total_snowfall_cm": "double",
    "hours_rain": "bigint",
    "hours_precipitation": "bigint",
    "hours_freezing": "bigint",
    "total_trips": "bigint",
    "bike_share_pct": "double",
    "has_rain": "boolean",
    "has_precipitation": "boolean",
}

# Hourly taxi-versus-bike measures with matching weather observations.
MOBILITY_HOURLY_COMPARISON_SCHEMA = {
    "pickup_date": "date",
    "pickup_hour": "int",
    "pickup_dayofweek": "int",
    "pickup_weekday": "string",
    "is_weekend": "boolean",
    "taxi_trips": "bigint",
    "taxi_avg_duration_min": "double",
    "taxi_revenue": "double",
    "bike_trips": "bigint",
    "bike_avg_duration_min": "double",
    "bike_member_trips": "bigint",
    "bike_casual_trips": "bigint",
    "temperature_2m_c": "double",
    "rain_mm": "double",
    "precipitation_mm": "double",
    "snowfall_cm": "double",
    "has_rain": "boolean",
    "has_precipitation": "boolean",
}
