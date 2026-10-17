"""Configuration shared by multiple pipelines."""

CATALOG = "workspace"
LANDING_ZONE_ROOT = f"/Volumes/{CATALOG}/raw_files/landing_zone"

META_COLUMNS = {"_source_file", "_ingested_at"}

GOLD_TABLES = {
    "taxi_weather_enriched": f"{CATALOG}.gold.taxi_weather_enriched",
    "daily_metrics": f"{CATALOG}.gold.daily_metrics",
    "hourly_ridership": f"{CATALOG}.gold.hourly_ridership",
    "mobility_daily_comparison": f"{CATALOG}.gold.mobility_daily_comparison",
    "mobility_hourly_comparison": f"{CATALOG}.gold.mobility_hourly_comparison",
}
