import unittest
from dataclasses import FrozenInstanceError, is_dataclass
from unittest.mock import patch

from taxi_showcase.pipelines.bike.config import create_config as create_bike_config
from taxi_showcase.pipelines.shared.config import (
    gold_tables,
    parse_catalog_argument,
    parse_landing_catalog_argument,
)
from taxi_showcase.pipelines.taxi.config import create_config as create_taxi_config
from taxi_showcase.pipelines.weather.config import (
    create_config as create_weather_config,
)


class PipelineConfigTests(unittest.TestCase):
    def test_pipeline_configs_are_frozen_dataclasses(self: "PipelineConfigTests") -> None:
        configs = (
            create_taxi_config("workspace"),
            create_bike_config("workspace"),
            create_weather_config("workspace"),
        )
        for config in configs:
            with self.subTest(config=type(config).__name__):
                self.assertTrue(is_dataclass(config))
                with self.assertRaises(FrozenInstanceError):
                    config.silver_table = "unexpected"

    def test_pipeline_configs_expose_expected_tables_and_landing_zones(self: "PipelineConfigTests") -> None:
        taxi_config = create_taxi_config("analytics")
        bike_config = create_bike_config("analytics")
        weather_config = create_weather_config("analytics")
        self.assertEqual(taxi_config.landing_zone, "/Volumes/workspace/raw_files/landing_zone/taxi")
        self.assertEqual(taxi_config.silver_table, "analytics.silver.taxi_trips")
        self.assertEqual(bike_config.landing_zone, "/Volumes/workspace/raw_files/landing_zone/bike")
        self.assertEqual(bike_config.silver_table, "analytics.silver.bike_trips")
        self.assertEqual(weather_config.landing_zone, "/Volumes/workspace/raw_files/landing_zone/weather")
        self.assertEqual(weather_config.silver_table, "analytics.silver.weather_hourly")

    def test_landing_zone_uses_shared_catalog_while_tables_follow_environment(self: "PipelineConfigTests") -> None:
        for env_catalog in ("taxi_dev", "taxi_prod"):
            config = create_taxi_config(env_catalog, "workspace")
            self.assertEqual(config.landing_zone, "/Volumes/workspace/raw_files/landing_zone/taxi")
            self.assertEqual(config.bronze_table, f"{env_catalog}.bronze.taxi_trips_raw")
            self.assertEqual(config.tracking_table, f"{env_catalog}.raw_files.taxi_ingested_files")

    def test_landing_catalog_defaults_to_workspace(self: "PipelineConfigTests") -> None:
        with patch("sys.argv", ["pipeline.py", "--catalog", "taxi_dev"]):
            self.assertEqual(parse_landing_catalog_argument(), "workspace")
        with patch("sys.argv", ["pipeline.py", "--catalog", "taxi_dev", "--landing-catalog", "shared"]):
            self.assertEqual(parse_catalog_argument(), "taxi_dev")
            self.assertEqual(parse_landing_catalog_argument(), "shared")

    def test_bike_config_includes_csv_options(self: "PipelineConfigTests") -> None:
        self.assertEqual(create_bike_config("workspace").csv_options["header"], True)

    def test_gold_tables_are_attributes(self: "PipelineConfigTests") -> None:
        tables = gold_tables("analytics")
        self.assertEqual(tables.taxi_weather_enriched, "analytics.gold.taxi_weather_enriched")
        with self.assertRaises(FrozenInstanceError):
            tables.daily_metrics = "unexpected"

    def test_catalog_is_read_from_task_arguments(self: "PipelineConfigTests") -> None:
        with patch("sys.argv", ["pipeline.py", "--catalog", "analytics"]):
            self.assertEqual(parse_catalog_argument(), "analytics")

    def test_catalog_rejects_invalid_sql_identifiers(self: "PipelineConfigTests") -> None:
        with (
            patch("sys.argv", ["pipeline.py", "--catalog", "bad-catalog"]),
            self.assertRaisesRegex(ValueError, "simple Unity Catalog identifier"),
        ):
            parse_catalog_argument()


if __name__ == "__main__":
    unittest.main()
