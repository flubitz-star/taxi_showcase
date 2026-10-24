import unittest

from taxi_showcase.schema_validation import validate_schema
from taxi_showcase.schemas.bike import BRONZE_SCHEMA as BIKE_BRONZE_SCHEMA
from taxi_showcase.schemas.bike import SILVER_SCHEMA as BIKE_SILVER_SCHEMA
from taxi_showcase.schemas.taxi import SILVER_SCHEMA as TAXI_SILVER_SCHEMA
from taxi_showcase.schemas.weather import BRONZE_SCHEMA as WEATHER_BRONZE_SCHEMA
from taxi_showcase.schemas.weather import DLQ_SCHEMA as WEATHER_DLQ_SCHEMA
from taxi_showcase.schemas.weather import SILVER_SCHEMA as WEATHER_SILVER_SCHEMA


class ValidateSchemaTests(unittest.TestCase):
    def test_accepts_matching_columns_and_types(self: "ValidateSchemaTests") -> None:
        validate_schema(
            {"id": "int", "created_at": "timestamp"},
            {"id": "int", "created_at": "timestamp"},
        )

    def test_rejects_different_timestamp_types(self: "ValidateSchemaTests") -> None:
        with self.assertRaisesRegex(
            ValueError,
            "created_at: erwartet 'timestamp', gefunden 'timestamp_ntz'",
        ):
            validate_schema(
                {"created_at": "timestamp_ntz"},
                {"created_at": "timestamp"},
            )

    def test_rejects_different_integer_type_names(self: "ValidateSchemaTests") -> None:
        with self.assertRaisesRegex(
            ValueError,
            "id: erwartet 'bigint', gefunden 'long'",
        ):
            validate_schema({"id": "long"}, {"id": "bigint"})

    def test_reports_missing_and_unexpected_columns(self: "ValidateSchemaTests") -> None:
        with self.assertRaisesRegex(ValueError, "Fehlende Spalten: \\['id'\\]"):
            validate_schema({"extra": "string"}, {"id": "int"})

        with self.assertRaisesRegex(ValueError, "Unerwartete Spalten: \\['extra'\\]"):
            validate_schema({"id": "int", "extra": "string"}, {"id": "int"})

    def test_reports_type_mismatches_and_context(self: "ValidateSchemaTests") -> None:
        with self.assertRaises(ValueError) as error:
            validate_schema(
                {"id": "string"},
                {"id": "int"},
                context="Tabelle 'example'",
            )

        message = str(error.exception)
        self.assertIn("Tabelle 'example'", message)
        self.assertIn("id: erwartet 'int', gefunden 'string'", message)

    def test_silver_event_timestamps_use_timestamp_ntz(self: "ValidateSchemaTests") -> None:
        self.assertEqual(
            TAXI_SILVER_SCHEMA["tpep_pickup_datetime"],
            "timestamp_ntz",
        )
        self.assertEqual(
            TAXI_SILVER_SCHEMA["tpep_dropoff_datetime"],
            "timestamp_ntz",
        )
        self.assertEqual(BIKE_SILVER_SCHEMA["started_at"], "timestamp_ntz")
        self.assertEqual(BIKE_SILVER_SCHEMA["ended_at"], "timestamp_ntz")
        self.assertEqual(BIKE_BRONZE_SCHEMA["started_at"], "timestamp")
        self.assertEqual(BIKE_BRONZE_SCHEMA["ended_at"], "timestamp")
        self.assertEqual(WEATHER_SILVER_SCHEMA["ts"], "timestamp_ntz")

    def test_weather_dlq_keeps_bronze_fields_and_quarantine_metadata(self: "ValidateSchemaTests") -> None:
        self.assertEqual(
            {name: WEATHER_DLQ_SCHEMA[name] for name in WEATHER_BRONZE_SCHEMA},
            WEATHER_BRONZE_SCHEMA,
        )
        self.assertEqual(WEATHER_DLQ_SCHEMA["_dlq_reason"], "string")
        self.assertEqual(WEATHER_DLQ_SCHEMA["_dlq_removed_at"], "timestamp")

if __name__ == "__main__":
    unittest.main()
