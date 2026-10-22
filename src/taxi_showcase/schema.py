"""Schema validation and strict table writes."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from taxi_showcase.schema_validation import validate_schema

spark = SparkSession.builder.getOrCreate()


_SIMPLE_NAME_TO_TYPE = {
    "string": StringType(),
    "int": IntegerType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "timestamp": TimestampType(),
    "timestamp_ntz": TimestampNTZType(),
    "date": DateType(),
    "boolean": BooleanType(),
}


def expected_to_struct_type(expected_columns):
    """Build a Spark StructType from a name-to-type schema contract."""
    fields = []
    for name, simple in expected_columns.items():
        dtype = _SIMPLE_NAME_TO_TYPE.get(simple)
        if dtype is None:
            raise ValueError(
                f"Unbekannter Typ '{simple}' fuer Spalte '{name}' im Schema-Vertrag "
                f"- unterstuetzt: {sorted(_SIMPLE_NAME_TO_TYPE)}"
            )
        fields.append(StructField(name, dtype, True))
    return StructType(fields)


def _dataframe_columns(df: DataFrame):
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}


def validate_dataframe_schema(df: DataFrame, expected_columns, context="DataFrame"):
    validate_schema(_dataframe_columns(df), expected_columns, context)


def validate_table_schema(table_name: str, expected_columns):
    """Validate an existing table before it is read from or written to."""
    if spark.catalog.tableExists(table_name):
        validate_dataframe_schema(
            spark.table(table_name),
            expected_columns,
            context=f"Tabelle '{table_name}'",
        )


def write_table(df: DataFrame, table_name: str, expected_columns, mode="overwrite"):
    """Validate the output schema and write it to the Delta table."""
    validate_dataframe_schema(df, expected_columns, context=f"Ausgabe fuer '{table_name}'")
    if mode == "append":
        validate_table_schema(table_name, expected_columns)
    df.write.format("delta").mode(mode).saveAsTable(table_name)


def load_to_bronze(raw_df, file_name, bronze_table_name, source_schema, bronze_schema):
    """Validate each source file and append it to Bronze without schema evolution."""
    validate_dataframe_schema(
        raw_df,
        source_schema,
        context=f"Quelldatei '{file_name}'",
    )
    out_df = (
        raw_df
        .withColumn("_source_file", lit(file_name))
        .withColumn("_ingested_at", current_timestamp())
    )
    write_table(out_df, bronze_table_name, bronze_schema, mode="append")
