"""Schema validation and evolution functions."""

from pyspark.sql import SparkSession
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
    TimestampType,
)

from taxi_showcase.pipelines.shared.config import META_COLUMNS

spark = SparkSession.builder.getOrCreate()


def get_spark_type_name(dtype) -> str:
    """Vereinfachte Typ-Namen fuer den Vergleich (Normalisierung)."""
    name = dtype.simpleString()
    return name.replace("timestamp_ntz", "timestamp").replace("long", "bigint")


_SIMPLE_NAME_TO_TYPE = {
    "string":    StringType(),
    "int":       IntegerType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     DoubleType(),
    "timestamp": TimestampType(),
    "date":      DateType(),
    "boolean":   BooleanType(),
}


def expected_to_struct_type(expected_columns):
    """Leitet ein StructType (Lese-Schema) aus expected_columns ab."""
    fields = []
    for name, simple in expected_columns.items():
        key = simple.replace("timestamp_ntz", "timestamp")
        dtype = _SIMPLE_NAME_TO_TYPE.get(key)
        if dtype is None:
            raise ValueError(
                f"Unbekannter Typ '{simple}' fuer Spalte '{name}' in expected_columns \u2013 "
                f"unterstuetzt: {sorted(_SIMPLE_NAME_TO_TYPE)}"
            )
        fields.append(StructField(name, dtype, True))
    return StructType(fields)


def validate_schema(actual_cols, expected_columns, meta_columns=META_COLUMNS):
    """Prueft ein Ist-Schema gegen ein erwartetes Spalten-Dictionary."""
    expected_names = set(expected_columns.keys())
    actual_names = set(actual_cols.keys())

    missing = expected_names - actual_names
    if missing:
        print(f"\n\u26a0\ufe0f  Fehlende Spalten (im Dictionary, nicht in Daten): {sorted(missing)}")
    else:
        print("\n\u2705 Alle Dictionary-Spalten vorhanden.")

    unexpected = actual_names - expected_names - meta_columns
    if unexpected:
        print(f"\u2139\ufe0f  Zusaetzliche Spalten (nicht im Dictionary): {sorted(unexpected)}")
        print("    \u2192 Moegliche Schema-Evolution. Siehe load_with_schema_evolution().")
    else:
        print("\u2705 Keine unerwarteten Spalten.")

    print("\n\u2500\u2500 Typ-Vergleich \u2500\u2500")
    type_mismatches = []
    for col_name, expected_type in sorted(expected_columns.items()):
        if col_name in actual_cols:
            actual_type = actual_cols[col_name]
            norm_actual = actual_type.replace("timestamp_ntz", "timestamp").replace("long", "bigint")
            norm_expected = expected_type.replace("timestamp_ntz", "timestamp")
            if norm_actual != norm_expected:
                type_mismatches.append((col_name, expected_type, actual_type))

    if type_mismatches:
        print("\u26a0\ufe0f  Typ-Abweichungen:")
        for col_name, exp, act in type_mismatches:
            print(f"  {col_name}: erwartet='{exp}', gefunden='{act}'")
    else:
        print("\u2705 Alle Typen uebereinstimmend (nach Normalisierung).")

    print("\n\u2500\u2500 Zusammenfassung \u2500\u2500")
    print(f"  Dictionary-Spalten: {len(expected_names)}")
    print(f"  Daten-Spalten:      {len(actual_names - meta_columns)}")
    print(f"  Fehlend:            {len(missing)}")
    print(f"  Zusaetzlich:         {len(unexpected)}")
    print(f"  Typ-Abweichungen:   {len(type_mismatches)}")

    return missing, unexpected, type_mismatches


def detect_schema_drift(raw_df, bronze_table_name, meta_columns=META_COLUMNS):
    """Vergleicht das Schema einer neuen Datei mit der Bronze-Tabelle."""
    if not spark.catalog.tableExists(bronze_table_name):
        return set(), set(), []

    raw_fields = {f.name: get_spark_type_name(f.dataType) for f in raw_df.schema.fields}
    bronze_schema = spark.sql(f"DESCRIBE TABLE {bronze_table_name}").collect()
    bronze_fields = {row.col_name: row.data_type for row in bronze_schema}
    for meta in meta_columns:
        bronze_fields.pop(meta, None)

    bronze_fields = {k: v.replace("timestamp_ntz", "timestamp").replace("long", "bigint")
                     for k, v in bronze_fields.items()}

    new_cols = set(raw_fields) - set(bronze_fields)
    missing_cols = set(bronze_fields) - set(raw_fields)

    type_changes = []
    common = set(raw_fields) & set(bronze_fields)
    for col in sorted(common):
        if raw_fields[col] != bronze_fields[col]:
            type_changes.append((col, bronze_fields[col], raw_fields[col]))

    return new_cols, missing_cols, type_changes


def load_with_schema_evolution(raw_df, file_name, bronze_table_name, meta_columns=META_COLUMNS):
    """Laedt einen DataFrame in Bronze mit mergeSchema bei neuen Spalten."""
    new_cols, missing_cols, type_changes = detect_schema_drift(raw_df, bronze_table_name, meta_columns)

    if type_changes:
        msg = f"\u274c Typ-Aenderung erkannt in {file_name} \u2013 manuelle Pruefung erforderlich!\n"
        for col_name, old_t, new_t in type_changes:
            msg += f"  {col_name}: Bronze='{old_t}' -> Parquet='{new_t}'\n"
        raise RuntimeError(msg)

    use_merge = bool(new_cols)
    if new_cols:
        print(f"  \u26a0\ufe0f  Neue Spalten erkannt: {sorted(new_cols)} -> mergeSchema aktiviert")
    if missing_cols:
        print(f"  \u2139\ufe0f  Spalten fehlen in Parquet (werden mit NULL gefuellt): {sorted(missing_cols)}")

    out_df = (raw_df
              .withColumn("_source_file", lit(file_name))
              .withColumn("_ingested_at", current_timestamp()))

    writer = (out_df.write
              .format("delta")
              .mode("append"))

    if use_merge:
        writer = writer.option("mergeSchema", "true")

    writer.saveAsTable(bronze_table_name)
    return use_merge
