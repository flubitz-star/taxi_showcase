"""Shared Landing Zone → Bronze ingestion flow for file-per-DataFrame pipelines."""

import os
from typing import Any, Callable, Mapping, Optional, Protocol

from pyspark.sql import DataFrame

from taxi_showcase.ingest_helpers import verify_bronze
from taxi_showcase.schema import (
    load_to_bronze,
    validate_dataframe_schema,
    validate_table_schema,
)
from taxi_showcase.tracking import get_pending_ingest_files


class BronzeIngressConfig(Protocol):
    """Minimum configuration required by the shared Bronze ingestion."""

    tracking_table: str
    bronze_table: str
    expected_columns: Mapping[str, Any]


def run_bronze_ingress(
    cfg: BronzeIngressConfig,
    bronze_schema: Any,
    read_file: Callable[[str], DataFrame],
    prepare_table: Optional[Callable[[str], None]] = None,
) -> None:
    """Load all pending tracked files into Bronze.

    read_file turns a file path into a raw DataFrame; prepare_table optionally
    creates the Bronze table before its schema is validated.
    """
    tracking_table = cfg.tracking_table
    bronze_table = cfg.bronze_table

    print(f"Bronze:   {bronze_table}")
    print(f"Tracking: {tracking_table}")

    if prepare_table is not None:
        prepare_table(bronze_table)

    # Validate the Bronze schema before querying the table; skip if it does not exist yet.
    validate_table_schema(bronze_table, bronze_schema)

    # Select files registered by the landing-zone step but not yet present in Bronze.
    new_files = get_pending_ingest_files(tracking_table, bronze_table)
    print(f"\nZu ladende Dateien: {len(new_files)}")
    for f in new_files:
        print(f"  {f.file_name}")

    if not new_files:
        print("Keine neuen Dateien \u2013 Bronze ist bereits aktuell.")
        return

    pending_dataframes = []
    for file_info in new_files:
        # A tracked path may no longer exist; skip it with a visible warning.
        if not os.path.exists(file_info.file_path):
            print(f"\u26a0\ufe0f  Datei nicht gefunden: {file_info.file_name}")
            continue

        raw_df = read_file(file_info.file_path)
        print(f"Schema-Check gegen: {file_info.file_name}")
        validate_dataframe_schema(
            raw_df,
            cfg.expected_columns,
            context=f"Quelldatei '{file_info.file_name}'",
        )
        pending_dataframes.append((file_info, raw_df))

    if not pending_dataframes:
        print("Keine verf\u00fcgbaren Dateien zum Laden.")
        return

    # Append validated source files with their filename and ingestion timestamp.
    total_rows = 0
    loaded_count = 0
    for f, df in pending_dataframes:
        print(f"Lade {f.file_name} ...")
        row_count = df.count()
        load_to_bronze(
            df,
            f.file_name,
            bronze_table,
            cfg.expected_columns,
            bronze_schema,
        )
        total_rows += row_count
        loaded_count += 1
        print(f"  -> {row_count:,} Zeilen geladen")

    print(
        f"\nIngest abgeschlossen: {loaded_count} Datei(en), "
        f"{total_rows:,} Zeilen in {bronze_table} geladen."
    )

    verify_bronze(bronze_table)
