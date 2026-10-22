"""Spark-independent validation for schema contracts."""


def validate_schema(actual_cols, expected_columns, context="DataFrame"):
    """Raise an actionable error unless columns and types match the contract."""
    expected_names = set(expected_columns)
    actual_names = set(actual_cols)
    missing = sorted(expected_names - actual_names)
    unexpected = sorted(actual_names - expected_names)
    type_mismatches = [
        (name, expected_columns[name], actual_cols[name])
        for name in sorted(expected_names & actual_names)
        if expected_columns[name] != actual_cols[name]
    ]

    if missing or unexpected or type_mismatches:
        details = [f"Schema-Pruefung fehlgeschlagen fuer {context}."]
        if missing:
            details.append(f"Fehlende Spalten: {missing}")
        if unexpected:
            details.append(f"Unerwartete Spalten: {unexpected}")
        if type_mismatches:
            details.append(
                "Typ-Abweichungen: "
                + ", ".join(
                    f"{name}: erwartet '{expected}', gefunden '{actual}'"
                    for name, expected, actual in type_mismatches
                )
            )
        raise ValueError("\n".join(details))

    print(f"Schema-Pruefung erfolgreich: {context}")
