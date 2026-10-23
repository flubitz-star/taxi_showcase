"""Expected source columns and CSV options for CitiBike trips."""

# Source contract for CitiBike trip CSV fields and their Spark types.
EXPECTED_COLUMNS = {
    "ride_id":            "string",
    "rideable_type":      "string",
    "started_at":         "timestamp",
    "ended_at":           "timestamp",
    "start_station_name": "string",
    "start_station_id":   "string",
    "end_station_name":   "string",
    "end_station_id":     "string",
    "start_lat":          "double",
    "start_lng":          "double",
    "end_lat":            "double",
    "end_lng":             "double",
    "member_casual":       "string",
}

# Read the header and allow fractional seconds when source timestamps include them.
CSV_OPTIONS = {
    "header": True,
    "timestampFormat": "yyyy-MM-dd HH:mm:ss[.SSS]",
}
