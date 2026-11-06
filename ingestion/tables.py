TAXI = {
    "table": "taxi_trips",
    "columns": [
        ("vendorid", "INT64"),
        ("tpep_pickup_datetime", "DATETIME"),
        ("tpep_dropoff_datetime", "DATETIME"),
        ("passenger_count", "INT64"),
        ("trip_distance", "FLOAT64"),
        ("ratecodeid", "INT64"),
        ("store_and_fwd_flag", "STRING"),
        ("pulocationid", "INT64"),
        ("dolocationid", "INT64"),
        ("payment_type", "INT64"),
        ("fare_amount", "FLOAT64"),
        ("extra", "FLOAT64"),
        ("mta_tax", "FLOAT64"),
        ("tip_amount", "FLOAT64"),
        ("tolls_amount", "FLOAT64"),
        ("improvement_surcharge", "FLOAT64"),
        ("total_amount", "FLOAT64"),
        ("congestion_surcharge", "FLOAT64"),
        ("airport_fee", "FLOAT64"),
        ("cbd_congestion_fee", "FLOAT64"),  # only exists in newer files
    ],
    # Partition by pickup day, cluster by the columns we'll filter/group on
    "options": "PARTITION BY DATE(tpep_pickup_datetime) "
               "CLUSTER BY pulocationid, dolocationid",
    "month_filter": "tpep_pickup_datetime",
}

WEATHER = {
    "table": "weather_hourly",
    "columns": [
        ("time", "DATETIME"),
        ("temperature_2m", "FLOAT64"),
        ("precipitation", "FLOAT64"),
        ("rain", "FLOAT64"),
        ("snowfall", "FLOAT64"),
        ("wind_speed_10m", "FLOAT64"),
        ("weather_code", "INT64"),
    ],
    "options": "",  # tiny table (~700 rows/month): partitioning adds nothing
    "month_filter": "time",
}