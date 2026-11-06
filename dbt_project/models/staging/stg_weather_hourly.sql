select
    `time` as weather_hour,
    temperature_2m as temperature_c,
    precipitation as precipitation_mm,
    rain as rain_mm,
    snowfall as snowfall_cm,
    wind_speed_10m as wind_speed_kmh,
    weather_code,
    _loaded_at
from {{ source('raw', 'weather_hourly') }}