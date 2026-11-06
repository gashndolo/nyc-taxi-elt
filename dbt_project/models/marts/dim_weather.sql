select
    weather_hour,
    temperature_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    wind_speed_kmh,
    weather_code,
    case
        when weather_code = 0 then 'clear'
        when weather_code in (1, 2, 3) then 'cloudy'
        when weather_code in (45, 48) then 'fog'
        when weather_code between 51 and 57 then 'drizzle'
        when weather_code between 61 and 67 then 'rain'
        when weather_code between 71 and 77 then 'snow'
        when weather_code between 80 and 82 then 'rain_showers'
        when weather_code in (85, 86) then 'snow_showers'
        when weather_code between 95 and 99 then 'thunderstorm'
        else 'other'
    end as weather_condition,
    case
        when precipitation_mm is null then null
        when precipitation_mm = 0 then 'none'
        when precipitation_mm < 2.5 then 'light'
        when precipitation_mm < 7.6 then 'moderate'
        else 'heavy'
    end as precipitation_band,
    case
        when temperature_c is null then null
        when temperature_c < 0 then 'freezing'
        when temperature_c < 10 then 'cold'
        when temperature_c < 20 then 'mild'
        else 'warm'
    end as temperature_band
from {{ ref('stg_weather_hourly') }}