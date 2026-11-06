with trips_by_hour as (

    select
        pickup_hour,
        count(*) as trips,
        countif(payment_type_id = 1) as card_trips,
        sum(fare_amount) as total_fare_amount,
        sum(trip_distance_miles) as total_distance_miles,
        sum(if(payment_type_id = 1, fare_amount, 0)) as card_fare_amount,
        sum(if(payment_type_id = 1, tip_amount, 0)) as card_tip_amount
    from {{ ref('fct_trips') }}
    group by pickup_hour

)

select
    w.weather_hour,
    date(w.weather_hour) as weather_date,
    extract(hour from w.weather_hour) as hour_of_day,
    extract(dayofweek from w.weather_hour) as day_of_week,  -- 1 = Sunday
    extract(dayofweek from w.weather_hour) in (1, 7) as is_weekend,
    w.temperature_c,
    w.precipitation_mm,
    w.snowfall_cm,
    w.wind_speed_kmh,
    w.weather_condition,
    w.precipitation_band,
    w.temperature_band,
    coalesce(t.trips, 0) as trips,
    coalesce(t.card_trips, 0) as card_trips,
    coalesce(t.total_fare_amount, 0) as total_fare_amount,
    coalesce(t.total_distance_miles, 0) as total_distance_miles,
    coalesce(t.card_fare_amount, 0) as card_fare_amount,
    coalesce(t.card_tip_amount, 0) as card_tip_amount,
    safe_divide(t.card_tip_amount, t.card_fare_amount) as tip_pct
from {{ ref('dim_weather') }} as w
left join trips_by_hour as t
    on t.pickup_hour = w.weather_hour