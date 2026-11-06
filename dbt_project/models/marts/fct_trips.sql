{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'pickup_date', 'data_type': 'date'},
        cluster_by=['pickup_zone_id']
    )
}}

select
    trip_id,
    vendor_id,
    pickup_date,
    pickup_at,
    pickup_hour,
    dropoff_at,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type_id,
    payment_type_name,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    _loaded_at
from {{ ref('stg_taxi_trips') }}

{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}