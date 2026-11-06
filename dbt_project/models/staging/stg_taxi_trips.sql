with source as (

    select * from {{ source('raw', 'taxi_trips') }}

),

renamed as (

    select
        vendorid as vendor_id,
        tpep_pickup_datetime as pickup_at,
        tpep_dropoff_datetime as dropoff_at,
        passenger_count,
        trip_distance as trip_distance_miles,
        ratecodeid as rate_code_id,
        pulocationid as pickup_zone_id,
        dolocationid as dropoff_zone_id,
        payment_type as payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_amount,
        _loaded_at
    from source

),

filtered as (

    -- Drop records that cannot be real trips
    select * from renamed
    where pickup_at < dropoff_at
      and datetime_diff(dropoff_at, pickup_at, hour) < 24
      and trip_distance_miles > 0
      and fare_amount > 0
      and tip_amount >= 0

),

keyed as (

    -- TLC has no trip ID, so build one from the trip's identifying fields
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_at', 'dropoff_at',
            'pickup_zone_id', 'dropoff_zone_id',
            'trip_distance_miles', 'fare_amount', 'total_amount',
            'passenger_count'
        ]) }} as trip_id,
        *
    from filtered

),

deduped as (

    select
        *,
        row_number() over (partition by trip_id order by _loaded_at desc) as rn
    from keyed

)

select
    * except (rn),
    date(pickup_at) as pickup_date,
    datetime_trunc(pickup_at, hour) as pickup_hour,
    datetime_diff(dropoff_at, pickup_at, minute) as trip_duration_minutes,
    case payment_type_id
        when 0 then 'flex_fare'
        when 1 then 'credit_card'
        when 2 then 'cash'
        when 3 then 'no_charge'
        when 4 then 'dispute'
        when 5 then 'unknown'
        when 6 then 'voided'
    end as payment_type_name
from deduped
where rn = 1