{{ config(
    materialized='table',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["vendor_id", "pickup_location_id", "dropoff_location_id"]
) }}

with trips as (

    select 
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'pickup_location_id',
            'service_type'
        ]) }} as trip_id,

        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        coalesce(payment_type, 0) as payment_type,
        {{ get_payment_type_description('coalesce(payment_type, 0)') }} as payment_type_description,
        service_type

    from {{ ref('int_trips_unioned') }}

)

select *
from trips
qualify row_number() over (
    partition by trip_id
    order by dropoff_datetime, dropoff_location_id
) = 1