{{ config(
    tags = ['dimension', 'trips']
) }}

SELECT * FROM (
    SELECT
        vendor_id,
        pu_location_id,
        do_location_id,
        ratecode_id,
        payment_type,
        passenger_count,
        total_amount,
        fare_amount,
        fee,
        extra,
        trip_distance,
        trip_duration_minutes,
        pickup_datetime,
        dropoff_datetime,
        NULL AS trip_type, -- green has no trip type
        store_and_fwd_flag,
        pickup_hour,
        'green' AS taxi_type
    FROM
        {{ ref('green_taxi_deduplicated') }}
    UNION ALL
    SELECT
        vendor_id,
        pu_location_id,
        do_location_id,
        ratecode_id,
        payment_type,
        passenger_count,
        total_amount,
        fare_amount,
        airport_fee as fee,
        extra,
        trip_distance,
        trip_duration_minutes,
        pickup_datetime,
        dropoff_datetime,
        trip_type,
        store_and_fwd_flag,
        pickup_hour,
        'yellow' AS taxi_type
    FROM
        {{ ref('yellow_taxi_deduplicated') }}
) trip 
WHERE 
    trip_duration_minutes > 0
    AND passenger_count BETWEEN 1 AND 6
    AND dropoff_datetime > pickup_datetime
