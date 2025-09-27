{{ config(
    tags = ['facts', 'trip', 'hourly']
) }}

SELECT * FROM {{ build_trip_fact('pickup_hour', 'pickup_hour') }} hs