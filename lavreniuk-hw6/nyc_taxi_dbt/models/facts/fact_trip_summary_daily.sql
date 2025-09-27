{{ config(
    tags = ['facts', 'trip', 'daily']
) }}

SELECT * FROM {{ build_trip_fact('CAST(pickup_datetime AS DATE)', 'pickup_date') }} ds