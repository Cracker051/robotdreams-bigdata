{{ config(
    tags = ['dimension', 'locations']
) }}

WITH src AS (

    SELECT
        location_id::int,
        borough,
        zone,
        service_zone
    FROM
        {{ source(
            'stage',
            'taxi_lookup_zone_stage'
        ) }} AS s
)
SELECT
    *
FROM
    src
