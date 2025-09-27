{{ config(
    tags = ['deduplication', 'green_taxi']
) }}

SELECT
    *
FROM
    {{ taxi_deduplication(
        source(
            'stage',
            'green_taxi_stage'
        )
    ) }}
    sq
