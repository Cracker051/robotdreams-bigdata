{{ config(
    tags = ['deduplication', 'yellow_taxi']
) }}

SELECT
    *
FROM
    {{ taxi_deduplication(
        source(
            'stage',
            'yellow_taxi_stage'
        )
    ) }}
    sq
