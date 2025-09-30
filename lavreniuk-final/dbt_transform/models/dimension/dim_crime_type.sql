WITH res AS (
    SELECT DISTINCT ON (iucr_code)
        iucr_code,
        primary_description,
        secondary_description,
        index_code
    FROM {{ ref('stg_ucr_codes') }} a
)

SELECT * FROM res