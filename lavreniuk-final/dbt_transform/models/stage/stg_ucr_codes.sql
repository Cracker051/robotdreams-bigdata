WITH res AS (
    SELECT
        UPPER(TRIM(iucr_code)) as iucr_code,
        UPPER(TRIM(primary_description)) as primary_description,
        UPPER(TRIM(secondary_description)) as secondary_description,
        UPPER(TRIM(index_code)) AS index_code
    FROM {{ source('silver', 'silver_ucr_codes') }}
)

SELECT * FROM res