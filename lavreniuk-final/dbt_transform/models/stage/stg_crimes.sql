WITH res AS (
    SELECT
        *,
        c.crime_datetime::DATE AS crime_date,
        EXTRACT('HOUR' FROM c.crime_datetime) AS crime_hour,
        EXTRACT('DOW' FROM c.crime_datetime) AS crime_day_of_week,
        EXTRACT('MONTH' FROM c.crime_datetime) AS crime_month,
        EXTRACT('YEAR' FROM c.crime_datetime) AS crime_year
    FROM {{ source('silver', 'silver_crimes') }} c
    LEFT JOIN {{ ref('stg_ucr_codes') }} i USING (iucr_code)
)

SELECT * FROM res