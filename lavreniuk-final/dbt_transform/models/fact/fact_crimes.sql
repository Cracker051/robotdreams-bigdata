WITH res AS (
    SELECT
        crime_id,
        case_number,
        crime_datetime,
        crime_date,
        crime_hour,
        crime_day_of_week,
        crime_month,
        crime_year,
        coalesce(is_arrest, false) as is_arrest,
        coalesce(is_domestic, false) as is_domestic,
        primary_type,
        fbi_code,
        r.location_key,
        dc.*
    FROM {{ ref('stg_crimes') }} a
    LEFT JOIN  {{ ref('dim_location') }} r ON r.block = a.block AND r.longitude = a.longitude AND r.latitude = a.latitude
    LEFT JOIN {{ ref('dim_crime_type') }} dc USING (iucr_code)
)

SELECT * FROM res