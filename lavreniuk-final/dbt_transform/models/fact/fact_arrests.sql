WITH res AS (
    SELECT
        sa.*,
        ct.*
    FROM {{ ref('stg_arrests') }} sa
    LEFT JOIN {{ ref('fact_crimes') }} cr ON sa.case_number = cr.case_number AND sa.arrest_date::DATE = cr.crime_date::DATE
    LEFT JOIN {{ ref('dim_crime_type') }} ct ON ct.iucr_code = cr.iucr_code
)

SELECT * FROM res