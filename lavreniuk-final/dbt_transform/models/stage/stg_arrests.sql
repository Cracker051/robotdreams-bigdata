WITH res AS (
    SELECT
       *
    FROM {{ source('silver', 'silver_arrests') }} a
    -- LEFT JOIN {{ ref('stg_ucr_codes') }} c USING iucr_code -- We dont have column, named as 'primary_charge_code'
    WHERE EXTRACT('YEAR' FROM a.arrest_date) IS NOT NULL -- check if date type
)

SELECT * FROM res