SELECT
    crime_date,
    primary_type,
    district_id,
    community_area_id,
    COUNT(1) as total_crimes,
    COUNT(1) FILTER (WHERE is_arrest) AS crimes_with_arrest,
    COUNT(1) FILTER (WHERE is_domestic) AS domestic_crimes
FROM {{ ref('fact_crimes') }}
LEFT JOIN {{ ref('dim_location') }} USING (location_key)
GROUP BY crime_date, primary_type, district_id, community_area_id