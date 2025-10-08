WITH res AS (
    SELECT DISTINCT ON (block, latitude, longitude, x_coordinate, y_coordinate)
        md5(block || latitude || longitude || x_coordinate || y_coordinate) as location_key,
        block,
        location_description,
        latitude,
        longitude,
        x_coordinate,
        y_coordinate,
        beat_id,
        district_id,
        ward_id,
        community_area_id,
        CONCAT_WS(' ', block, location_description) as location_full_text
    FROM {{ ref('stg_crimes') }} a
)

SELECT * FROM res