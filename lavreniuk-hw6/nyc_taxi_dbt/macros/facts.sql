{% macro build_trip_fact(grouping_column, alias) %}
    (
        SELECT
            {{ grouping_column }} AS {{ alias }},
            t.pu_location_id,
            t.do_location_id,
            COALESCE(SUM(fee), 0) AS total_fee,
            COALESCE(SUM(extra), 0) AS total_extra,
            COALESCE(SUM(total_amount), 0) AS total_amount,
            COALESCE(SUM(trip_distance), 0) AS total_trip_distance,
            COALESCE(AVG(trip_duration_minutes), 0) AS avg_trip_duration_minutes,
            COALESCE(MODE() within GROUP (ORDER BY t.taxi_type), '-') AS most_trips_by
        FROM
            {{ ref('trips') }} t
            JOIN {{ ref('locations') }} pu_loc
            ON pu_loc.location_id :: INT = t.pu_location_id
            JOIN {{ ref('locations') }} do_loc
            ON do_loc.location_id :: INT = t.do_location_id
        GROUP BY
            {{ grouping_column }},
            t.pu_location_id,
            t.do_location_id
    )
{% endmacro %}
