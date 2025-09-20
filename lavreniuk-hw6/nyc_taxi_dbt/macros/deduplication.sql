-- Because each taxi has the same deduplication strategy and lookup columns, we can present it as macros
{% macro taxi_deduplication(table_name) %}
    (
        SELECT
            *
        FROM
            (
                SELECT
                    t.*,
                    ROW_NUMBER() over (
                        PARTITION BY vendor_id,
                        pu_location_id,
                        do_location_id,
                        pickup_datetime,
                        dropoff_datetime
                        ORDER BY
                            pickup_datetime
                    ) AS rn
                FROM
                    {{ table_name }}
                    t
            ) t
        WHERE
            rn = 1
    )
{% endmacro %}
