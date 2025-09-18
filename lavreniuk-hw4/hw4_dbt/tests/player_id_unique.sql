-- another test can be described via generic test in dbt utils in shema.yaml

WITH players AS (
    SELECT
        player_id
    FROM
        {{ ref('int_players_male') }}
    UNION
    ALL
    SELECT
        player_id
    FROM
        {{ ref('int_players_female') }}
),
test_cte AS (
    SELECT
        player_id,
        COUNT(1) AS cnt
    FROM
        players
    GROUP BY
        player_id
)
SELECT 1
FROM
    test_cte
WHERE cnt > 1