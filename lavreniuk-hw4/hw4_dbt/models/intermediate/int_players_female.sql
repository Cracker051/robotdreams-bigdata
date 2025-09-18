{{ config(
    materialized='table',
    tags=['intermediate', 'female_player']
) }}

WITH players_female AS (
    SELECT * FROM {{ ref("stg_players_female") }}
)

SELECT DISTINCT ON (player_id) -- Because player_id is not unique
    *,
    'FEMALE' AS gender
FROM players_female
ORDER BY player_id, fifa_update_date DESC