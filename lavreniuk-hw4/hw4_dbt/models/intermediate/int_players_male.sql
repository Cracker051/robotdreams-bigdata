{{ config(
    materialized='table',
    tags=['intermediate', 'male_player']
) }}

WITH players_male AS (
    SELECT * FROM {{ ref("stg_players_male") }}
)

SELECT 
    *,
    'MALE' AS gender
FROM players_male