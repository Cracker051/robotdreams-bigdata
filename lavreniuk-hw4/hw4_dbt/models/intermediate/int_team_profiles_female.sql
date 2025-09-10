{{ config(
    materialized='table',
    tags=['intermediate', 'female_team_profile']
) }}

WITH players_female AS (
    SELECT * FROM {{ ref("int_players_female") }}
)

SELECT 
    club_team_id AS club_id,
    club_name AS club_name,
    league_id AS league_id,
    SUM(value_eur) AS total_value,
    AVG(overall) AS avg_overall,
    AVG(age) AS avg_age,
    SUM(potential) AS total_potential
FROM players_female
WHERE club_team_id IS NOT NULL
GROUP BY club_team_id, club_name, league_id