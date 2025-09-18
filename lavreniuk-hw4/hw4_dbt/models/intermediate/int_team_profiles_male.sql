{{ config(
    materialized='table',
    tags=['intermediate', 'male_team_profile']
) }}

WITH players_male AS (
    SELECT * FROM {{ ref("int_players_male") }}
)

SELECT 
    club_team_id AS club_id,
    club_name AS club_name,
    league_id AS league_id,
    SUM(value_eur) AS total_value,
    AVG(overall) AS avg_overall,
    AVG(age) AS avg_age,
    SUM(potential) AS total_potential
FROM players_male
WHERE club_team_id IS NOT NULL
GROUP BY club_team_id, club_name, league_id