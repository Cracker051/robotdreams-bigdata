
{{ config(
    materialized='table',
    tags=['intermediate', 'male_league']
) }}

WITH players_male AS (
    SELECT * FROM {{ ref("int_players_male") }}
), teams_male AS (
    SELECT * FROM {{ ref("int_team_profiles_male") }}
), league_teams AS (
    SELECT
        tm.league_id,
        AVG(tm.total_value) AS avg_team_cost,
        AVG(tm.total_potential) AS avg_team_potential
    FROM teams_male tm
    GROUP BY league_id
), league_players AS (
    SELECT
        pm.league_id,
        pm.league_name,
        COUNT(DISTINCT player_id) as total_players,
        (COUNT(1) FILTER(WHERE age <= 23 OR age > 85) != 0) as has_talents,
        AVG(pm.overall) as avg_overall,
        AVG(pm.potential) as avg_potential,
        AVG(pm.age) as avg_age,
        AVG(pm.value_eur) as avg_value_eur,
        AVG(pm.wage_eur) as avg_wage_eur
    FROM players_male pm
    GROUP BY pm.league_id, pm.league_name
)

SELECT 
    lp.*,
    lt.avg_team_cost,
    lt.avg_team_potential
FROM league_players lp
LEFT JOIN league_teams lt ON lp.league_id = lt.league_id