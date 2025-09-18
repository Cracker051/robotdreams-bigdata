{{ config(
    materialized='table',
    tags=['intermediate', 'female_league']
) }}

WITH players_female AS (
    SELECT * FROM {{ ref("int_players_female") }}
), teams_female AS (
    SELECT * FROM {{ ref("int_team_profiles_female") }}
), league_teams AS (
    SELECT
        tf.league_id,
        AVG(tf.total_value) AS avg_team_cost,
        AVG(tf.total_potential) AS avg_team_potential
    FROM teams_female tf
    GROUP BY league_id
), league_players AS (
    SELECT
        pf.league_id,
        pf.league_name,
        COUNT(DISTINCT player_id) as total_players,
        (COUNT(1) FILTER(WHERE age <= 23 OR age > 85) != 0) as has_talents,
        AVG(pf.overall) as avg_overall,
        AVG(pf.potential) as avg_potential,
        AVG(pf.age) as avg_age,
        AVG(pf.value_eur) as avg_value_eur,
        AVG(pf.wage_eur) as avg_wage_eur
    FROM players_female pf
    GROUP BY pf.league_id, pf.league_name
)

SELECT 
    lp.*,
    lt.avg_team_cost,
    lt.avg_team_potential
FROM league_players lp
LEFT JOIN league_teams lt ON lp.league_id = lt.league_id