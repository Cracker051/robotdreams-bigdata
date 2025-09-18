{{ config(tags = ['mart', 'team_dashboard_analytics']) }}

WITH male_players AS (
    SELECT
        player_id || '_male' AS player_id,
        club_team_id || '_male' AS club_id,
        overall,
        potential,
        value_eur,
        wage_eur,
        age
    FROM
        {{ ref('int_players_male') }}
),
female_players AS (
    SELECT
        player_id || '_female' AS player_id,
        club_team_id || '_female' AS club_id,
        overall,
        potential,
        value_eur,
        wage_eur,
        age
    FROM
        {{ ref('int_players_female') }}
),
players AS (
    SELECT
        *
    FROM
        female_players
    UNION
    ALL
    SELECT
        *
    FROM
        male_players
),
male_teams AS (
    SELECT
        t.club_id || '_male' AS club_id,
        t.club_name,
        t.league_id,
        t.total_value,
        t.avg_overall,
        t.avg_age,
        t.total_potential
    FROM
        {{ ref('int_team_profiles_male') }} t
),
female_teams AS (
    SELECT
        t.club_id || '_female' AS club_id,
        t.club_name,
        t.league_id,
        t.total_value,
        t.avg_overall,
        t.avg_age,
        t.total_potential
    FROM
        {{ ref('int_team_profiles_female') }} t
),
teams AS (
    SELECT
        *
    FROM
        male_teams
    UNION
    ALL
    SELECT
        *
    FROM
        female_teams
),
aggregated AS (
    SELECT
        p.club_id,
        count(DISTINCT p.player_id) AS player_count,
        avg(p.overall) AS avg_overall_players,
        avg(p.potential) AS avg_potential_players,
        sum(p.value_eur) AS total_value_players,
        sum(p.wage_eur) AS total_wages,
        avg(p.age) AS avg_age_players
    FROM
        players p
    GROUP BY
        p.club_id
),
key_players AS (
    SELECT
        DISTINCT ON (p.club_id) p.club_id,
        p.player_id AS best_player_id,
        p.overall AS best_player_overall,
        (p.potential - p.overall) AS overperformance
    FROM
        players p
    ORDER BY
        p.club_id,
        p.overall DESC
),
max_overperformance AS (
    SELECT
        club_id,
        max(potential - overall) AS max_overperformance
    FROM
        players
    GROUP BY
        club_id
)
SELECT
    t.club_id,
    t.club_name,
    t.league_id,
    COALESCE(a.player_count, 0) AS player_count,
    COALESCE(a.avg_overall_players, t.avg_overall) AS avg_overall,
    COALESCE(a.avg_potential_players, t.total_potential) AS avg_potential,
    COALESCE(a.total_value_players, t.total_value) AS total_value,
    COALESCE(a.total_wages, 0) AS total_wages,
    COALESCE(a.avg_age_players, t.avg_age) AS avg_age,
    k.best_player_id,
    k.best_player_overall,
    m.max_overperformance
FROM
    teams t
    LEFT JOIN aggregated a ON t.club_id = a.club_id
    LEFT JOIN key_players k ON t.club_id = k.club_id
    LEFT JOIN max_overperformance m ON t.club_id = m.club_id