{{  config(
    tags=['mart', 'male_analytics']
)}}

with male_players as (
    select *
    from {{ ref('int_players_male') }}
)

SELECT
    player_id,
    overall,
    potential,
    potential - overall AS growth_gap,
    age,
    height_cm,
    weight_kg,
    value_eur,
    wage_eur,
    release_clause_eur,
    pace,
    shooting,
    passing,
    dribbling,
    defending,
    physic,
    gender,
    CASE
        WHEN age < 21 THEN 'U21'
        WHEN age BETWEEN 21
        AND 28 THEN 'Prime'
        ELSE 'Veteran'
    END AS age_group,
    CASE
        WHEN player_positions LIKE '%RB%'
        OR player_positions LIKE '%LB%' THEN 'Wingback'
        WHEN player_positions LIKE '%ST%'
        OR player_positions LIKE '%CF%' THEN 'Poacher'
        WHEN player_positions LIKE '%CM%'
        OR player_positions LIKE '%CDM%' THEN 'Regista'
        ELSE 'Other'
    END AS role_classification
FROM
    male_players