{{ config(
    materialized='table',
    tags=['stage', 'male_player']
) }}

with src as (
  select
    {{ select_snake(source('raw','male_players'), alias='s', type_map=var('male_players_type_map', {})) }}
  from {{ source('raw','male_players') }} as s
)
select
  *
from src