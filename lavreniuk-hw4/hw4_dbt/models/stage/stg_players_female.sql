{{ config(
    materialized='table',
    tags=['stage', 'female_player']
) }}

with src as (
  select
    {{ select_snake(source('raw','female_players'), alias='s', type_map=var('female_players_type_map', {})) }}
  from {{ source('raw','female_players') }} as s
)
select
  *
from src