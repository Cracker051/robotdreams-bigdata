{{ config(
    materialized='table',
    tags=['stage', 'female_teams']
) }}

with src as (
  select
    {{ select_snake(source('raw','female_teams'), alias='s', type_map=var('female_teams_type_map', {})) }}
  from {{ source('raw','female_teams') }} as s
)
select
  *
from src