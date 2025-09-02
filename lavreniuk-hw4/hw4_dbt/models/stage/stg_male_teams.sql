{{ config(
    materialized='table',
    tags=['stage', 'male_teams']
) }}

with src as (
  select
    {{ select_snake(source('raw','male_teams'), alias='s', type_map=var('male_teams_type_map', {})) }}
  from {{ source('raw','male_teams') }} as s
)
select
  *
from src