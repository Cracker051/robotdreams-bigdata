{{ config(
    materialized='table',
    tags=['stage', 'female_coaches']
) }}

with src as (
  select
    {{ select_snake(source('raw','female_coaches'), alias='s', type_map=var('female_coaches_type_map', {})) }}
  from {{ source('raw','female_coaches') }} as s
)
select
  *
from src