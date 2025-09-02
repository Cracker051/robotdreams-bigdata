{{ config(
    materialized='table',
    tags=['stage', 'male_coaches']
) }}

with src as (
  select
    {{ select_snake(source('raw','male_coaches'), alias='s', type_map=var('male_coaches_type_map', {})) }}
  from {{ source('raw','male_coaches') }} as s
)
select
  *
from src