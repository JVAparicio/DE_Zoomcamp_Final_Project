
{{ config(materialized='view') }}

with top100_2018 as 
(
  select 
    cast(ID as integer) as game_id,
    cast(Name as string) as game_name,
    cast(year as numeric) as game_year,
    cast(Rank as integer) as rank,
    cast(Average as numeric) as average,
    cast(date as timestamp) as rank_date,
  from {{ source('staging','top100_2018') }}
)

select *
from top100_2018
