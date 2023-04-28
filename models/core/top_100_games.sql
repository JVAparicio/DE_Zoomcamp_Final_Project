{{ config(
    materialized='table',
    partition_by={
      "field": "rank_date",
      "data_type": "timestamp",
      "granularity": "year"
    }
)}}


select *
from {{ ref('stg_top_100_2017') }}

union all

select *
from {{ ref('stg_top_100_2018') }}

union all

select *
from {{ ref('stg_top_100_2019') }}

union all

select *
from {{ ref('stg_top_100_2020') }}

union all

select *
from {{ ref('stg_top_100_2021') }}


union all

select *
from {{ ref('stg_top_100_2022') }}