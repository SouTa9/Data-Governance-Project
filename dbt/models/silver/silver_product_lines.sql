{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='product_line'
  )
}}

with source_data as (
    select * from {{ source('bronze', 'product_lines') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by PRODUCT_LINE
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    PRODUCT_LINE,
    {{ normalize_text('TEXT_DESCRIPTION') }} as TEXT_DESCRIPTION,
    {{ normalize_text('HTML_DESCRIPTION') }} as HTML_DESCRIPTION,
    IMAGE,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
