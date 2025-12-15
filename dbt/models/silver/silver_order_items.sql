{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key=['order_number', 'product_code']
  )
}}

with source_data as (
    select * from {{ source('bronze', 'order_details') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by ORDER_NUMBER, PRODUCT_CODE
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    ORDER_NUMBER,
    ORDER_LINE_NUMBER,
    PRODUCT_CODE,
    QUANTITY_ORDERED,
    PRICE_EACH,
    QUANTITY_ORDERED * PRICE_EACH as LINE_REVENUE,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
