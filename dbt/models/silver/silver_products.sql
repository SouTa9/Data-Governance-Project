{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='product_code'
  )
}}

with source_data as (
    select * from {{ source('bronze', 'products') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by PRODUCT_CODE
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    PRODUCT_CODE,
    initcap({{ normalize_text('PRODUCT_NAME') }}) as PRODUCT_NAME,
    {{ normalize_text('PRODUCT_LINE') }} as PRODUCT_LINE,
    {{ normalize_text('PRODUCT_SCALE') }} as PRODUCT_SCALE,
    initcap({{ normalize_text('PRODUCT_VENDOR') }}) as PRODUCT_VENDOR,
    {{ normalize_text('PRODUCT_DESCRIPTION') }} as PRODUCT_DESCRIPTION,
    QUANTITY_IN_STOCK,
    BUY_PRICE,
    MSRP,
    round({{ safe_divide('MSRP - BUY_PRICE', 'nullif(BUY_PRICE,0)', 'null') }} * 100, 2) as PROFIT_MARGIN_PCT,
    case
        when QUANTITY_IN_STOCK is null then 'UNKNOWN'
        when QUANTITY_IN_STOCK = 0 then 'OUT_OF_STOCK'
        when QUANTITY_IN_STOCK < 100 then 'LOW_STOCK'
        when QUANTITY_IN_STOCK < 500 then 'MEDIUM_STOCK'
        else 'HIGH_STOCK'
    end as STOCK_STATUS,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
