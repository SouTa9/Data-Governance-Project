{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='order_number'
  )
}}

with source_data as (
    select * from {{ source('bronze', 'orders') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by ORDER_NUMBER
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    ORDER_NUMBER,
    ORDER_DATE,
    REQUIRED_DATE,
    SHIPPED_DATE,
    {{ normalize_upper('STATUS') }} as STATUS,
    {{ normalize_text('COMMENTS') }} as COMMENTS,
    CUSTOMER_NUMBER,
    datediff(day, ORDER_DATE, SHIPPED_DATE) as DAYS_TO_SHIP,
    datediff(day, ORDER_DATE, REQUIRED_DATE) as DAYS_TO_REQUIRED,
    case
        when SHIPPED_DATE is null and current_date > REQUIRED_DATE then true
        when SHIPPED_DATE > REQUIRED_DATE then true
        else false
    end as IS_LATE,
    case when STATUS = 'CANCELLED' then true else false end as IS_CANCELLED,
    case when STATUS in ('IN PROCESS', 'ON HOLD', 'RESOLVED') then true else false end as IS_OPEN_ORDER,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
