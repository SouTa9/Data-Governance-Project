{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key=['customer_number', 'check_number']
  )
}}

with source_data as (
    select * from {{ source('bronze', 'payments') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by CUSTOMER_NUMBER, CHECK_NUMBER
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    CUSTOMER_NUMBER,
    CHECK_NUMBER,
    PAYMENT_DATE,
    AMOUNT,
    case when AMOUNT < 0 then 'REFUND' else 'PAYMENT' end as PAYMENT_TYPE,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
