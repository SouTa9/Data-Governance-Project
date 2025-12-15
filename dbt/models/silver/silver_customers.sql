{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='customer_number'
  )
}}

with source_data as (
    select
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        CONTACT_FIRST_NAME,
        CONTACT_LAST_NAME,
        PHONE,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        SALES_REP_EMPLOYEE_NUMBER,
        CREDIT_LIMIT,
        _LOADED_AT,
        _SOURCE_TABLE
    from {{ source('bronze', 'customers') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by CUSTOMER_NUMBER
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest_customers as (
    select * from ranked where ROW_NUM = 1
)
select
    CUSTOMER_NUMBER,
    initcap({{ normalize_text('CUSTOMER_NAME') }}) as CUSTOMER_NAME,
    initcap({{ normalize_text('CONTACT_FIRST_NAME') }}) as CONTACT_FIRST_NAME,
    initcap({{ normalize_text('CONTACT_LAST_NAME') }}) as CONTACT_LAST_NAME,
    {{ normalize_text('PHONE') }} as PHONE,
    {{ normalize_text('ADDRESS_LINE_1') }} as ADDRESS_LINE_1,
    {{ normalize_text('ADDRESS_LINE_2') }} as ADDRESS_LINE_2,
    initcap({{ normalize_text('CITY') }}) as CITY,
    {{ normalize_upper('STATE') }} as STATE,
    {{ normalize_text('POSTAL_CODE') }} as POSTAL_CODE,
    {{ normalize_upper('COUNTRY') }} as COUNTRY,
    case
        when COUNTRY in ('USA', 'CANADA') then 'NA'
        when COUNTRY in ('FRANCE', 'SPAIN', 'NORWAY', 'SWEDEN', 'FINLAND', 'DENMARK', 'UK', 'GERMANY') then 'EMEA'
        when COUNTRY in ('JAPAN', 'SINGAPORE', 'AUSTRALIA', 'HONG KONG') then 'APAC'
        else 'OTHER'
    end as REGION,
    SALES_REP_EMPLOYEE_NUMBER,
    case when CREDIT_LIMIT is not null and CREDIT_LIMIT > 0 then CREDIT_LIMIT end as CREDIT_LIMIT,
    PHONE is null as IS_MISSING_PHONE,
    CREDIT_LIMIT is null as IS_MISSING_CREDIT_LIMIT,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest_customers
