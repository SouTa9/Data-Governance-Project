{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='office_code'
  )
}}

with source_data as (
    select * from {{ source('bronze', 'offices') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by OFFICE_CODE
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    OFFICE_CODE,
    initcap({{ normalize_text('CITY') }}) as CITY,
    {{ normalize_upper('STATE') }} as STATE,
    {{ normalize_upper('COUNTRY') }} as COUNTRY,
    {{ normalize_text('POSTAL_CODE') }} as POSTAL_CODE,
    initcap({{ normalize_text('TERRITORY') }}) as TERRITORY,
    {{ normalize_text('PHONE') }} as PHONE,
    {{ normalize_text('ADDRESS_LINE_1') }} as ADDRESS_LINE_1,
    {{ normalize_text('ADDRESS_LINE_2') }} as ADDRESS_LINE_2,
    case
        when COUNTRY in ('USA', 'CANADA') then 'NA'
        when COUNTRY in ('FRANCE', 'SPAIN', 'NORWAY', 'SWEDEN', 'FINLAND', 'DENMARK', 'UK', 'GERMANY') then 'EMEA'
        when COUNTRY in ('JAPAN', 'SINGAPORE', 'AUSTRALIA', 'HONG KONG') then 'APAC'
        else 'OTHER'
    end as REGION,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
