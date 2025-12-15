{{
  config(
    materialized='table',
    schema='silver',
    tags=['silver'],
    unique_key='employee_number'
  )
}}

with source_data as (
    select
        EMPLOYEE_NUMBER,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        EXTENSION,
        JOB_TITLE,
        OFFICE_CODE,
        REPORTS_TO,
        _LOADED_AT,
        _SOURCE_TABLE
    from {{ source('bronze', 'employees') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by EMPLOYEE_NUMBER
            order by _LOADED_AT desc
        ) as ROW_NUM
    from source_data
),
latest as (
    select * from ranked where ROW_NUM = 1
)
select
    EMPLOYEE_NUMBER,
    initcap({{ normalize_text('FIRST_NAME') }}) as FIRST_NAME,
    initcap({{ normalize_text('LAST_NAME') }}) as LAST_NAME,
    {{ normalize_text('EMAIL') }} as EMAIL,
    {{ normalize_text('EXTENSION') }} as PHONE_EXTENSION,
    {{ normalize_upper('JOB_TITLE') }} as JOB_TITLE,
    case when upper(coalesce(trim(JOB_TITLE), '')) like '%SALES%' then true else false end as IS_SALES_ROLE,
    OFFICE_CODE,
    REPORTS_TO as MANAGER_EMPLOYEE_NUMBER,
    _LOADED_AT,
    _SOURCE_TABLE,
    current_timestamp() as DBT_UPDATED_AT
from latest
