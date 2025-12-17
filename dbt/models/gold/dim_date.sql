{{
  config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension'],
    unique_key='date_key'
  )
}}

with date_bounds as (
    select
        coalesce(min(ORDER_DATE), current_date - 1095) as MIN_DATE,  -- 3 years back
        coalesce(max(coalesce(SHIPPED_DATE, ORDER_DATE)), current_date + 365) as MAX_DATE  -- 1 year forward
    from {{ ref('silver_orders') }}
),
date_spine as (
    select
        dateadd(day, row_number() over (order by seq4()) - 1, 
                (select MIN_DATE from date_bounds)) as DATE_DAY
    from table(generator(rowcount => 5000))  
    qualify DATE_DAY <= (select dateadd(day, 365, MAX_DATE) from date_bounds)
)
select
    to_number(to_char(DATE_DAY, 'YYYYMMDD')) as DATE_KEY,
    DATE_DAY,
    year(DATE_DAY) as YEAR_NUMBER,
    month(DATE_DAY) as MONTH_NUMBER,
    day(DATE_DAY) as DAY_NUMBER,
    to_char(DATE_DAY, 'YYYY-MM-DD') as DATE_ISO,
    to_char(DATE_DAY, 'Mon') as MONTH_NAME_SHORT,
    to_char(DATE_DAY, 'Month') as MONTH_NAME,
    to_char(DATE_DAY, 'DY') as WEEKDAY_SHORT,
    to_char(DATE_DAY, 'Day') as WEEKDAY_NAME,
    extract(quarter from DATE_DAY) as QUARTER_NUMBER,
    week(DATE_DAY) as WEEK_OF_YEAR,
    case when dayofweek(DATE_DAY) in (6,7) then true else false end as IS_WEEKEND,
    current_timestamp() as DBT_UPDATED_AT
from date_spine
