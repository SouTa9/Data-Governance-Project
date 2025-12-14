{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'scd2']
    )
}}

-- ============================================================================
-- SCD TYPE 2 EMPLOYEE DIMENSION
-- Purpose: Business-ready employee dimension with job/office history
-- Source: employees_snapshot (SNAPSHOTS schema)
-- 
-- SCD2 History: Each employee can have multiple rows representing different
--               versions over time (promotions, transfers, etc.)
-- ============================================================================

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['EMPLOYEE_NUMBER', 'DBT_VALID_FROM']) }} AS EMPLOYEE_KEY,
    
    -- Business Key
    EMPLOYEE_NUMBER,
    
    -- Employee Attributes
    FIRST_NAME,
    LAST_NAME,
    CONCAT(FIRST_NAME, ' ', LAST_NAME) AS FULL_NAME,
    EMAIL,
    PHONE_EXTENSION,
    IS_SALES_ROLE,
    
    -- Organization (SCD Type 2 tracked)
    OFFICE_CODE,
    MANAGER_EMPLOYEE_NUMBER,
    JOB_TITLE,
    
    -- Derived: Job Level
    CASE 
        WHEN JOB_TITLE LIKE '%President%' THEN 'Executive'
        WHEN JOB_TITLE LIKE '%VP%' OR JOB_TITLE LIKE '%Manager%' THEN 'Management'
        ELSE 'Staff'
    END AS JOB_LEVEL,
    
    -- Derived: Is Manager
    CASE 
        WHEN JOB_TITLE LIKE '%Manager%' OR JOB_TITLE LIKE '%VP%' OR JOB_TITLE LIKE '%President%' 
        THEN TRUE ELSE FALSE 
    END AS IS_MANAGER,
    
    -- SCD Type 2 Columns
    DBT_VALID_FROM AS VALID_FROM,
    COALESCE(DBT_VALID_TO, '9999-12-31'::TIMESTAMP) AS VALID_TO,
    CASE WHEN DBT_VALID_TO IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,
    
    -- Row Version
    ROW_NUMBER() OVER (
        PARTITION BY EMPLOYEE_NUMBER 
        ORDER BY DBT_VALID_FROM
    ) AS VERSION_NUMBER,
    
    -- Audit Columns
    _LOADED_AT AS SOURCE_LOADED_AT,
    _SOURCE_TABLE AS SOURCE_TABLE,
    DBT_UPDATED_AT AS SNAPSHOT_UPDATED_AT,
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM {{ ref('employees_snapshot') }}
