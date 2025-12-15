{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'scd2']
    )
}}

-- ============================================================================
-- SCD TYPE 2 CUSTOMER DIMENSION
-- Purpose: Business-ready customer dimension with full history tracking
-- Source: customers_snapshot (SNAPSHOTS schema)
-- 
-- SCD2 History: Each customer can have multiple rows representing different
--               versions over time. IS_CURRENT=TRUE marks the active version.
-- ============================================================================

SELECT
    -- Surrogate Key (unique per version)
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_NUMBER', 'DBT_VALID_FROM']) }} AS CUSTOMER_KEY,
    
    -- Business Key
    CUSTOMER_NUMBER,
    
    -- Customer Attributes
    CUSTOMER_NAME,
    CONTACT_FIRST_NAME,
    CONTACT_LAST_NAME,
    CONCAT(CONTACT_FIRST_NAME, ' ', CONTACT_LAST_NAME) AS CONTACT_FULL_NAME,
    PHONE,
    
    -- Address (SCD Type 2 tracked)
    ADDRESS_LINE_1,
    ADDRESS_LINE_2,
    CITY,
    STATE,
    POSTAL_CODE,
    COUNTRY,
    
    -- Business Data
    SALES_REP_EMPLOYEE_NUMBER,
    CREDIT_LIMIT,
    
    -- Credit Tier (derived)
    CASE 
        WHEN CREDIT_LIMIT >= 100000 THEN 'Platinum'
        WHEN CREDIT_LIMIT >= 50000 THEN 'Gold'
        WHEN CREDIT_LIMIT >= 25000 THEN 'Silver'
        ELSE 'Bronze'
    END AS CREDIT_TIER,
    
    -- SCD Type 2 Columns (from dbt snapshot)
    DBT_VALID_FROM AS VALID_FROM,
    COALESCE(DBT_VALID_TO, '9999-12-31'::TIMESTAMP) AS VALID_TO,
    CASE WHEN DBT_VALID_TO IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,
    
    -- Row Version (for ordering)
    ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_NUMBER 
        ORDER BY DBT_VALID_FROM
    ) AS VERSION_NUMBER,
    
    -- Audit Columns
    _LOADED_AT AS SOURCE_LOADED_AT,
    _SOURCE_TABLE AS SOURCE_TABLE,
    DBT_UPDATED_AT AS SNAPSHOT_UPDATED_AT,
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM {{ ref('customers_snapshot') }}
