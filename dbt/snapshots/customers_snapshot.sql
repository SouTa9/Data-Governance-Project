{% snapshot customers_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='CUSTOMER_NUMBER',
        strategy='check',
        check_cols=[
            'CUSTOMER_NAME',
            'CONTACT_LAST_NAME',
            'CONTACT_FIRST_NAME',
            'PHONE',
            'ADDRESS_LINE_1',
            'ADDRESS_LINE_2',
            'CITY',
            'STATE',
            'POSTAL_CODE',
            'COUNTRY',
            'SALES_REP_EMPLOYEE_NUMBER',
            'CREDIT_LIMIT'
        ],
        invalidate_hard_deletes=true
    )
}}

-- ============================================================================
-- CUSTOMERS SNAPSHOT - SCD TYPE 2 HISTORY TRACKING
-- Purpose: Track all historical changes to customer records
-- Strategy: Check columns for changes
-- ============================================================================

SELECT
    -- Business Key
    CUSTOMER_NUMBER,
    
    -- Contact Information
    CUSTOMER_NAME,
    CONTACT_LAST_NAME,
    CONTACT_FIRST_NAME,
    PHONE,
    
    -- Address Information (tracked for SCD Type 2)
    ADDRESS_LINE_1,
    ADDRESS_LINE_2,
    CITY,
    STATE,
    POSTAL_CODE,
    COUNTRY,
    REGION,
    
    -- Business Data
    SALES_REP_EMPLOYEE_NUMBER,
    CREDIT_LIMIT,
    IS_MISSING_PHONE,
    IS_MISSING_CREDIT_LIMIT,
    
    -- Audit Columns
    _LOADED_AT,
    _SOURCE_TABLE

FROM {{ ref('silver_customers') }}  

{% endsnapshot %}
