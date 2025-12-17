{% snapshot employees_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='EMPLOYEE_NUMBER',
        strategy='check',
        check_cols=[
            'FIRST_NAME',
            'LAST_NAME',
            'EMAIL',
            'PHONE_EXTENSION',
            'JOB_TITLE',
            'IS_SALES_ROLE',
            'OFFICE_CODE',
            'MANAGER_EMPLOYEE_NUMBER'
        ],
        invalidate_hard_deletes=true
    )
}}

-- ============================================================================
-- EMPLOYEES SNAPSHOT - SCD TYPE 2 HISTORY TRACKING
-- Purpose: Track job title changes, office transfers, reporting changes
-- Strategy: Check columns for any changes
-- ============================================================================

SELECT
    -- Business Key
    EMPLOYEE_NUMBER,
    
    -- Employee Information
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_EXTENSION,
    JOB_TITLE,
    IS_SALES_ROLE,
    
    -- Organization (tracked for SCD Type 2)
    OFFICE_CODE,
    MANAGER_EMPLOYEE_NUMBER,
    
    -- Audit Columns
    _LOADED_AT,
    _SOURCE_TABLE

FROM {{ ref('silver_employees') }}  

{% endsnapshot %}
