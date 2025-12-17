-- ============================================================================
-- TEST: Employee SCD2 Has Exactly One Current Version Per Employee
-- ============================================================================
-- Layer: Gold (Data Quality - SCD2 Validation)
-- Type: Singular Test (Custom SQL)
-- Dimension: Uniqueness - "Is there exactly one current version?"
-- 
-- WHAT THIS TESTS:
--   - Each employee (EMPLOYEE_NUMBER) has exactly ONE row with IS_CURRENT = TRUE
--   - No employees have zero current versions (missing data)
--   - No employees have multiple current versions (deduplication bug)
--
-- BUSINESS RULES:
--   - IS_CURRENT = TRUE means this is the active employee version
--   - Only ONE version can be current at any time
--   - All active employees must have a current version
--
-- WHEN IT FAILS:
--   - 0 current versions: Employee terminated or SCD2 logic bug
--   - 2+ current versions: Deduplication failed in snapshot
--
-- HOW TO DEBUG:
--   1. Identify failing EMPLOYEE_NUMBER
--   2. Query dim_employee_scd2 for all versions:
--      SELECT * FROM dim_employee_scd2 WHERE EMPLOYEE_NUMBER = XXX ORDER BY VALID_FROM
--   3. Check snapshot logic in snapshots/scd_employee.sql
--   4. Verify VALID_TO is NULL for current version only
--
-- BUSINESS IMPACT IF IGNORED:
--   - Sales rep assignment incorrect
--   - Organizational hierarchy broken
--   - HR reports show wrong data
-- ============================================================================

SELECT
    EMPLOYEE_NUMBER,
    COUNT(*) AS current_version_count
FROM {{ ref('dim_employee_scd2') }}
WHERE IS_CURRENT = TRUE
GROUP BY EMPLOYEE_NUMBER
HAVING COUNT(*) != 1

-- Returns employees with != 1 current version (test fails)
-- Returns 0 rows if all employees have exactly 1 current version (test passes)
