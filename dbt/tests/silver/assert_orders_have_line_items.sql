-- ============================================================================
-- TEST: All Orders Have At Least One Line Item
-- ============================================================================
-- Layer: Silver (Data Quality - Consistency)
-- Type: Singular Test (Custom SQL)
-- Dimension: Consistency - "Is data consistent across related tables?"
-- 
-- WHAT THIS TESTS:
--   - Referential integrity: Orders must have line items
--   - No orphaned order headers
--   - Data completeness across parent/child relationship
--
-- WHEN IT FAILS:
--   - Order header exists without any products
--   - Data ingestion partial failure
--   - Missing foreign key enforcement in source
--
-- HOW TO DEBUG:
--   1. Check source system for orphaned orders
--   2. Verify order_details ingestion completed
--   3. Compare ORDER_NUMBER across both Bronze tables
--
-- BUSINESS IMPACT IF IGNORED:
--   - Orders with $0 revenue (no line items)
--   - Incomplete order history
--   - Customer service issues
-- ============================================================================

SELECT
    o.ORDER_NUMBER,
    o.ORDER_DATE,
    o.CUSTOMER_NUMBER
FROM {{ ref('silver_orders') }} o
LEFT JOIN {{ ref('silver_order_items') }} oi
    ON o.ORDER_NUMBER = oi.ORDER_NUMBER
WHERE oi.ORDER_NUMBER IS NULL

-- Returns orphaned orders (test fails)
-- Returns 0 rows if all orders have line items (test passes)
