/*
 * Test: Validate LINE_PROFIT calculation (should equal LINE_REVENUE - LINE_COST)
 * Layer: Gold
 * Purpose: Ensures derived financial measures are calculated correctly.
 *          This is a critical business logic validation for revenue accounting.
 * Expected Result: No rows returned (all calculations are correct)
 */

SELECT
    ORDER_ITEM_KEY,
    LINE_REVENUE,
    LINE_COST,
    LINE_PROFIT,
    (LINE_REVENUE - LINE_COST) AS expected_profit,
    ABS(LINE_PROFIT - (LINE_REVENUE - LINE_COST)) AS profit_diff
FROM {{ ref('fact_order_items') }}
WHERE ABS(LINE_PROFIT - (LINE_REVENUE - LINE_COST)) >= 0.01  -- Allow 1 cent rounding tolerance
