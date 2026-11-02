-- =============================================================================
-- business_analysis.sql
-- Business Analysis Queries on cleaned_food_orders
-- Run these against food_orders_analytics_db
-- =============================================================================

USE food_orders_analytics_db;

-- ---------------------------------------------------------------------------
-- Q1. What is the total order value?
-- ---------------------------------------------------------------------------
SELECT
    ROUND(SUM(`Order Value`), 2) AS Total_Order_Value
FROM cleaned_food_orders;

-- ---------------------------------------------------------------------------
-- Q2. What is the average order value?
-- ---------------------------------------------------------------------------
SELECT
    ROUND(AVG(`Order Value`), 2) AS Average_Order_Value
FROM cleaned_food_orders;

-- ---------------------------------------------------------------------------
-- Q3. Which payment method has the highest number of orders?
-- ---------------------------------------------------------------------------
SELECT
    `Payment Method`,
    COUNT(*) AS Number_of_Orders
FROM cleaned_food_orders
GROUP BY `Payment Method`
ORDER BY Number_of_Orders DESC
LIMIT 5;

-- ---------------------------------------------------------------------------
-- Q4. Which order channel has the highest number of orders?
-- ---------------------------------------------------------------------------
SELECT
    `Order Channel`,
    COUNT(*) AS Number_of_Orders
FROM cleaned_food_orders
GROUP BY `Order Channel`
ORDER BY Number_of_Orders DESC
LIMIT 5;

-- ---------------------------------------------------------------------------
-- Q5. What is the average delivery time in minutes?
-- ---------------------------------------------------------------------------
SELECT
    ROUND(AVG(Delivery_Time_Minutes), 2) AS Avg_Delivery_Time_Minutes
FROM cleaned_food_orders
WHERE Delivery_Time_Minutes >= 0;

-- ---------------------------------------------------------------------------
-- Q6. What is the total platform net revenue?
--     Formula: Commission Fee + Delivery Fee
--              − Payment Processing Fee − Refunds/Chargebacks
-- ---------------------------------------------------------------------------
SELECT
    ROUND(SUM(Platform_Net_Revenue), 2)                          AS Total_Platform_Net_Revenue,
    ROUND(
        SUM(`Commission Fee` + `Delivery Fee`
            - `Payment Processing Fee` - `Refunds/Chargebacks`),
        2
    )                                                            AS Total_Verified_Net_Revenue
FROM cleaned_food_orders;
