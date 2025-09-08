-- Build a yearly KPI view
CREATE OR REPLACE VIEW `northwind-analytics-470720.northwind.v_yearly_kpis` AS

-- 1) Sum money per order (price × quantity × (1 - discount))
WITH od_agg AS (
  SELECT
    od.OrderID,
    SUM(od.UnitPrice * od.Quantity * (1 - COALESCE(od.Discount, 0))) AS revenue
  FROM `northwind-analytics-470720.northwind.Order_Details` od
  GROUP BY od.OrderID
),

-- 2) Attach a year to each order
orders_with_year AS (
  SELECT
    o.OrderID,
    EXTRACT(YEAR FROM o.OrderDate) AS order_year
  FROM `northwind-analytics-470720.northwind.Orders` o
  WHERE o.OrderDate IS NOT NULL        -- skip bad dates
),

-- 3) One row per order with its year and revenue
order_rev AS (
  SELECT
    oy.order_year,
    oa.revenue
  FROM orders_with_year oy
  JOIN od_agg oa
    USING (OrderID)
)

-- 4) Roll up to year level
SELECT
  order_year,                              -- the calendar year
  COUNT(*) AS orders,                      -- number of orders that year
  ROUND(SUM(revenue), 2) AS revenue,       -- total money that year
  ROUND(SAFE_DIVIDE(SUM(revenue), COUNT(*)), 2) AS avg_order_value -- avg $ per order
FROM order_rev
GROUP BY order_year
ORDER BY order_year;
