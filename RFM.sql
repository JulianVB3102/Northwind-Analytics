-- Build a Customer RFM view
CREATE OR REPLACE VIEW `northwind-analytics-470720.northwind.v_customer_rfm` AS

-- 1) Order-level revenue (avoid double-counting line items)
WITH order_rev AS (
  SELECT
    o.CustomerID,
    o.OrderID,
    DATE(o.OrderDate) AS order_date,
    -- revenue per order: unit price * qty * (1 - discount)
    SUM(od.UnitPrice * od.Quantity * (1 - IFNULL(od.Discount, 0))) AS order_revenue
  FROM `northwind-analytics-470720.northwind.Orders` o
  JOIN `northwind-analytics-470720.northwind.Order_Details` od
    ON o.OrderID = od.OrderID
  GROUP BY o.CustomerID, o.OrderID, order_date
),

-- 2) Customer rollup (when was last order? how many orders? how much spent?)
customer_agg AS (
  SELECT
    c.CustomerID,
    MAX(order_date)                                 AS last_order_dt,   -- most recent purchase
    COUNT(*)                                        AS frequency,       -- number of orders
    SUM(order_revenue)                              AS monetary         -- total spend
  FROM `northwind-analytics-470720.northwind.Customers` c
  JOIN order_rev r USING (CustomerID)  -- keeps only customers with orders
  GROUP BY c.CustomerID
),

-- 3) Add recency in days (smaller is better)
scores AS (
  SELECT
    CustomerID,
    DATE_DIFF(CURRENT_DATE(), last_order_dt, DAY)   AS recency_days,
    frequency,
    monetary
  FROM customer_agg
),

-- 4) Rank into quintiles and format the RFM code
ranked AS (
  SELECT
    CustomerID,
    recency_days,
    frequency,
    ROUND(monetary, 2)                              AS monetary,
    -- R is inverted: more recent (smaller recency_days) should get higher score
    NTILE(5) OVER (ORDER BY recency_days DESC)      AS r_score,
    NTILE(5) OVER (ORDER BY frequency)              AS f_score,
    NTILE(5) OVER (ORDER BY monetary)               AS m_score
  FROM scores
)

-- 5) Final output
SELECT
  CustomerID,
  recency_days,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  -- compact RFM label like "543"
  FORMAT('%d%d%d', r_score, f_score, m_score)       AS rfm_segment
FROM ranked
ORDER BY monetary DESC;
