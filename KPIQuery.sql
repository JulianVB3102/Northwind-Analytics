CREATE OR REPLACE VIEW `northwind-analytics-470720.northwind.v_kpi_last_12m` AS
WITH last12 AS (
  SELECT *
  FROM `northwind-analytics-470720.northwind.Orders`
  WHERE DATE(OrderDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
),
rev AS (
  SELECT o.OrderID,
         SUM(od.UnitPrice * od.Quantity * (1 - IFNULL(od.Discount,0))) AS revenue
  FROM last12 o
  JOIN `northwind-analytics-470720.northwind.Order_Details` od
    ON o.OrderID = od.OrderID
  GROUP BY o.OrderID
)
SELECT
  COUNT(DISTINCT o.OrderID) AS orders,
  COUNT(DISTINCT o.CustomerID) AS customers,
  ROUND(SUM(rev.revenue),2) AS revenue,
  ROUND(SAFE_DIVIDE(SUM(rev.revenue), COUNT(DISTINCT o.OrderID)),2) AS avg_order_value
FROM last12 o
JOIN rev USING (OrderID);
