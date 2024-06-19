
  
    

  create  table "northwind"."public"."dm_monthly_supplier_gross_revenue__dbt_tmp"
  
  
    as
  
  (
    WITH order_details AS (
    SELECT
        "ORDERID",
        "PRODUCTID",
        "UNITPRICE",
        "QUANTITY",
        "DISCOUNT",
        ("UNITPRICE" * (1 - "DISCOUNT") * "QUANTITY") AS gross_revenue
    FROM "northwind"."public"."raw_order_details"
),
orders AS (
    SELECT
        "ORDERID",
        "SHIPPEDDATE"
    FROM "northwind"."public"."raw_orders"
),
products AS (
    SELECT
        "PRODUCTID",
        "SUPPLIERID"
    FROM "northwind"."public"."raw_products"
)
SELECT
    s."COMPANYNAME",
    TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM') AS month,
    SUM(od.gross_revenue) AS gross_revenue
FROM order_details od
JOIN orders o ON od."ORDERID" = o."ORDERID"
JOIN products p ON od."PRODUCTID" = p."PRODUCTID"
JOIN "northwind"."public"."raw_suppliers" s ON p."SUPPLIERID" = s."SUPPLIERID"
GROUP BY s."COMPANYNAME", TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM')
ORDER BY month DESC
  );
  