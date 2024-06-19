WITH order_details AS (
    SELECT
        "ORDERID",
        "PRODUCTID",
        "QUANTITY"
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
        "CATEGORYID"
    FROM "northwind"."public"."raw_products"
),
categories AS (
    SELECT
        "CATEGORYID",
        "CATEGORYNAME"
    FROM "northwind"."public"."raw_categories"
),
category_sales AS (
    SELECT
        c."CATEGORYNAME",
        TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM') AS month,
        SUM(od."QUANTITY") AS total_quantity
    FROM order_details od
    JOIN orders o ON od."ORDERID" = o."ORDERID"
    JOIN products p ON od."PRODUCTID" = p."PRODUCTID"
    JOIN categories c ON p."CATEGORYID" = c."CATEGORYID"
    GROUP BY c."CATEGORYNAME", TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM')
)
SELECT
    cs."CATEGORYNAME",
    cs.month,
    cs.total_quantity
FROM (
    SELECT
        "CATEGORYNAME",
        month,
        total_quantity,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_quantity DESC) AS row_num
    FROM category_sales
) cs
WHERE cs.row_num = 1
ORDER BY cs.month DESC