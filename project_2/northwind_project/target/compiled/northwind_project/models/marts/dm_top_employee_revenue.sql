WITH order_details AS (
    SELECT
        "ORDERID",
        "PRODUCTID",
        ("UNITPRICE" * (1 - "DISCOUNT") * "QUANTITY") AS gross_revenue
    FROM "northwind"."public"."raw_order_details"
),
orders AS (
    SELECT
        "ORDERID",
        "SHIPPEDDATE",
        "EMPLOYEEID"
    FROM "northwind"."public"."raw_orders"
)
SELECT
    e."FIRSTNAME" || ' ' || e."LASTNAME" AS employee_name,
    TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM') AS month,
    SUM(od.gross_revenue) AS gross_revenue
FROM order_details od
JOIN orders o ON od."ORDERID" = o."ORDERID"
JOIN "northwind"."public"."raw_employees" e ON o."EMPLOYEEID" = e."EMPLOYEEID"
GROUP BY employee_name, TO_CHAR(DATE_TRUNC('month', o."SHIPPEDDATE"), 'YYYY-MM')
ORDER BY gross_revenue DESC
LIMIT 1