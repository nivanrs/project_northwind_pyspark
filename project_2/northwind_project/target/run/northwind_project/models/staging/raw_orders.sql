
  
    

  create  table "northwind"."public"."raw_orders__dbt_tmp"
  
  
    as
  
  (
    select
    *      
from
    "northwind"."public"."orders"
  );
  