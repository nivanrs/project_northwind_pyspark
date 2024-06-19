
  
    

  create  table "northwind"."public"."raw_order_details__dbt_tmp"
  
  
    as
  
  (
    select
    *      
from
    "northwind"."public"."order_details"
  );
  