
  
    

  create  table "northwind"."public"."raw_products__dbt_tmp"
  
  
    as
  
  (
    select
    *     
from
    "northwind"."public"."products"
  );
  