
  
    

  create  table "northwind"."public"."raw_suppliers__dbt_tmp"
  
  
    as
  
  (
    select
    *    
from
    "northwind"."public"."suppliers"
  );
  