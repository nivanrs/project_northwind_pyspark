
  
    

  create  table "northwind"."public"."raw_employees__dbt_tmp"
  
  
    as
  
  (
    select
    *    
from
    "northwind"."public"."employees"
  );
  