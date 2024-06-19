
  
    

  create  table "northwind"."public"."raw_categories__dbt_tmp"
  
  
    as
  
  (
    select
    *    
from 
    "northwind"."public"."categories"
  );
  