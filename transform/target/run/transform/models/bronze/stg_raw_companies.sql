
  create or replace view
    "lakehouse"."bronze"."stg_raw_companies"
  security definer
  as
    

EXPLAIN 
SELECT * FROM bronze.raw_companies_listing 
LIMIT 10;
  ;
