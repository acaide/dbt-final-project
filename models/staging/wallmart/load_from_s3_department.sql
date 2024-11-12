WITH s3_data AS (
  SELECT
    $1 AS store
    , $2 AS dept
    , $3 AS date
    , $4 AS weekly_sales
    , $5 AS isholiday
  FROM 
    {{ source("STAGING","SCD1_DEPARTMENT")}}
)

SELECT * FROM s3_data;
