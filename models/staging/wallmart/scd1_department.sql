-- This ensures dbt only processes new or modified records.
-- unique_key == primary key for incremental updates
{{ config(
    materialized='incremental',
    unique_key='store'  
) }}


-- load data from staging table
WITH s3_data AS (
  SELECT
    $1 AS store
    , $2 AS dept
    , $3 AS date
    , $4 AS weekly_sales
    , $5 AS isholiday
  FROM 
    {{ source("STAGING","SOURCE_DEPARTMENT")}}
)

-- apply SCD1 logic
updated_data AS (
    SELECT
        s.store,
        , s.dept
        , s.date
        , s.weekly_sales
        , s.isholiday
    FROM
        s3_data s
    LEFT JOIN {{ ref('_department') }} target
        ON target.customer_id = s.customer_id
    WHERE
        target.store IS NULL  -- condition for new store
        OR target.dept != s.dept -- conditions for updated values
        OR target.date != s.date
        OR target.weekly_sales != s.weekly_sales
        OR target.isholiday != s.isholiday
)

SELECT * FROM updated_data
