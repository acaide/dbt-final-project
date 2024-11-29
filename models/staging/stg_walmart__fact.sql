{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT
        date
        , store_id
        , dept_id --REMOVED - running into duplication issues!
        , store_type
        , store_size
        , weekly_sales
        , fuel_price
        , temperature
        , unemployment
        , cpi
        , markdown1
        , markdown2
        , markdown3
        , markdown4
        , markdown5
        , CURRENT_TIMESTAMP as updated_at
    FROM
        {{ source("STAGING","STAGING_FACT") }}  
    
    -- add store_type, store_size
    LEFT JOIN (
        SELECT
            *
        FROM {{ source("STAGING","STAGING_STORE") }} 
    ) store
    USING (store_id)  

    -- add weekly_sales, dept_id
    LEFT JOIN ( 
        SELECT
            store_id
            , date
            , dept_id
            , weekly_sales
        FROM {{ source("STAGING","STAGING_DEPARTMENT") }}
    ) dep
    USING(store_id, date)
)

SELECT * FROM existing_data