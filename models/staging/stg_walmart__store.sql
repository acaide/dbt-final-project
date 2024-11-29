{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT
        store_id
        , dept_id
        , store_type
        , store_size
        , CURRENT_TIMESTAMP as insert_date
    FROM
        {{ source("STAGING","STAGING_STORE") }}  
    -- add dept_id to data from STAGING_DEPARTMENT
    LEFT JOIN (
        SELECT DISTINCT
        store_id
        , dept_id
    FROM {{ source("STAGING","STAGING_DEPARTMENT") }})
    USING(store_id)
)

SELECT * FROM existing_data