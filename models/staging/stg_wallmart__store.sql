{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT
        MD5(CONCAT(store_id, dept_id)) AS location_id
        , store_id
        , dept_id
        , size
    FROM
        {{ source("STAGING","STAGING_STORE") }}  
)

SELECT * FROM existing_data