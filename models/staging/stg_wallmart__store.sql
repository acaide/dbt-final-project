{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT
        store_id
        , store_type
        , store_size
    FROM
        {{ source("STAGING","STAGING_STORE") }}  
)

SELECT * FROM existing_data