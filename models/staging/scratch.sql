{{ config(materialized='table') }}

WITH new_data AS (
    SELECT *
    FROM {{ ref('stg_wallmart__store') }}
    LEFT JOIN (
        SELECT DISTINCT
        store_id
        , dept_id
    FROM {{ source("STAGING","STAGING_DEPARTMENT") }})
    USING(store_id)
)
SELECT * FROM new_data