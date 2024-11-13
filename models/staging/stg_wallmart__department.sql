{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT
        MD5(CONCAT(store_id, dept_id)) AS location_id
        , store_id
        , dept_id
        , date
        , weekly_sales
        , isholiday
    FROM
        {{ source("STAGING","STAGING_DEPARTMENT") }}
)

SELECT * FROM existing_data