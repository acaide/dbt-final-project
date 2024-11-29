{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT DISTINCT
        MD5(date) AS date_id
        , date
        , isholiday
        , CURRENT_TIMESTAMP as insert_date
    FROM
        {{ source("STAGING","STAGING_DEPARTMENT") }}
)

SELECT * FROM existing_data