{{ config(materialized='table') }}

WITH existing_data AS (
    SELECT DISTINCT
        MD5(date) AS date_id
        , date
        , isholiday
    FROM
        {{ source("STAGING","STAGING_FACT") }}
)

SELECT * FROM existing_data