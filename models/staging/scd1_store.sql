{{ 
    config(
        materialized='incremental',
        unique_key='store_id') 
}}

-- apply SCD1 logic

WITH updated_data AS (
    SELECT * 
    FROM {{ ref('stg_wallmart__store') }} s
    LEFT JOIN {{ source('EXISTING','STORE') }} target
        ON target.location_id = s.location_id
    WHERE
        target.location_id IS NULL  -- condition for new store
        OR target.size != s.size
)

SELECT * FROM updated_data
