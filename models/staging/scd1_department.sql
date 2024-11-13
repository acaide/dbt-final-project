{{ 
    config(
        materialized='incremental',
        unique_key='store_id') 
}}

-- apply SCD1 logic

WITH updated_data AS (
    SELECT * 
    FROM {{ ref('stg_wallmart__department') }} s
    LEFT JOIN {{ source('SOURCE','DEPARTMENT') }} target
        ON target.location_id = s.location_id
    WHERE
        target.location_id IS NULL  -- condition for new store
        OR target.dept_id != s.dept_id -- conditions for updated values
        OR target.date != s.date
        OR target.weekly_sales != s.weekly_sales
        OR target.isholiday != s.isholiday
)

SELECT * FROM updated_data
