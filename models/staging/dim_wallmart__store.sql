{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id']
) }}

WITH new_data AS (
    SELECT *
    FROM {{ ref('stg_wallmart__store') }}
    LEFT JOIN (
        SELECT DISTINCT
        store_id
        , dept_id
    FROM {{ source("STAGING","STAGING_DEPARTMENT") }})
    USING(store_id)
),

-- apply SCD1 logic
changes AS (
    SELECT
        s.store_id
        , s.dept_id
        , s.store_type
        , s.store_size
        -- Set insert_date only for new rows (i.e., if no match found in existing_data)
        , CASE 
            WHEN e.store_id IS NULL OR e.dept_id IS NULL THEN CURRENT_TIMESTAMP 
            ELSE e.insert_date  -- Keep existing insert_date if record already exists
        END AS insert_date
        -- Update update_date when store_type or store_size changes
        , CASE 
            WHEN e.store_id IS NULL OR e.dept_id IS NULL THEN NULL
            WHEN e.store_type != s.store_type 
                OR e.store_size != s.store_size THEN CURRENT_TIMESTAMP
            ELSE e.update_date
        END AS update_date
    FROM new_data s
    LEFT JOIN {{ source('EXISTING','STORE') }} e
        ON s.store_id = e.store_id
        AND s.dept_id = e.dept_id
)

SELECT * FROM changes
