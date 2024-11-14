{{ 
    config(
        materialized='incremental',
        unique_key='date_id') 
}}

-- apply SCD1 logic
WITH changes AS (
    SELECT 
        s.date_id
        , s.date
        , s.isholiday
        -- Set insert_date only for new rows (i.e., when there's no match in the existing data)
        , CASE 
            WHEN e.date_id IS NULL THEN CURRENT_TIMESTAMP
            ELSE e.insert_date  -- Retain existing insert_date if the row already exists
        END AS insert_date
        -- Set update_date only if there's a change in the data (i.e., date or isholiday has changed)
        , CASE 
            WHEN e.date_id IS NULL THEN NULL  -- New rows should have NULL for update_date
            WHEN e.date != s.date OR e.isholiday != s.isholiday THEN CURRENT_TIMESTAMP
            ELSE e.update_date  -- Retain the existing update_date if there's no change
        END AS update_date
    FROM {{ ref('stg_wallmart__date') }}  s
    LEFT JOIN {{ source('EXISTING','DATE') }} e
        ON s.date_id = e.date_id
)

SELECT * FROM changes
