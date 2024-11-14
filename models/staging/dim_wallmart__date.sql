{{ 
    config(
        materialized='incremental',
        unique_key='date_id') 
}}

-- apply SCD1 logic
WITH changes AS (
    SELECT 
        s.date_id,
        s.date,
        s.isholiday,
        -- Insert the current timestamp for new rows; ALWAYS apply
        CURRENT_TIMESTAMP AS insert_date,
        -- Set update_date IF there is a change
        CASE 
            WHEN e.date_id IS NOT NULL AND 
                 (e.date != s.date OR e.isholiday != s.isholiday) 
            THEN CURRENT_TIMESTAMP 
            ELSE e.update_date
        END AS update_date
    FROM {{ ref('stg_wallmart__date') }}  s
    LEFT JOIN {{ source('EXISTING','DATE') }} e
        ON s.date_id = e.date_id
)

SELECT * FROM changes
