{{ 
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key=['store_id', 'dept_id'],
        merge_exclude_columns = ['insert_date']
        
    ) 
}}

-- apply SCD1 logic
changes AS (
    SELECT
        s.store_id
        , s.dept_id
        , s.store_type
        , s.store_size
        , insert_date as src_insert_date
        , CURRENT_TIMESTAMP as insert_date
        , CURRENT_TIMESTAMP as update_date
    FROM new_data s
)

SELECT * FROM changes
