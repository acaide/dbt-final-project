{{ 
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key='date_id',
        merge_exclude_columns = ['insert_date']
        
    ) 
}}

WITH changes AS (
    SELECT 
        s.date_id
        , s.date
        , s.isholiday
        , insert_date as src_insert_date
        , CURRENT_TIMESTAMP as insert_date
        , CURRENT_TIMESTAMP as update_date
    FROM {{ ref('stg_walmart__date') }}  s
)
SELECT * FROM changes

{% if is_incremental() -%}
where src_insert_date >= (select max(insert_date) from {{ this }})
{%- endif -%}
