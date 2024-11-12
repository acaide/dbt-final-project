WITH raw_data AS (
    SELECT
        store
        , dept
        , date
        , weekly_sales
        , isholiday
    FROM
        {{ source("STAGING","DEPARTMENT") }}  
)

SELECT * FROM raw_data
WHERE customer_id IS NOT NULL