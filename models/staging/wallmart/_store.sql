WITH raw_data AS (
    SELECT
        store
        , dept
        , size
    FROM
        {{ source("STAGING","STORE") }}  
)

SELECT * FROM raw_data
WHERE customer_id IS NOT NULL