WITH existing_data AS (
    SELECT
        MD5(CONCAT(store_id, dept_id)) AS location_id
        , store_id
        , dept_id
        , size
    FROM
        {{ source("SOURCE","STORE") }}  
)

SELECT * FROM existing_data
WHERE store IS NOT NULL