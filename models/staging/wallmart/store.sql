WITH raw_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        updated_at
    FROM
        {{ source("STAGING","SCD1_STORE") }}  
)

SELECT * FROM raw_data
WHERE customer_id IS NOT NULL