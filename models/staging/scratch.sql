{{ config(materialized='table') }}

SELECT * FROM {{ ref("stg_wallmart__store") }}