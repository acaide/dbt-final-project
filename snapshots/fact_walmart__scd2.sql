{% snapshot product_snapshot %}
{{
    config(
      target_database='WALMART',
      target_schema='SILVER',
      unique_key=['date','store_id'],
      strategy='timestamp',
      updated_at = 'updated_at'
    )
}}
select * from {{ ref('stg_walmart__fact') }}
{% endsnapshot %}