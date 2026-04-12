{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with source_data as (
    select
    id,
    customer_id
    from { source('raw', 'orders') }
),

select * from source_data
