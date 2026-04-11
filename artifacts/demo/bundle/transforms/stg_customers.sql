{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

with source_data as (
    select
    id,
    email
    from { source('raw', 'customers') }
),

select * from source_data
