{{ config(
        materialized='view'
    )
}}


select * from {{ ref('dbt_demo_core', 'f2') }}