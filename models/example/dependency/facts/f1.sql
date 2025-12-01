{{ config(
        materialized='table'
    )
}}


SELECT r2.id, r2.val, d1.r1_val, d1.r2_val
FROM {{ source('dlt', 'r2') }}
JOIN {{ ref('d1') }} d1 ON r2.id = d1.id