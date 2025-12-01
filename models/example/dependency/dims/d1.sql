{{ config(
        materialized='table'
    )
}}


SELECT r1.id, r1.val AS r1_val, r2.val AS r2_val
FROM {{ source('dependency', 'r1') }} r1
JOIN {{ source('dlt', 'r2') }} ON r1.id = r2.id