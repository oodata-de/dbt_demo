{{ config(
        materialized='table'
    )
}}


SELECT r2.id, r2.val AS r2_val, r3.val AS r3_val, 1 as fixed_value
FROM {{ source('dlt', 'r2') }} r2
JOIN {{ source('dependency', 'r3') }} r3 ON r2.id = r3.id