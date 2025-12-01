{{ config(
        materialized='table',
        group='sales'
    )
}}


SELECT d1.id, d1.r1_val, d2.r3_val, r4.val AS r4_val
FROM {{ ref('d1') }} d1
JOIN {{ ref('d2') }} d2 ON d1.id = d2.id
JOIN {{ source('dependency', 'r4') }} r4 ON d1.id = r4.id