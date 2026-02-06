SELECT *
FROM {{ ref('dim_license_endorsements_pivoted') }}
WHERE endorsement_count >= 10
