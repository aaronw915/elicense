SELECT *
FROM {{ ref('int_license_endorsements_pivoted') }}
WHERE endorsement_count >= 10