SELECT *
FROM {{ ref('int_license_specialties_pivoted') }}
WHERE specialty_count >= 10
