SELECT *
FROM {{ ref('dim_license_specialties_pivoted') }}
WHERE specialty_count >= 10
