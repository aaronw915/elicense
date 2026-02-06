SELECT *
FROM {{ ref('stg_licenses') }}
WHERE business_license = true
