SELECT *
FROM {{ ref('stg_license') }}
WHERE business_license = 'false'
