WITH addresses AS (
    SELECT * FROM {{ ref('stg_address') }}
),

licenses AS (
    SELECT * FROM {{ ref('stg_license') }}
)

SELECT
    l.license_id,
    CASE
        WHEN l.hide_from_public = TRUE THEN 'HIDDEN'
        ELSE a.parcel_city
    END AS parcel_city,
    CASE
        WHEN l.hide_from_public = TRUE THEN 'HIDDEN'
        ELSE a.parcel_state
    END AS parcel_state,
    CASE
        WHEN l.hide_from_public  = TRUE THEN 'HIDDEN'
        ELSE a.parcel_county
    END AS parcel_county,
    CASE
        WHEN l.hide_from_public = TRUE THEN 'HIDDEN'
        ELSE a.parcel_zip_code
    END AS parcel_zip_code,
    CASE
        WHEN l.hide_from_public = TRUE THEN 'HIDDEN'
        ELSE a.parcel_country
    END AS parcel_country
FROM
    licenses l
LEFT JOIN addresses a ON l.license_id = a.license_id
