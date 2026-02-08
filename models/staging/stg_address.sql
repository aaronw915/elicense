WITH source AS (SELECT * FROM {{ source('ELICENSE_RAW', 'LICENSE_PARCEL') }}),

renamed AS (
    SELECT
        musw__license2__c AS license_id,
        UPPER(parcel_street_address__c) AS parcel_street_address,
        UPPER(parcel_city__c) AS parcel_city,
        UPPER(parcel_country__c) AS parcel_country,
        UPPER(parcel_county__c) AS parcel_county,
        UPPER(parcel_state__c) AS parcel_state,
        parcel_zip_code__c AS parcel_zip_code,
        ROW_NUMBER() OVER (
            PARTITION BY musw__license2__c
            ORDER BY
                createddate DESC
        ) AS rn
    FROM
        source
    WHERE
        public__c = TRUE
)

SELECT * FROM renamed
WHERE rn = 1