WITH source AS (
    SELECT *
    FROM {{ source('ELICENSE_RAW', 'EXTERNAL_RECORD') }}
    WHERE license2__c IS NOT NULL
),

distinct_specialties AS (
    SELECT DISTINCT
        license2__c AS specialty_license_id,

        med_specialty_certifying_organization__c
            AS med_specialty_type,
        med_specialty__c
            AS med_specialty,
        med_subspecialties__c
            AS med_sub_specialty,

        certifying_organization__c
            AS national_certification_organization,
        CAST(expiration_date__c AS DATE)
            AS national_certification_expiration_date,
        specialty__c
            AS national_certification_specialty,
        focus__c
            AS national_certification_foci
    FROM source
)

SELECT *
FROM distinct_specialties
