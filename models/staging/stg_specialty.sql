WITH source AS (
    SELECT *
    FROM
        {{ source('ELICENSE_RAW', 'EXTERNAL_RECORD') }}
    WHERE license2__c IS NOT null
),

renamed AS (
    SELECT
        id AS specialty_id,
        name,
        contact_external_record__c AS contact_id,
        license2__c AS specialty_license_id,
        med_specialty_certifying_organization__c AS med_specialty_type,
        med_specialty_org_other_details__c AS med_specialty_other_type,
        med_specialty__c AS med_specialty,
        med_specialty_other_details__c AS med_specialty_other_specialty,
        med_subspecialties__c AS med_sub_specialty,
        med_sub_specialty_other_details__c AS med_sub_specialty_other_specialty,
        certifying_organization__c AS national_certification_organization,
        CAST(expiration_date__c AS DATE) AS national_certification_expiration_date,
        specialty__c AS national_certification_specialty,
        focus__c AS national_certification_foci,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                lastmodifieddate DESC NULLS LAST
        ) AS rn
    FROM
        source
)

SELECT
    specialty_id,
    name,
    contact_id,
    specialty_license_id,
    med_specialty_type,
    med_specialty_other_type,
    med_specialty,
    med_specialty_other_specialty,
    med_sub_specialty,
    med_sub_specialty_other_specialty,
    national_certification_organization,
    national_certification_expiration_date,
    national_certification_specialty,
    national_certification_foci
FROM
    renamed
WHERE
    rn = 1