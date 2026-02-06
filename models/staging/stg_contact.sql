WITH source AS (
    SELECT *
    FROM
        {{ source('ELICENSE_RAW', 'CONTACT') }}
),

renamed AS (
    SELECT
        id AS contact_id,
        lastname,
        firstname,
        middlename,
        contact_suffix__c AS suffix,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY lastmodifieddate DESC
        ) AS rn
    FROM
        source
)

SELECT
    contact_id,
    lastname,
    firstname,
    middlename,
    suffix
FROM
    renamed
WHERE rn = 1
