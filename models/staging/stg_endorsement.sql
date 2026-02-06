WITH source AS (
    SELECT *
    FROM
        {{ source('ELICENSE_RAW', 'QUALIFIER') }}
),

renamed AS (
    SELECT
        id AS endorsement_id,
        license__c AS endorsement_license_id,
        CASE
            WHEN board__c = 'Medical Board' THEN NULL
            ELSE name
        END AS endorsement_number,
        status__c AS endorsement_status,
        sub_status__c AS endorsement_sub_status,
        sub_category__c AS endorsement_sub_category,
        qualifier_type__c AS endorsement_type,
        board_action__c AS endorsement_board_action,
        CAST(qualifier_issue_date__c AS DATE) AS issue_date,
        CAST(expiration_date__c AS DATE) AS expiration_date,
        ROW_NUMBER() OVER (
            PARTITION BY
                license__c,
                name
            ORDER BY
                createddate ASC,
                lastmodifieddate DESC
        ) AS rn
    FROM
        source
    WHERE
        NOT hide_from_portal__c
        AND LOWER(TRIM(status__c)) NOT IN ('pending', 'generate fee')
        AND NOT (
            qualifier_type__c = 'Certificate to Recommend'
            AND status__c <> 'Active'
        )
)

SELECT
    endorsement_id,
    endorsement_license_id,
    endorsement_number,
    endorsement_status,
    endorsement_sub_status,
    endorsement_sub_category,
    endorsement_type,
    endorsement_board_action,
    issue_date,
    expiration_date
FROM
    renamed
WHERE
    rn = 1
