WITH source AS (
    SELECT *
    FROM {{ source('ELICENSE_RAW', 'LICENSE') }}
    WHERE
        NOT hide_from_public__c
        AND LOWER(TRIM(musw__status__c)) NOT IN ('pending', 'generate fee')
        AND musw__type__c NOT IN (
            'Continuing Education Provider',
            'Dialysis Technician - Temporary (IDT)',
            'Employer'
        )
),

renamed AS (
    SELECT
        id AS license_id,
        name AS license_number,
        musw__applicant__c AS contact_id,
        musw__type__c AS type,
        musw__primary_licensee__c AS account_id,
        board__c AS board_name,
        board_action__c AS board_action,
        musw__status__c AS status,
        hide_from_public__c AS hide_from_public,
        hide_from_portal__c AS hide_from_portal,
        compact_eligible__c AS compact_eligible_flag,
        CASE
            WHEN
                board__c = 'Nursing Board'
                AND musw__type__c IN (
                    'Registered Nurse (RN)',
                    'Licensed Practical Nurse (LPN)'
                )
                AND (
                    (
                        musw__status__c = 'Active'
                        AND (sub_status__c IS NULL OR sub_status__c = '')
                    )
                    OR (
                        musw__status__c = 'Inactive'
                        AND COALESCE(sub_status__c, '') IN (
                            '',
                            'Board Action',
                            'Deceased',
                            'Revocation',
                            'Surrendered',
                            'Suspended',
                            'Lapsed'
                        )
                    )
                    OR (
                        musw__status__c = 'Closed'
                        AND COALESCE(sub_status__c, '') IN (
                            '', 'Deceased', 'Board Action'
                        )
                    )
                )
                AND compact_eligible__c = TRUE THEN 'Multi-State'
            WHEN
                board__c = 'Nursing Board'
                AND musw__type__c IN (
                    'Registered Nurse (RN)',
                    'Licensed Practical Nurse (LPN)'
                )
                AND (
                    (
                        musw__status__c = 'Active'
                        AND (sub_status__c IS NULL OR sub_status__c = '')
                    )
                    OR (
                        musw__status__c = 'Inactive'
                        AND COALESCE(sub_status__c, '') IN (
                            '',
                            'Board Action',
                            'Deceased',
                            'Revocation',
                            'Surrendered',
                            'Suspended',
                            'Lapsed'
                        )
                    )
                    OR (
                        musw__status__c = 'Closed'
                        AND COALESCE(sub_status__c, '') IN (
                            '', 'Deceased', 'Board Action'
                        )
                    )
                )
                AND (compact_eligible__c = FALSE OR compact_eligible__c IS NULL)
                THEN 'Single-State'
            WHEN
                board__c = 'Nursing Board'
                AND UPPER(SUBSTRING(name, 1, 3)) = 'APP'
                THEN NULL
            ELSE NULL
        END AS compact_eligibility,
        sub_status__c AS sub_status,
        sub_category__c AS sub_category,
        business_license__c AS business_license,
        lastmodifieddate
    FROM source
),

-- Stage 1: current state per Salesforce record
latest_per_license_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY license_id
            ORDER BY lastmodifieddate DESC
        ) AS rn_id
    FROM renamed
),

deduped_by_id AS (
    SELECT *
    FROM latest_per_license_id
    WHERE rn_id = 1
),

-- Stage 2: current state per real-world license
latest_per_license_number AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY license_number
            ORDER BY lastmodifieddate DESC
        ) AS rn_license
    FROM deduped_by_id
)

SELECT
    license_id,
    account_id,
    contact_id,
    license_number,
    type,
    board_name,
    hide_from_portal,
    hide_from_public,
    status,
    sub_status,
    sub_category,
    business_license,
    compact_eligibility,
    compact_eligible_flag,
    lastmodifieddate
FROM latest_per_license_number
WHERE rn_license = 1
