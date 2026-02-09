WITH source AS (
    SELECT
        *,
        {{ normalize_label('type') }} AS normalized_type,
        {{ normalize_csv_text('licensee_name') }} AS normalized_licensee_name,
        {{ normalize_whitespace(
            normalize_dashes(
                normalize_apostrophes('sub_category')
            )
        ) }} AS normalized_sub_category
    FROM {{ ref('stg_license') }}
    WHERE
        LOWER(TRIM(status)) NOT IN ('pending', 'generate fee')
        AND type NOT IN (
            'Continuing Education Provider',
            'Dialysis Technician - Temporary (IDT)',
            'Employer'
        )
),

max_pipeline AS (
    SELECT MAX(pipeline_start_date) AS max_pipeline_start_date
    FROM {{ source('ELICENSE_RAW', 'LICENSE') }}
),

-- Stage 1: current state per Salesforce record
latest_per_license_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY license_id
            ORDER BY lastmodifieddate DESC
        ) AS rn_id
    FROM source
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
    l.license_id,
    l.account_id,
    l.contact_id,
    l.license_number,
    l.normalized_licensee_name AS licensee_name,
    l.normalized_type AS type,
    l.board_name,
    l.board_action,
    l.hide_from_portal,
    l.hide_from_public,
    l.status,
    l.sub_status,
    l.normalized_sub_category AS sub_category,
    l.business_license,

    CASE
        WHEN
            l.board_name = 'Nursing Board'
            AND l.type IN (
                'Registered Nurse (RN)',
                'Licensed Practical Nurse (LPN)'
            )
            AND (
                (
                    l.status = 'Active'
                    AND (l.sub_status IS NULL OR l.sub_status = '')
                )
                OR (
                    l.status = 'Inactive'
                    AND COALESCE(l.sub_status, '') IN (
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
                    l.status = 'Closed'
                    AND COALESCE(l.sub_status, '') IN (
                        '', 'Deceased', 'Board Action'
                    )
                )
            )
            AND l.compact_eligible_flag = TRUE
            THEN 'Multi-State'

        WHEN
            l.board_name = 'Nursing Board'
            AND l.type IN (
                'Registered Nurse (RN)',
                'Licensed Practical Nurse (LPN)'
            )
            AND (
                (
                    l.status = 'Active'
                    AND (l.sub_status IS NULL OR l.sub_status = '')
                )
                OR (
                    l.status = 'Inactive'
                    AND COALESCE(l.sub_status, '') IN (
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
                    l.status = 'Closed'
                    AND COALESCE(l.sub_status, '') IN (
                        '', 'Deceased', 'Board Action'
                    )
                )
            )
            AND (l.compact_eligible_flag = FALSE OR l.compact_eligible_flag IS NULL)
            THEN 'Single-State'

        WHEN
            l.board_name = 'Nursing Board'
            AND UPPER(SUBSTRING(l.license_number, 1, 3)) = 'APP'
            THEN NULL

        ELSE NULL
    END AS compact_eligibility,

    l.compact_eligible_flag,
    l.license_issue_date,
    l.license_effective_date,
    l.license_expiration_date,
    m.max_pipeline_start_date,
    l.lastmodifieddate

FROM latest_per_license_number l
CROSS JOIN max_pipeline m
WHERE
    l.rn_license = 1
    AND l.hide_from_portal = FALSE
