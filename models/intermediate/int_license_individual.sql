WITH licenses AS (
    SELECT * FROM {{ ref('stg_license') }}
    WHERE business_license = FALSE
),

accounts AS (
    SELECT * FROM {{ ref('stg_account') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_address') }}
),

contacts AS (
    SELECT * FROM {{ ref('stg_contact') }}
),

prepared_data AS (
    SELECT
        l.board_name,
        l.license_number,
        l.type,
        l.status,
        l.sub_status,
        l.sub_category,
        l.board_action,
        l.license_issue_date,
        l.license_effective_date,
        l.license_expiration_date,
        l.compact_eligibility,
        l.compact_eligible_flag,
        l.hide_from_portal,
        l.hide_from_public,
        -- Bringing in address fields
        addr.parcel_street_address,
        addr.parcel_city,
        addr.parcel_state,
        addr.parcel_county,
        addr.parcel_country,
        addr.parcel_zip_code,
        -- Standardizing the licensee name using contact fields
        UPPER(TRIM(
            COALESCE(c.lastname, '') || ' '
            || COALESCE(c.suffix, '') || ', '
            || COALESCE(c.firstname, '') || ' '
            || COALESCE(c.middlename, '')
        )) AS licensee_name

    FROM licenses l
    LEFT JOIN accounts a ON l.account_id = a.account_id
    LEFT JOIN addresses addr ON l.license_id = addr.license_id
    LEFT JOIN contacts c ON l.contact_id = c.contact_id
)

SELECT
    licensee_name,
    board_name,
    license_number,
    type,
    status,
    sub_status,
    sub_category,
    board_action,

    -- Address Info
    parcel_city,
    parcel_state,
    parcel_county,
    parcel_country,
    parcel_zip_code,

    -- Dates
    license_issue_date,
    license_effective_date,
    license_expiration_date
FROM prepared_data