WITH licenses AS (
    SELECT * FROM {{ ref('int_license') }}
    WHERE business_license <> TRUE
),

accounts AS (
    SELECT * FROM {{ ref('stg_account') }}
),

addresses AS (
    SELECT * FROM {{ ref('int_address') }}
),

contacts AS (
    SELECT * FROM {{ ref('stg_contact') }}
),

endorsements AS (
    SELECT * FROM {{ ref('int_license_endorsements_pivoted') }}
),

specialties AS (
    SELECT * FROM {{ ref('int_license_specialties_pivoted') }}
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
        addr.parcel_city,
        addr.parcel_state,
        addr.parcel_county,
        addr.parcel_country,
        addr.parcel_zip_code,
        -- Standardizing the licensee name using contact fields
        UPPER(TRIM(
            COALESCE(c.lastname, '')
            || CASE
                WHEN c.suffix IS NOT NULL AND TRIM(c.suffix) <> ''
                    THEN ' ' || c.suffix
                ELSE ''
            END
            || ', '
            || COALESCE(c.firstname, '')
            || CASE
                WHEN c.middlename IS NOT NULL AND TRIM(c.middlename) <> ''
                    THEN ' ' || c.middlename
                ELSE ''
            END
        )) AS licensee_name,
        e.*,
        s.*

    FROM licenses l
    LEFT JOIN accounts a ON l.account_id = a.account_id
    LEFT JOIN addresses addr ON l.license_id = addr.license_id
    LEFT JOIN contacts c ON l.contact_id = c.contact_id
    LEFT JOIN endorsements e ON l.license_id = e.endorsement_license_id
    LEFT JOIN specialties s ON l.license_id = s.specialty_license_id
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY license_number
            ORDER BY license_effective_date DESC, license_issue_date DESC
        ) AS rn
    FROM prepared_data
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
    parcel_zip_code,
    parcel_county,
    parcel_country,
    
    -- Dates
    license_issue_date,
    license_effective_date,
    license_expiration_date,

    --Endorsements
    endorsement_number_1,
    endorsement_type_1,
    endorsement_status_1,
    endorsement_sub_status_1,
    endorsement_sub_category_1,
    endorsement_issue_date_1,
    endorsement_expiration_date_1,
    endorsement_number_2,
    endorsement_type_2,
    endorsement_status_2,
    endorsement_sub_status_2,
    endorsement_sub_category_2,
    endorsement_issue_date_2,
    endorsement_expiration_date_2,
    endorsement_number_3,
    endorsement_type_3,
    endorsement_status_3,
    endorsement_sub_status_3,
    endorsement_sub_category_3,
    endorsement_issue_date_3,
    endorsement_expiration_date_3,
    endorsement_number_4,
    endorsement_type_4,
    endorsement_status_4,
    endorsement_sub_status_4,
    endorsement_sub_category_4,
    endorsement_issue_date_4,
    endorsement_expiration_date_4,
    endorsement_number_5,
    endorsement_type_5,
    endorsement_status_5,
    endorsement_sub_status_5,
    endorsement_sub_category_5,
    endorsement_issue_date_5,
    endorsement_expiration_date_5,
    endorsement_number_6,
    endorsement_type_6,
    endorsement_status_6,
    endorsement_sub_status_6,
    endorsement_sub_category_6,
    endorsement_issue_date_6,
    endorsement_expiration_date_6,
    endorsement_number_7,
    endorsement_type_7,
    endorsement_status_7,
    endorsement_sub_status_7,
    endorsement_sub_category_7,
    endorsement_issue_date_7,
    endorsement_expiration_date_7,
    endorsement_number_8,
    endorsement_type_8,
    endorsement_status_8,
    endorsement_sub_status_8,
    endorsement_sub_category_8,
    endorsement_issue_date_8,
    endorsement_expiration_date_8,
    endorsement_number_9,
    endorsement_type_9,
    endorsement_status_9,
    endorsement_sub_status_9,
    endorsement_sub_category_9,
    endorsement_issue_date_9,
    endorsement_expiration_date_9,
    endorsement_number_10,
    endorsement_type_10,
    endorsement_status_10,
    endorsement_sub_status_10,
    endorsement_sub_category_10,
    endorsement_issue_date_10,
    endorsement_expiration_date_10,

    --specialties
    med_specialty_1,
    med_sub_specialty_1,
    national_cert_organization_1,
    national_cert_expiration_date_1,
    med_specialty_2,
    med_sub_specialty_2,
    national_cert_organization_2,
    national_cert_expiration_date_2,
    med_specialty_3,
    med_sub_specialty_3,
    national_cert_organization_3,
    national_cert_expiration_date_3,
    med_specialty_4,
    med_sub_specialty_4,
    national_cert_organization_4,
    national_cert_expiration_date_4,
    med_specialty_5,
    med_sub_specialty_5,
    national_cert_organization_5,
    national_cert_expiration_date_5,
    med_specialty_6,
    med_sub_specialty_6,
    national_cert_organization_6,
    national_cert_expiration_date_6,
    med_specialty_7,
    med_sub_specialty_7,
    national_cert_organization_7,
    national_cert_expiration_date_7,
    med_specialty_8,
    med_sub_specialty_8,
    national_cert_organization_8,
    national_cert_expiration_date_8,
    med_specialty_9,
    med_sub_specialty_9,
    national_cert_organization_9,
    national_cert_expiration_date_9,
    med_specialty_10,
    med_sub_specialty_10,
    national_cert_organization_10,
    national_cert_expiration_date_10
FROM ranked
WHERE rn = 1
ORDER BY licensee_name, license_number
