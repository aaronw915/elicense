WITH licenses AS (
    SELECT *
    FROM {{ ref('int_license') }}
    WHERE
        business_license = TRUE
        AND hide_from_portal = FALSE
),

accounts AS (
    SELECT * FROM {{ ref('stg_account') }}
),

addresses AS (
    SELECT * FROM {{ ref('int_address') }}
),

prepared_data AS (
    SELECT
        l.*,

        -- Account fields
        a.doing_business_as_1,
        a.doing_business_as_all_3,

        -- Address fields
        addr.parcel_street_address,
        addr.parcel_city,
        addr.parcel_state,
        addr.parcel_zip_code,
        addr.parcel_county,
        addr.parcel_country,

        -- Standardized fields for name logic
        UPPER(TRIM(l.licensee_name)) AS clean_name,
        UPPER(TRIM(a.doing_business_as_1)) AS clean_dba1,
        UPPER(TRIM(a.doing_business_as_all_3)) AS clean_dba3

    FROM licenses l
    LEFT JOIN accounts a ON l.account_id = a.account_id
    LEFT JOIN addresses addr ON l.license_id = addr.license_id
),

business_prepared AS (
    SELECT
        -- Business Naming Logic
        CASE
            WHEN clean_name = clean_dba1 AND clean_name = clean_dba3
                THEN clean_name

            WHEN
                clean_name != clean_dba1
                AND clean_dba1 IS NOT NULL
                AND clean_dba1 != ''
                THEN clean_name || ' D.B.A ' || clean_dba1

            WHEN
                clean_name = clean_dba1
                AND clean_name != clean_dba3
                AND clean_dba3 IS NOT NULL
                AND clean_dba3 != ''
                THEN clean_name || ' D.B.A ' || clean_dba3

            ELSE clean_name
        END AS business_name,

        board_name,
        license_number,
        type,
        status,
        sub_status,
        sub_category,
        board_action,

        -- Address Info
        parcel_street_address,
        parcel_city,
        parcel_state,
        parcel_zip_code,
        parcel_county,
        parcel_country,

        -- Dates
        license_issue_date,
        license_effective_date,
        license_expiration_date

    FROM prepared_data
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY license_number
            ORDER BY license_effective_date DESC, license_issue_date DESC
        ) AS rn
    FROM business_prepared
)

SELECT
    business_name,
    board_name,
    license_number,
    type,
    status,
    sub_status,
    sub_category,
    board_action,
    parcel_street_address,
    parcel_city,
    parcel_state,
    parcel_zip_code,
    parcel_county,
    parcel_country,
    license_issue_date,
    license_effective_date,
    license_expiration_date
FROM ranked
WHERE rn = 1
ORDER BY business_name, license_number
