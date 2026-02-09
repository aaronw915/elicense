WITH licenses AS (
    SELECT * FROM {{ ref('stg_license') }}
    WHERE
        business_license = TRUE
        AND hide_from_portal = FALSE
),

accounts AS (
    SELECT * FROM {{ ref('stg_account') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_address') }}
),

prepared_data AS (
    SELECT
        l.*,
        -- Bringing in account fields
        a.doing_business_as_1,
        a.doing_business_as_all_3,
        -- Bringing in address fields
        addr.parcel_street_address,
        addr.parcel_city,
        addr.parcel_state,
        addr.parcel_county,
        addr.parcel_country,
        addr.parcel_zip_code,
        -- Standardizing names for the comparison logic
        UPPER(TRIM(l.licensee_name)) AS clean_name,
        UPPER(TRIM(a.doing_business_as_1)) AS clean_dba1,
        UPPER(TRIM(a.doing_business_as_all_3)) AS clean_dba3
    FROM licenses l
    LEFT JOIN accounts a ON l.account_id = a.account_id
    LEFT JOIN addresses addr ON l.license_id = addr.license_id
)

SELECT
    -- Business Naming Logic
    CASE
        -- 1. All names are the same
        WHEN clean_name = clean_dba1 AND clean_name = clean_dba3
            THEN clean_name

        -- 2. Name is different from DBA 1
        WHEN clean_name != clean_dba1 AND clean_dba1 IS NOT NULL AND clean_dba1 != ''
            THEN clean_name || ' D.B.A ' || clean_dba1

        -- 3. Name matches DBA 1, but is different from DBA 3
        WHEN clean_name = clean_dba1 AND clean_name != clean_dba3 AND clean_dba3 IS NOT NULL AND clean_dba3 != ''
            THEN clean_name || ' D.B.A ' || clean_dba3

        -- 4. Fallback (if DBAs are null/empty or all match)
        ELSE clean_name
    END AS business_name,

    -- Categorization & Status
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
    parcel_county,
    parcel_country,
    parcel_zip_code,

    -- Dates
    license_issue_date,
    license_effective_date,
    license_expiration_date
FROM prepared_data
